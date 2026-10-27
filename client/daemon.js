// daemon.js
const http = require('http');
const { exec, spawn } = require('child_process');
const path = require('path');
const os = require('os');

// 获取当前脚本所在目录
const scriptDir = __dirname;

// 配置
const CONFIG = {
    targetUrl: 'http://app.example.com/index.html',
    checkInterval: 60000, // 检查间隔 60秒（1分钟）
    maxRetries: 3, // 最大重试次数
    retryInterval: 5000, // 重试间隔 5秒
    // 要查找的进程名/命令行特征：必须能唯一匹配 http-proxy 的 client 进程，
    // 用 index.js 会误匹配任何 index.js 进程（含 daemon 自身被拉起的子进程），
    // 这里用 client 目录下的 index.js 完整路径片段。
    processName: path.join(scriptDir, 'index.js'),
    // 使用绝对 node 路径 + index.js 绝对路径，避免依赖 cwd 与 PATH
    startCommand: `"${process.execPath}" "${path.join(scriptDir, 'index.js')}"`,
    workingDir: scriptDir // 工作目录为daemon.js所在目录
};

// 状态管理
let failCount = 0;
let isRestarting = false;
let checkTimer = null;
let childProcess = null; // 保存子进程引用

// 检测操作系统
const isWindows = os.platform() === 'win32';
const isLinux = os.platform() === 'linux';
const isMac = os.platform() === 'darwin';

/**
 * 标准化路径（跨平台）
 */
function normalizePath(filePath) {
    if (isWindows) {
        return filePath.replace(/\//g, '\\');
    } else {
        // Linux/Mac: 转换Windows路径格式
        return filePath.replace(/^[A-Z]:/, '').replace(/\\/g, '/');
    }
}

/**
 * 检查URL是否可访问
 */
function checkUrl(url) {
    return new Promise((resolve) => {
        const request = http.get(url, (response) => {
            // 只要状态码是200-399都视为成功
            if (response.statusCode >= 200 && response.statusCode < 400) {
                resolve(true);
            } else {
                resolve(false);
            }
            response.resume(); // 消耗响应数据，避免内存泄漏
        });

        request.on('error', (err) => {
            console.log(`[${new Date().toLocaleString()}] 请求失败: ${err.message}`);
            resolve(false);
        });

        // 设置超时，避免长时间等待
        request.setTimeout(10000, () => {
            request.destroy();
            resolve(false);
        });

        request.end();
    });
}

/**
 * 执行系统命令（跨平台）
 */
function executeCommand(command, options = {}) {
    return new Promise((resolve, reject) => {
        console.log(`[${new Date().toLocaleString()}] 执行命令: ${command}`);
        exec(command, options, (error, stdout, stderr) => {
            if (error) {
                console.log(`[${new Date().toLocaleString()}] 命令执行错误: ${error.message}`);
                reject(error);
                return;
            }
            if (stderr && !options.silent) {
                console.log(`[${new Date().toLocaleString()}] 命令stderr: ${stderr}`);
            }
            if (stdout && !options.silent) {
                console.log(`[${new Date().toLocaleString()}] 命令输出: ${stdout.trim() || '无输出'}`);
            }
            resolve(stdout);
        });
    });
}

/**
 * 获取进程PID（跨平台）
 */
async function getProcessPid(processName) {
    try {
        let findCommand;
        let options = { silent: true };
        
        if (isWindows) {
            // Windows: 使用wmic查找
            findCommand = `wmic process where "name='node.exe' and commandline like '%${processName}%'" get processid`;
        } else {
            // Linux/Mac: 使用ps和grep查找
            findCommand = `ps aux | grep "node.*${processName}" | grep -v grep | awk '{print $2}'`;
        }
        
        const result = await executeCommand(findCommand, options);
        const lines = result.split('\n').filter(line => line.trim());
        
        if (lines.length === 0) {
            return null;
        }
        
        // 返回第一个找到的PID
        const pid = lines[0].trim();
        if (pid && /^\d+$/.test(pid)) {
            return parseInt(pid);
        }
        return null;
    } catch (error) {
        // 静默失败，不输出错误日志
        return null;
    }
}

/**
 * 通过PID杀死进程（跨平台）
 */
async function killProcessByPid(pid) {
    if (!pid) {
        console.log(`[${new Date().toLocaleString()}] 没有有效的PID`);
        return false;
    }

    try {
        let killCommand;
        let options = {};
        
        if (isWindows) {
            killCommand = `taskkill /F /PID ${pid}`;
        } else {
            killCommand = `kill -9 ${pid}`;
        }
        
        await executeCommand(killCommand, options);
        console.log(`[${new Date().toLocaleString()}] 已杀死进程 PID: ${pid}`);
        return true;
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 杀死进程失败:`, error.message);
        return false;
    }
}

/**
 * 启动服务（使用spawn以便控制进程，跨平台）
 */
async function startServiceWithSpawn() {
    try {
        console.log(`[${new Date().toLocaleString()}] 尝试启动服务...`);
        
        // 解析命令和参数
        let commandParts = CONFIG.startCommand.split(' ');
        let command = commandParts[0];
        let args = commandParts.slice(1);
        
        // 获取工作目录
        let cwd = CONFIG.workingDir || scriptDir;
        
        // Linux/Mac: 转换路径格式
        if (!isWindows) {
            cwd = normalizePath(cwd);
            // 如果路径以/开头，直接使用
            if (!cwd.startsWith('/')) {
                cwd = '/' + cwd;
            }
        }
        
        console.log(`[${new Date().toLocaleString()}] 工作目录: ${cwd}`);
        console.log(`[${new Date().toLocaleString()}] 执行命令: ${command}`);
        console.log(`[${new Date().toLocaleString()}] 参数: ${args.join(' ')}`);
        
        // 使用spawn启动进程，可以获取进程引用
        const spawnOptions = {
            cwd: cwd,
            stdio: 'inherit',
            detached: false,
            shell: true,
            env: process.env
        };
        
        // Windows特殊处理
        if (isWindows) {
            spawnOptions.windowsHide = true;
        }
        
        childProcess = spawn(command, args, spawnOptions);
        
        console.log(`[${new Date().toLocaleString()}] 服务已启动，PID: ${childProcess.pid}`);
        
        // 监听进程退出事件
        childProcess.on('exit', (code, signal) => {
            console.log(`[${new Date().toLocaleString()}] 服务进程退出，代码: ${code}, 信号: ${signal}`);
            childProcess = null;
        });
        
        childProcess.on('error', (err) => {
            console.log(`[${new Date().toLocaleString()}] 服务进程错误: ${err.message}`);
            childProcess = null;
        });
        
        // 等待一下确保进程启动
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        return true;
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 启动服务失败:`, error.message);
        childProcess = null;
        return false;
    }
}

/**
 * 启动服务（备用方法，使用exec，跨平台）
 */
async function startServiceWithExec() {
    try {
        console.log(`[${new Date().toLocaleString()}] 尝试启动服务（备用方式）...`);
        
        let startCommand;
        let options = {};
        
        // 获取工作目录
        let cwd = CONFIG.workingDir || scriptDir;
        
        if (isWindows) {
            startCommand = `start /B ${CONFIG.startCommand}`;
        } else {
            // Linux/Mac: 使用nohup在后台运行
            const normalizedCwd = normalizePath(cwd);
            const normalizedCmd = CONFIG.startCommand;
            startCommand = `cd "${normalizedCwd}" && nohup ${normalizedCmd} > /dev/null 2>&1 &`;
        }
        
        options.cwd = cwd;
        options.shell = true;
        
        console.log(`[${new Date().toLocaleString()}] 工作目录: ${cwd}`);
        console.log(`[${new Date().toLocaleString()}] 启动命令: ${startCommand}`);
        
        await executeCommand(startCommand, options);
        console.log(`[${new Date().toLocaleString()}] 服务启动命令已执行`);
        
        // 等待进程启动
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        // 获取新启动进程的PID
        const pid = await getProcessPid(CONFIG.processName);
        if (pid) {
            console.log(`[${new Date().toLocaleString()}] 找到进程 PID: ${pid}`);
            // 创建一个模拟的进程对象以便后续管理
            childProcess = { 
                pid: pid,
                kill: function(signal) {
                    return killProcessByPid(this.pid);
                }
            };
            return true;
        } else {
            console.log(`[${new Date().toLocaleString()}] 未找到启动的进程`);
            return false;
        }
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 启动服务失败:`, error.message);
        return false;
    }
}

/**
 * 查找并杀死上次启动的进程（跨平台）
 */
async function killProcess() {
    try {
        // 方法1: 如果有子进程引用，直接杀死
        if (childProcess) {
            console.log(`[${new Date().toLocaleString()}] 使用进程引用杀死上次启动的进程 PID: ${childProcess.pid}`);
            
            // 如果是spawn创建的进程，有kill方法
            if (childProcess.kill && typeof childProcess.kill === 'function') {
                // 先尝试优雅关闭
                childProcess.kill('SIGTERM');
                console.log(`[${new Date().toLocaleString()}] 已发送SIGTERM信号到进程 PID: ${childProcess.pid}`);
                
                // 等待进程退出
                await new Promise(resolve => setTimeout(resolve, 2000));
                
                // 检查进程是否还存在，如果存在则强制杀死
                try {
                    const pid = await getProcessPid(CONFIG.processName);
                    if (pid && pid === childProcess.pid) {
                        await killProcessByPid(childProcess.pid);
                    }
                } catch (e) {
                    console.log(`[${new Date().toLocaleString()}] 进程可能已退出`);
                }
                
                childProcess = null;
                return true;
            } else {
                // 如果是通过exec启动的，只有pid
                await killProcessByPid(childProcess.pid);
                childProcess = null;
                return true;
            }
        }
        
        // 方法2: 通过查找进程名杀死（备用方案）
        console.log(`[${new Date().toLocaleString()}] 没有进程引用，通过进程名查找并杀死`);
        const pid = await getProcessPid(CONFIG.processName);
        if (false && pid) {
            await killProcessByPid(pid);
            childProcess = null;
            return true;
        } else {
            console.log(`[${new Date().toLocaleString()}] 未找到${CONFIG.processName}进程`);
            return false;
        }
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 杀死进程失败:`, error.message);
        return false;
    }
}

/**
 * 执行重启流程
 */
async function restartService() {
    if (isRestarting) {
        console.log(`[${new Date().toLocaleString()}] 正在重启中，跳过本次操作`);
        return;
    }

    isRestarting = true;
    console.log(`[${new Date().toLocaleString()}] ========== 开始重启服务 ==========`);

    try {
        // 1. 杀死上次启动的进程
        await killProcess();
        
        // 等待2秒确保进程完全退出
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // 2. 启动服务（优先使用spawn方式）
        let startSuccess = await startServiceWithSpawn();
        
        // 如果spawn方式失败，使用备用方式
        if (!startSuccess || !childProcess) {
            console.log(`[${new Date().toLocaleString()}] spawn方式启动失败，尝试使用exec方式`);
            await startServiceWithExec();
        }
        
        // 重置失败计数
        failCount = 0;
        console.log(`[${new Date().toLocaleString()}] ========== 服务重启完成 ==========`);
        
        // 重启后验证服务
        console.log(`[${new Date().toLocaleString()}] 等待服务启动后进行验证...`);
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        const isHealthy = await checkUrl(CONFIG.targetUrl);
        if (isHealthy) {
            console.log(`[${new Date().toLocaleString()}] 服务验证成功 ✅`);
        } else {
            console.log(`[${new Date().toLocaleString()}] 服务验证失败，将在下次检测时处理`);
        }
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 重启过程中发生错误:`, error.message);
    } finally {
        isRestarting = false;
    }
}

/**
 * 健康检查主函数
 */
async function healthCheck() {
    // 如果正在重启，跳过本次检查
    if (isRestarting) {
        console.log(`[${new Date().toLocaleString()}] 正在重启中，跳过检测`);
        return;
    }

    try {
        console.log(`[${new Date().toLocaleString()}] 正在检查服务状态...`);
        let isHealthy = await checkUrl(CONFIG.targetUrl);
        
        if (isHealthy) {
            console.log(`[${new Date().toLocaleString()}] 服务运行正常 ✅`);
            failCount = 0; // 重置失败计数
            return;
        }

        // 重试检测次数
        while(!isHealthy && failCount < CONFIG.maxRetries) {
            // 等待重试间隔
            await new Promise((resolve) => setTimeout(resolve, CONFIG.retryInterval));
            
            isHealthy = await checkUrl(CONFIG.targetUrl);
            if (isHealthy) {
                console.log(`[${new Date().toLocaleString()}] 重试后服务运行正常 ✅`);
                failCount = 0; // 重置失败计数
                return;
            }

            // 连接失败
            failCount++;
            console.log(`[${new Date().toLocaleString()}] 连接失败 (${failCount}/${CONFIG.maxRetries}) ❌`);
        }

        if (failCount >= CONFIG.maxRetries) {
            console.log(`[${new Date().toLocaleString()}] 连续失败${failCount}次，触发重启机制`);
            await restartService();
        }
    } catch (error) {
        console.log(`[${new Date().toLocaleString()}] 健康检查发生异常:`, error.message);
        failCount++;
        if (failCount >= CONFIG.maxRetries) {
            console.log(`[${new Date().toLocaleString()}] 连续失败${failCount}次，触发重启机制`);
            await restartService();
        }
    }
}

/**
 * 启动守护进程
 */
async function startDaemon() {
    console.log(`[${new Date().toLocaleString()}] ========================================`);
    console.log(`[${new Date().toLocaleString()}] 守护进程启动`);
    console.log(`[${new Date().toLocaleString()}] 脚本目录: ${scriptDir}`);
    console.log(`[${new Date().toLocaleString()}] 操作系统: ${os.platform()} (${os.type()})`);
    console.log(`[${new Date().toLocaleString()}] 监控地址: ${CONFIG.targetUrl}`);
    console.log(`[${new Date().toLocaleString()}] 检查间隔: ${CONFIG.checkInterval}ms`);
    console.log(`[${new Date().toLocaleString()}] 最大重试: ${CONFIG.maxRetries}次`);
    console.log(`[${new Date().toLocaleString()}] 重试间隔: ${CONFIG.retryInterval}ms`);
    console.log(`[${new Date().toLocaleString()}] 启动命令: ${CONFIG.startCommand}`);
    console.log(`[${new Date().toLocaleString()}] 工作目录: ${CONFIG.workingDir}`);
    console.log(`[${new Date().toLocaleString()}] index.js路径: ${path.join(scriptDir, 'index.js')}`);
    console.log(`[${new Date().toLocaleString()}] ========================================`);

    // 检查index.js是否存在
    const indexPath = path.join(scriptDir, 'index.js');
    const fs = require('fs');
    if (!fs.existsSync(indexPath)) {
        console.log(`[${new Date().toLocaleString()}] 警告: ${indexPath} 不存在！`);
        console.log(`[${new Date().toLocaleString()}] 请确保index.js在守护进程同一目录下`);
    } else {
        console.log(`[${new Date().toLocaleString()}] index.js 文件存在 ✅`);
    }

    await restartService();

    // 立即执行一次检查
    healthCheck();

    // 定时检查
    checkTimer = setInterval(healthCheck, CONFIG.checkInterval);

    // 优雅退出
    process.on('SIGINT', () => {
        console.log(`\n[${new Date().toLocaleString()}] 收到退出信号，正在停止守护进程...`);
        if (checkTimer) {
            clearInterval(checkTimer);
        }
        // 杀死子进程
        if (childProcess) {
            console.log(`[${new Date().toLocaleString()}] 正在杀死子进程...`);
            try {
                if (childProcess.kill && typeof childProcess.kill === 'function') {
                    childProcess.kill('SIGTERM');
                } else if (childProcess.pid) {
                    killProcessByPid(childProcess.pid);
                }
            } catch (e) {
                console.log(`[${new Date().toLocaleString()}] 杀死子进程失败:`, e.message);
            }
        }
        process.exit(0);
    });
    
    // 捕获未处理的异常
    process.on('uncaughtException', (error) => {
        console.log(`[${new Date().toLocaleString()}] 未捕获的异常:`, error);
    });
}

// 启动守护进程
startDaemon();