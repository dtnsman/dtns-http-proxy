# Gitee 代码上传说明

本文档记录 DTNS HTTP Tunnel（开源版）仓库在 Gitee 上的创建、上传与发布过程，便于后续维护者复现与追溯。

---

## 一、仓库信息

| 项目 | 内容 |
| --- | --- |
| Gitee 账号 | `dtnsman` |
| 仓库地址 | https://gitee.com/dtnsman/dtns-http-proxy |
| 默认分支 | `main` |
| 许可证 | GPLv3（GNU General Public License v3.0） |
| 本地目录 | `E:\dtns.http-proxy\opensource` |

---

## 二、前置准备

### 1. 环境要求

- Git 2.52+（`git --version` 确认）
- 可访问 `gitee.com` 的网络

### 2. 私人令牌（Access Token）

令牌保存在本机文件中，**切勿提交到仓库**：

```text
C:\Users\FIREBAT\.ssh\opensource-gitee-token.txt
```

Gitee 私人令牌需具备以下权限：

- `projects` — 创建、读取仓库
- `pull_requests` — 可选

> 安全提示：令牌等同密码。推送完成后应立即从 remote URL 中移除，见第四节第 3 步。

---

## 三、创建仓库

通过 Gitee OpenAPI 创建私有仓库：

```powershell
$t = (Get-Content -Raw 'C:\Users\FIREBAT\.ssh\opensource-gitee-token.txt').Trim()
$h = @{ Authorization = "token $t" }

Invoke-RestMethod -Uri 'https://gitee.com/api/v5/user/repos' `
  -Method Post -Headers $h -Body @{
    name        = 'dtns-http-proxy'
    description = 'HTTP/WebSocket 内网穿透（反向隧道）客户端与服务端，协议 2.0'
    private     = 'true'
    auto_init   = 'false'
  }
```

`auto_init = false` 很重要：远端保持空仓库，避免出现初始提交与本地历史分叉。

---

## 四、上传代码

### 1. 进入本地仓库

```powershell
Set-Location 'E:\dtns.http-proxy\opensource'
git status
```

确认工作区干净、当前分支为 `main`。

### 2. 配置远程地址

```powershell
$t = (Get-Content -Raw 'C:\Users\FIREBAT\.ssh\opensource-gitee-token.txt').Trim()
git remote remove origin 2>$null
git remote add origin "https://dtnsman:$t@gitee.com/dtnsman/dtns-http-proxy.git"
```

### 3. 推送

```powershell
$env:GIT_TERMINAL_PROMPT = '0'
git push -u origin main --porcelain
```

**推送成功后立即清除 URL 中的令牌**：

```powershell
git remote set-url origin 'https://gitee.com/dtnsman/dtns-http-proxy.git'
git remote -v
```

之后推送时会提示输入账号密码，密码填私人令牌即可；也可配置凭据管理器缓存。

### 4. 验证

```powershell
git ls-remote origin
git status -sb
```

`git ls-remote` 输出的 `refs/heads/main` 哈希应与本地 `git rev-parse HEAD` 一致。

---

## 五、常见问题

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `remote: Incorrect username or password` | 令牌错误或权限不足 | 重新生成令牌，确认含 `projects` 权限 |
| `! [rejected] main -> main (fetch first)` | 远端非空（`auto_init=true`） | 远端删库重建，或先 `git pull --rebase` |
| `Could not resolve host` | 网络/DNS 问题 | 检查网络与代理设置 |
| 仓库创建后 API 返回 404 | 权限不足或名称已占用 | 更换仓库名，或确认令牌 scope |

---

## 六、发布记录

### 发布 1 — 2026-10-27 · 首次开源发布

**仓库**：`dtnsman/dtns-http-proxy`（私有）
**分支**：`main`
**许可证**：GPLv3

#### 提交历史

| 哈希 | 说明 |
| --- | --- |
| `be77601` | Initial open-source release: client and server sources with example configs |
| `a2b5bd7` | chore: add root .gitignore |
| `74a8e0b` | chore: switch license to GPLv3 |
| `2e4b006` | docs: correct license reference in README to GPLv3 |

#### 内容摘要

- 公开 `client/` 与 `server/` 两个目录源码，协议版本 2.0
- `config.js` 全部改写为**空白示例配置**，不含真实域名、密钥、隧道 ID
- 新增 `README.md`（特性、目录结构、快速开始、配置说明、安全提示、许可证）
- 新增根目录与子目录 `.gitignore`，排除 `node_modules`、日志、运行时配置
- 许可证由 MIT 切换为 GPLv3，并同步修正 README 中的许可证描述

#### 发布前隐私审计

- 扫描源码中残留的真实域名（`*.dtns.top`、`*.devmymc.*`）并全部替换为示例值
- 确认无硬编码 Token、私钥、证书、真实隧道 ID
- 确认 `node_modules` 未被纳入版本控制

#### 发布后状态

- Gitee 远端 `main` 与本地 HEAD 一致：`2e4b006`
- 工作区干净
- 仓库保持**私有**状态，如需公开可在仓库设置中切换

---

*文档生成时间：2026-10-27*
