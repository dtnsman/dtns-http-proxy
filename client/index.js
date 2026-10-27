// index.js - 客户端主文件
const WebSocket = require('ws');
const http = require('http');
const TunnelManager = require('./tunnel-manager');
const config = require('./config');
const { BinaryCodec } = require('./protocol');

class TunnelClient {
  constructor() {
    this.clientId = config.client.id;
    this.wsUrl = `${config.server.wsServerUrl}${config.server.wsPath}?clientId=${this.clientId}`;

    this.ws = null;
    this.tunnelManager = new TunnelManager(config);
    this.reconnectAttempts = 0;
    this.heartbeatInterval = null;
    this.registeredTunnels = new Set();

    // 服务端协议版本：welcome 里未声明时按 v1 处理（旧服务端）
    this.serverProtocolVersion = 1;

    this.sseRequestMap = new Map();

    // ========== 健康校验配置 ==========
    this.healthCheckConfig = config.client.healthCheck || {
      enabled: true,
      maxRetries: 3,
      retryInterval: 2000,
      postRegisterDelayMs: 5000,
      requestTimeoutMs: 10000
    };
    if (this.healthCheckConfig.postRegisterDelayMs === undefined) {
      this.healthCheckConfig.postRegisterDelayMs = 5000;
    }

    // 防止重注册并发
    this.reRegisterAllLock = false;
    // 防止健康校验并发
    this.isHealthChecking = false;
    // 已经触发过重注册的标记，避免同一注册周期内反复触发
    this.hasTriggeredReRegister = false;

    this.handleMessage = this.handleMessage.bind(this);
    this.handleClose = this.handleClose.bind(this);
    this.handleError = this.handleError.bind(this);

    this.setupTunnelManagerEvents();
  }

  setupTunnelManagerEvents() {
    this.tunnelManager.on('tunnelAdded', (tunnel) => {
      console.log(`隧道已添加: ${tunnel.domain}`);
      this.registerTunnelIfConnected(tunnel);
    });

    this.tunnelManager.on('tunnelRemoved', (tunnel) => {
      console.log(`隧道已移除: ${tunnel.domain}`);
      this.unregisterTunnelIfConnected(tunnel);
    });

    this.tunnelManager.on('requestCompleted', ({ domain }) => {
      // 可扩展
    });

    this.tunnelManager.on('requestFailed', ({ domain, error }) => {
      console.error(`隧道 ${domain} 请求失败:`, error.message);
    });
  }

  async start() {
    console.log(`启动内网穿透客户端: ${this.clientId}`);
    console.log(`可用隧道: ${this.tunnelManager.getAllTunnels().length} 个`);

    const enabledTunnels = this.tunnelManager.getEnabledTunnels();
    enabledTunnels.forEach(tunnel => {
      console.log(`  - ${tunnel.domain} -> ${tunnel.local.protocol}://${tunnel.local.host}:${tunnel.local.port}`);
    });

    await this.connect();
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.wsUrl, {
        perMessageDeflate: false,
        maxPayload: config.client.performance.maxPayloadSize,
        binaryType: 'nodebuffer'
      });

      this.ws.on('open', () => {
        console.log('WebSocket连接已建立');
        this.reconnectAttempts = 0;
        this.hasTriggeredReRegister = false;
        this.startHeartbeat();
        // 先声明能力：服务端据此决定是否对该客户端启用流式传输
        this.sendCapabilities();
        this.registerAllTunnels();
        resolve();
      });

      this.ws.on('message', this.handleMessage);
      this.ws.on('close', this.handleClose);
      this.ws.on('error', this.handleError);
    });
  }

  async handleMessage(data) {
    try {
      let message;

      if (Buffer.isBuffer(data)) {
        if (data.length >= 4 && data.slice(0, 4).toString() === 'DTNS') {
          message = BinaryCodec.decode(data);
        } else {
          message = JSON.parse(data.toString());
        }
      } else {
        message = JSON.parse(data);
      }

      switch (message.type) {
        case 'welcome':
          if (typeof message.protocolVersion === 'number') {
            this.serverProtocolVersion = message.protocolVersion;
          }
          // v2+ 服务端能正确还原数组头（多 Set-Cookie）；旧服务端只能逗号拼接。
          BinaryCodec.supportsMultiValueHeaders = this.serverProtocolVersion >= 2;
          BinaryCodec.useV2Status = this.serverProtocolVersion >= 2;
          console.log(`服务器欢迎消息: ${message.timestamp} (协议 v${this.serverProtocolVersion})`);
          break;

        case 'registered':
          this.handleTunnelRegistered(message);
          break;

        case 'http_request':
          await this.handleHttpRequest(message);
          break;

        case 'sse_close':
        case 'sse_data':
        case 'sse_end':
        case 'sse_error':
          this.handleSSEMessage(message);
          break;

        case 'websocket_upgraded':
        case 'websocket_data':
        case 'websocket_end':
        case 'websocket_error':
        case 'websocket_close':
          this.handleWebSocketMessage(message);
          break;

        case 'stream_open':
        case 'stream_chunk':
        case 'stream_end':
        case 'stream_ack':
        case 'stream_cancel':
          if (this.tunnelManager && this.tunnelManager.handleStreamMessage) {
            this.tunnelManager.handleStreamMessage(message, this.ws);
          }
          break;

        case 'tunnel_status':
          this.handleTunnelStatus(message);
          break;

        case 'command':
          this.handleCommand(message);
          break;

        case 'heartbeat_ack':
          break;

        default:
          console.log('收到未知消息类型:', message.type);
      }
    } catch (error) {
      console.error('处理消息失败:', error);
    }
  }

  sendBinaryMessage(message) {
    if (!this.ws || this.ws.readyState !== 1) return false;
    try {
      const buffer = BinaryCodec.encode(message);
      this.ws.send(buffer);
      return true;
    } catch (error) {
      console.error('发送二进制消息失败:', error);
      return false;
    }
  }

  sendCapabilities() {
    try {
      this.sendMessage({
        type: 'capabilities',
        clientId: this.clientId,
        stream: true,
        multiValueHeaders: true,
      });
      console.log('已声明客户端能力: stream=true');
    } catch (e) {
      console.error(`声明能力失败: ${e.message}`);
    }
  }

  sendMessage(message) {
    const preferBinary = config.protocol?.preferred === 'binary';
    if (!preferBinary) {
      return this.sendJsonMessage(message);
    }

    // ========== http_response 的状态码兼容处理 ==========
    // v1 协议里状态码只占 1 个字节，401/404/500 这类 >255 的状态码会被
    // 截断（401 -> 145），浏览器拿到非法状态码，所有 4xx/5xx 接口都会失败。
    // 分两种情况兜底：
    //   1) 服务端已支持 v2 -> 用 2 字节状态码，无损；
    //   2) 仍是旧服务端   -> 只把这一个响应改用 JSON 文本帧发送，
    //      旧服务端的 JSON 分支把 status 当普通数字处理，不会被截断。
    if (message.type === 'http_response' && typeof message.status === 'number') {
      if (message.status > 255 && this.serverProtocolVersion < 2) {
        return this.sendJsonMessage(this.toJsonResponse(message));
      }
      if (this.serverProtocolVersion >= 2) {
        return this.sendBinaryMessage({ ...message, _version: 2 });
      }
    }

    return this.sendBinaryMessage(message);
  }

  sendJsonMessage(message) {
    if (!this.ws || this.ws.readyState !== 1) return false;
    try {
      this.ws.send(JSON.stringify(message));
      return true;
    } catch (error) {
      console.error('发送JSON消息失败:', error);
      return false;
    }
  }

  /**
   * 把二进制 http_response 转成 JSON 分支可用的形式。
   * body 必须是字符串：服务端会按 UTF-8 重新编码并自己重算 content-length，
   * 因此这里丢掉原来的 content-length / transfer-encoding。
   */
  toJsonResponse(message) {
    const headers = {};
    for (const [key, value] of Object.entries(message.headers || {})) {
      if (value === undefined || value === null) continue;
      const lower = key.toLowerCase();
      // 这两个头由服务端根据 body 重新计算，原样带过去反而会冲突
      if (lower === 'content-length' || lower === 'transfer-encoding') continue;
      // JSON 表达不了多值头，数组统一按 ", " 拼接（dsh 只有一个 set-cookie，不受影响）
      headers[key] = Array.isArray(value) ? value.map(String).join(', ') : String(value);
    }

    let body = null;
    if (Buffer.isBuffer(message.body)) {
      const text = message.body.toString('utf8');
      // 非 UTF-8 的响应体会被 JSON 破坏，此时只保留状态码，避免送出乱码
      if (Buffer.from(text, 'utf8').equals(message.body)) {
        body = text;
      } else {
        console.warn(`[sendMessage] ${message.status} 响应体不是合法 UTF-8，已省略 body`);
      }
    } else if (typeof message.body === 'string') {
      body = message.body;
    }

    return { ...message, body, headers };
  }

  // ============================================================
  // ============ 隧道注册成功回调（仅第 1 个触发校验） ============
  // ============================================================
  async handleTunnelRegistered(message) {
    const { domain, success, error } = message;

    if (!success) {
      console.error(`隧道注册失败 ${domain}:`, error);
      return;
    }

    console.log(`隧道注册成功: ${domain}`);
    this.registeredTunnels.add(domain);

    if (!this.healthCheckConfig.enabled) return;
    if (this.hasTriggeredReRegister) {
      console.log(`[健康校验] 本轮已触发过重注册，跳过对 ${domain} 的校验`);
      return;
    }

    const enabledTunnels = this.tunnelManager.getEnabledTunnels();
    if (enabledTunnels.length === 0) return;

    const firstTunnel = enabledTunnels[0];
    if (firstTunnel.domain !== domain) return;

    // 注册成功后等待 5 秒再校验
    const delay = this.healthCheckConfig.postRegisterDelayMs;
    console.log(`[健康校验] 第 1 个隧道注册成功，等待 ${delay}ms 后开始 HTTP 校验: ${domain}`);
    await this.sleep(delay);

    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('[健康校验] 等待结束后 WebSocket 未连接，跳过校验');
      return;
    }
    if (this.hasTriggeredReRegister) {
      console.log('[健康校验] 等待期间已触发重注册，跳过本次校验');
      return;
    }

    console.log(`[健康校验] 开始 HTTP 校验第 1 个隧道: ${domain}`);

    try {
      const ok = await this.verifyFirstTunnelHttp();
      if (!ok) {
        if (this.hasTriggeredReRegister) {
          console.warn('[健康校验] 已触发过重注册，本次不重复触发');
          return;
        }
        this.hasTriggeredReRegister = true;
        console.warn(
          `[健康校验] 第 1 个隧道 ${domain} 连续 ${this.healthCheckConfig.maxRetries} 次失败，触发所有隧道重新注册`
        );
        await this.reRegisterAllTunnels();
      }
    } catch (err) {
      console.error(`[健康校验] 异常: ${err.message}`);
    }
  }

  // ============================================================
  // ============ 第 1 个隧道 HTTP 健康校验 ============
  // ============================================================
  async verifyFirstTunnelHttp() {
    if (this.isHealthChecking) {
      console.log('[健康校验] 已有校验进行中，跳过本次');
      return true;
    }
    this.isHealthChecking = true;

    try {
      const enabledTunnels = this.tunnelManager.getEnabledTunnels();
      if (!enabledTunnels.length) {
        console.warn('[健康校验] 没有已启用隧道，跳过校验');
        return true;
      }

      const tunnel = enabledTunnels[0];
      const domain = tunnel.domain;
      const url = `http://${domain}/`;

      const { maxRetries, retryInterval } = this.healthCheckConfig;

      console.log(`[健康校验] 开始校验第 1 个隧道: ${domain}, URL: ${url}`);

      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          const result = await this.httpGetCheck(url);
          const bodyText = result.body || '';

          if (bodyText.includes('No tunnel registered for domain')) {
            console.warn(`[健康校验] 第 ${attempt}/${maxRetries} 次失败: 返回了 No tunnel registered for domain`);
          } else {
            console.log(`[健康校验] 第 ${attempt}/${maxRetries} 次成功，状态码: ${result.statusCode}`);
            return true;
          }
        } catch (err) {
          console.warn(`[健康校验] 第 ${attempt}/${maxRetries} 次请求异常: ${err.message}`);
        }

        if (attempt < maxRetries) {
          await this.sleep(retryInterval);
        }
      }

      console.error(`[健康校验] 隧道 ${domain} 连续 ${maxRetries} 次检出 No tunnel registered for domain，判定失败`);
      return false;
    } finally {
      this.isHealthChecking = false;
    }
  }

  /**
   * 简单 HTTP GET（非 HTTPS）
   */
  httpGetCheck(url) {
    return new Promise((resolve, reject) => {
      const req = http.get(url, (res) => {
        let data = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: data
          });
        });
      });

      req.setTimeout(this.healthCheckConfig.requestTimeoutMs, () => {
        req.destroy();
        reject(new Error('HTTP 健康校验超时'));
      });

      req.on('error', reject);
    });
  }

  // ============================================================
  // ============ 方案 B：重连 WebSocket，模拟客户端重启 ============
  // ============================================================
  async reRegisterAllTunnels() {
    if (this.reRegisterAllLock) {
      console.log('[重新注册-全部] 已在执行中，跳过本次触发');
      return;
    }
    this.reRegisterAllLock = true;

    try {
      console.log('[重新注册-全部] 采用“重连 WebSocket”方式，模拟客户端重启');
      console.log('[重新注册-全部] 当前连接将关闭，重连后会自动重新注册所有隧道');

      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        // 关闭连接，触发 handleClose → scheduleReconnect → connect → open → registerAllTunnels
        this.ws.close(4000, 'ReRegisterAllTunnels');
      } else {
        console.warn('[重新注册-全部] WebSocket 未连接，直接尝试重连');
        this.scheduleReconnect();
      }
    } finally {
      this.reRegisterAllLock = false;
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async handleHttpRequest(message) {
    const {
      requestId,
      domain,
      method,
      url,
      headers,
      body,
      remoteAddress,
      isSSE = false,
      sseRequestId = null,
      isWebSocket = false,
      wsRequestId = null
    } = message;

    const isUpgradeToWebSocket = headers['upgrade'] &&
      headers['upgrade'].toLowerCase() === 'websocket';

    const actualIsWebSocket = isWebSocket || isUpgradeToWebSocket;

    if (!this.registeredTunnels.has(domain)) {
      console.error(`收到未注册隧道的请求: ${domain}`);
      this.sendErrorResponse(requestId, 404, `Tunnel ${domain} not registered`, domain);
      return;
    }

    if (isSSE && sseRequestId) {
      this.sseRequestMap.set(sseRequestId, {
        domain,
        startTime: Date.now(),
        requestId
      });
    }

    try {
      const response = await this.tunnelManager.forwardRequest(
        domain,
        {
          method,
          url,
          headers,
          body: body,
          remoteAddress,
          isSSE,
          sseRequestId,
          isWebSocket: actualIsWebSocket,
          wsRequestId: wsRequestId || requestId
        },
        this.ws
      );

      this.sendMessage({
        type: 'http_response',
        requestId,
        domain,
        ...response,
        isSSE: isSSE || false,
        sseRequestId: sseRequestId || null,
        isWebSocket: actualIsWebSocket || false,
        wsRequestId: wsRequestId || null
      });

      console.log(`请求完成: ${method} ${url} -> ${response.status}${isSSE ? ' (SSE)' : ''}${actualIsWebSocket ? ' (WebSocket)' : ''}`);
    } catch (error) {
      console.error(`转发请求失败 ${domain}:`, error);

      this.sendErrorResponse(requestId, 502, `Proxy Error: ${error.message}`, domain);

      if (sseRequestId) {
        this.sseRequestMap.delete(sseRequestId);
      }
    }
  }

  handleWebSocketMessage(message) {
    const { type, wsRequestId, data, error, status, code, reason } = message;

    switch (type) {
      case 'websocket_upgraded':
        console.log(`WebSocket升级成功: ${wsRequestId}, status: ${status}`);
        if (this.tunnelManager) {
          this.tunnelManager.handleWebSocketUpgraded(wsRequestId, { status });
        }
        break;

      case 'websocket_data':
        if (this.tunnelManager) {
          this.tunnelManager.handleWebSocketData(wsRequestId, data, message.isBinary, this.ws);
        }
        break;

      case 'websocket_end':
        console.log(`WebSocket连接结束: ${wsRequestId}, code: ${code}`);
        if (this.tunnelManager) {
          this.tunnelManager.handleWebSocketEnd(wsRequestId, { code, reason });
        }
        break;

      case 'websocket_error':
        console.error(`WebSocket错误: ${wsRequestId}`, error);
        if (this.tunnelManager) {
          this.tunnelManager.handleWebSocketError(wsRequestId, error);
        }
        break;

      case 'websocket_close':
        const reasonStr = typeof reason === 'string' ? reason : (reason ? JSON.stringify(reason) : '');
        console.log(`WebSocket连接关闭: ${wsRequestId}, code: ${code}, reason: ${reasonStr}`);
        if (this.tunnelManager) {
          this.tunnelManager.handleWebSocketClose(wsRequestId, { code, reason: reasonStr });
        }
        break;
    }
  }

  handleSSEMessage(message) {
    const { type, sseRequestId } = message;

    switch (type) {
      case 'sse_close':
        console.log(`服务器关闭SSE连接: ${sseRequestId}`);
        this.cleanupSSERequest(sseRequestId);
        break;

      case 'sse_data':
        break;

      case 'sse_end':
        console.log(`SSE连接正常结束: ${sseRequestId}`);
        this.cleanupSSERequest(sseRequestId);
        break;

      case 'sse_error':
        console.error(`SSE连接错误: ${sseRequestId}`, message.error);
        this.cleanupSSERequest(sseRequestId);
        break;
    }
  }

  cleanupSSERequest(sseRequestId) {
    this.sseRequestMap.delete(sseRequestId);
  }

  handleTunnelStatus(message) {
    console.log('收到隧道状态:', message);
  }

  handleCommand(message) {
    const { command, data } = message;

    console.log(`收到命令: ${command}`);

    switch (command) {
      case 'list_tunnels':
        this.sendTunnelList();
        break;

      case 'add_tunnel':
        this.addTunnelFromCommand(data);
        break;

      case 'remove_tunnel':
        this.removeTunnelFromCommand(data);
        break;

      case 'enable_tunnel':
        this.enableTunnelFromCommand(data);
        break;

      case 'disable_tunnel':
        this.disableTunnelFromCommand(data);
        break;

      case 'get_stats':
        this.sendStats();
        break;

      case 'get_sse_connections':
        this.sendSSEConnections();
        break;

      default:
        console.warn(`未知命令: ${command}`);
    }
  }

  sendErrorResponse(requestId, status, message, domain = '') {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      let validStatus = status;
      if (typeof validStatus !== 'number' || validStatus < 100 || validStatus > 599) {
        console.warn(`[sendErrorResponse] 无效状态码: ${validStatus}, 使用 502`);
        validStatus = 502;
      }

      this.sendMessage({
        type: 'http_response',
        requestId,
        domain,
        status: validStatus,
        headers: {
          'Content-Type': 'text/plain',
          'Cache-Control': 'no-cache'
        },
        body: Buffer.from(message, 'utf8'),
        error: message
      });
    }
  }

  registerAllTunnels() {
    const enabledTunnels = this.tunnelManager.getEnabledTunnels();
    console.log(`注册 ${enabledTunnels.length} 个隧道到服务器...`);

    enabledTunnels.forEach(tunnel => {
      this.registerTunnel(tunnel);
    });
  }

  registerTunnel(tunnel) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'register_tunnel',
        clientId: this.clientId,
        tunnel: {
          id: tunnel.id,
          domain: tunnel.domain,
          localPort: tunnel.local.port,
          localHost: tunnel.local.host,
          protocol: tunnel.local.protocol,
          description: tunnel.description
        }
      });

      console.log(`注册隧道: ${tunnel.domain}`);
    }
  }

  registerTunnelIfConnected(tunnel) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN && tunnel.enabled) {
      this.registerTunnel(tunnel);
    }
  }

  unregisterTunnelIfConnected(tunnel) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'unregister_tunnel',
        clientId: this.clientId,
        domain: tunnel.domain
      });

      console.log(`取消注册隧道: ${tunnel.domain}`);
      this.registeredTunnels.delete(tunnel.domain);
    }
  }

  sendTunnelList() {
    const tunnels = this.tunnelManager.exportTunnels();
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'tunnel_list',
        clientId: this.clientId,
        tunnels: tunnels,
        registered: Array.from(this.registeredTunnels)
      });
    }
  }

  addTunnelFromCommand(tunnelConfig) {
    const success = this.tunnelManager.addTunnelFromConfig(tunnelConfig);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'command_response',
        command: 'add_tunnel',
        success,
        message: success ? '隧道添加成功' : '隧道添加失败'
      });
    }
  }

  removeTunnelFromCommand(data) {
    const { domain } = data;
    const success = this.tunnelManager.removeTunnel(domain);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'command_response',
        command: 'remove_tunnel',
        success,
        message: success ? '隧道移除成功' : '隧道移除失败'
      });
    }
  }

  enableTunnelFromCommand(data) {
    const { domain } = data;
    const success = this.tunnelManager.enableTunnel(domain);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'command_response',
        command: 'enable_tunnel',
        success,
        message: success ? '隧道启用成功' : '隧道启用失败'
      });
    }
  }

  disableTunnelFromCommand(data) {
    const { domain } = data;
    const success = this.tunnelManager.disableTunnel(domain);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'command_response',
        command: 'disable_tunnel',
        success,
        message: success ? '隧道禁用成功' : '隧道禁用失败'
      });
    }
  }

  sendStats() {
    const stats = this.tunnelManager.getAllStats();
    const clientStats = {
      connected: this.ws && this.ws.readyState === WebSocket.OPEN,
      reconnectAttempts: this.reconnectAttempts,
      registeredTunnels: Array.from(this.registeredTunnels),
      sseConnections: Array.from(this.sseRequestMap.keys())
    };

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'stats',
        clientId: this.clientId,
        clientStats,
        tunnelStats: stats
      });
    }
  }

  sendSSEConnections() {
    const sseConnections = [];
    for (const [sseRequestId, info] of this.sseRequestMap.entries()) {
      sseConnections.push({
        sseRequestId,
        domain: info.domain,
        duration: Date.now() - info.startTime
      });
    }

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendMessage({
        type: 'sse_connections',
        clientId: this.clientId,
        connections: sseConnections
      });
    }
  }

  handleClose(code, reason) {
    console.log(`WebSocket连接已关闭: ${code} - ${reason || '无原因'}`);
    console.log('隧道统计:', this.tunnelManager.getAllStats());

    this.registeredTunnels.clear();
    this.sseRequestMap.clear();
    this.hasTriggeredReRegister = false;
    this.stopHeartbeat();

    if (this.tunnelManager) {
      this.tunnelManager.cleanupAllWebSocketConnections();
      this.tunnelManager.clearStaleWebSockets();
    }

    if (config.client.reconnect.enabled) {
      this.scheduleReconnect();
    }
  }

  handleError(error) {
    console.error('WebSocket错误:', error.message);
  }

  startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const stats = this.tunnelManager.getAllStats();

        this.sendMessage({
          type: 'heartbeat',
          clientId: this.clientId,
          timestamp: new Date().toISOString(),
          stats: {
            tunnelCount: this.tunnelManager.getAllTunnels().length,
            registeredCount: this.registeredTunnels.size,
            sseConnections: this.sseRequestMap.size,
            totalRequests: Object.values(stats).reduce((sum, s) => sum + s.requests, 0),
            totalErrors: Object.values(stats).reduce((sum, s) => sum + s.errors, 0)
          }
        });
      }
    }, config.client.heartbeat.interval);
  }

  stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  scheduleReconnect() {
    if (this.reconnectAttempts >= config.client.reconnect.maxAttempts) {
      console.error('达到最大重连次数，停止重连');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(
      config.client.reconnect.interval * this.reconnectAttempts,
      60000
    );

    console.log(`将在 ${delay}ms 后尝试重新连接 (尝试 ${this.reconnectAttempts})`);

    setTimeout(() => {
      console.log('尝试重新连接...');
      this.connect().catch(console.error);
    }, delay);
  }

  addTunnel(tunnelConfig) {
    return this.tunnelManager.addTunnel(tunnelConfig);
  }

  removeTunnel(domain) {
    return this.tunnelManager.removeTunnel(domain);
  }

  getTunnelStats(domain) {
    return this.tunnelManager.getTunnelStats(domain);
  }

  stop() {
    console.log('正在停止客户端...');
    console.log('最终统计:', this.tunnelManager.getAllStats());

    this.stopHeartbeat();
    this.sseRequestMap.clear();

    if (this.tunnelManager) {
      this.tunnelManager.cleanupAllWebSocketConnections();
    }

    if (this.ws) {
      this.ws.close();
    }
  }
}

const client = new TunnelClient();

const args = process.argv.slice(2);
if (args.length > 0) {
  if (args[0] === '--config' && args[1]) {
    try {
      const fs = require('fs');
      const customConfig = JSON.parse(fs.readFileSync(args[1], 'utf8'));
      config.tunnels = customConfig.tunnels || config.tunnels;
      console.log(`从文件加载配置: ${args[1]}`);
    } catch (error) {
      console.error('加载配置文件失败:', error);
    }
  }

  if (args[0] === '--add-tunnel' && args.length >= 4) {
    const tunnelConfig = {
      id: args[1],
      domain: args[2],
      local: {
        host: args[3],
        port: parseInt(args[4]) || 80,
        protocol: args[5] || 'http'
      },
      enabled: true,
      description: args[6] || '命令行添加的隧道'
    };

    config.tunnels.push(tunnelConfig);
    console.log('通过命令行添加隧道:', tunnelConfig.domain);
  }
}

client.start().catch(console.error);

process.on('SIGINT', () => {
  client.stop();
  process.exit(0);
});

module.exports = client;