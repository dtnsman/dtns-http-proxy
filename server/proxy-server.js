// proxy-server.js - 代理服务器（含双向背压流控）
const http = require('http');
const https = require('https');
const fs = require('fs');
const url = require('url');
const WebSocket = require('ws');
const crypto = require('crypto');
const { BinaryCodec } = require('./protocol');

// 有效的HTTP状态码范围
const VALID_STATUS_CODES = {
  '100': true, '101': true, '102': true, '103': true,
  '200': true, '201': true, '202': true, '203': true, '204': true, '205': true, '206': true, '207': true, '208': true, '226': true,
  '300': true, '301': true, '302': true, '303': true, '304': true, '305': true, '307': true, '308': true,
  '400': true, '401': true, '402': true, '403': true, '404': true, '405': true, '406': true, '407': true, '408': true, '409': true,
  '410': true, '411': true, '412': true, '413': true, '414': true, '415': true, '416': true, '417': true, '418': true, '421': true,
  '422': true, '423': true, '424': true, '425': true, '426': true, '428': true, '429': true, '431': true, '451': true,
  '500': true, '501': true, '502': true, '503': true, '504': true, '505': true, '506': true, '507': true, '508': true, '510': true, '511': true
};

// ============================================================
// 背压辅助函数
// ============================================================

/**
 * 暂停 browserWs 的底层 socket 读取（上行背压）
 */
function pauseBrowserWs(conn) {
  const { browserWs } = conn;
  if (!browserWs || conn._upPaused) return;
  try {
    const socket = browserWs._socket;
    if (socket && typeof socket.pause === 'function') {
      socket.pause();
      conn._upPaused = true;
      console.log(`[BP-UP] PAUSE ${conn.wsRequestId}`);
    } else {
      conn._upPaused = true;
      console.warn(`[BP-UP] _socket.pause not available on ${conn.wsRequestId}`);
    }
  } catch (e) {
    console.error(`[BP-UP] pause error: ${e.message}`);
  }
}

/**
 * 恢复 browserWs 的底层 socket 读取
 */
function resumeBrowserWs(conn) {
  const { browserWs } = conn;
  if (!browserWs || !conn._upPaused) return;
  try {
    const socket = browserWs._socket;
    if (socket && typeof socket.resume === 'function') {
      socket.resume();
      conn._upPaused = false;
      console.log(`[BP-UP] RESUME ${conn.wsRequestId}`);
    } else {
      conn._upPaused = false;
    }
  } catch (e) {
    console.error(`[BP-UP] resume error: ${e.message}`);
  }
}

class ProxyServer {
  constructor(websocketServer, config) {
    this.websocketServer = websocketServer;
    this.config = config;
    this.httpServer = null;
    this.httpsServer = null;
    this.requestCounter = 0;

    this.sseConnections = new Map();
    this.wsConnections = new Map();
    this.wsBrowserMap = new Map();
    // 流式传输：streamId -> 状态（上行/下行各自的窗口与计数）
    this.streams = new Map();
    // WebSocket 升级等待者：wsRequestId -> {resolve,reject}
    this.upgradeWaiters = new Map();

    // ============================================================
    // ★ 双向背压配置
    // ============================================================

    // 下行：公网服务器 → 浏览器（慢）
    // 背压对象：client.ws（隧道客户端的 socket）
    this.DOWN_HIGH_WATER = 256 * 1024;
    this.DOWN_LOW_WATER = 64 * 1024;

    // 上行：浏览器 → 公网服务器 → 隧道客户端
    // 背压对象：browserWs（浏览器的 socket）
    this.UP_HIGH_WATER = 256 * 1024;
    this.UP_LOW_WATER = 64 * 1024;

    // ★ 下行背压定时器：每 100ms 检查一次
    this._downBpTimer = setInterval(() => {
      try { this._updateDownBackpressure(); }
      catch (e) { console.error('[BP-DOWN] check error:', e.message); }
    }, 100);

    // ★ 上行背压定时器：每 100ms 检查一次
    this._upBpTimer = setInterval(() => {
      try { this._updateUpBackpressure(); }
      catch (e) { console.error('[BP-UP] check error:', e.message); }
    }, 100);

    // ★ 状态日志：每 5 秒
    this._bpLogTimer = setInterval(() => {
      for (const [id, conn] of this.wsConnections) {
        const bBuf = conn.browserWs ? (conn.browserWs.bufferedAmount || 0) : 0;
        const client = this.websocketServer.getClient(conn.clientId);
        const cBuf = client && client.ws ? (client.ws.bufferedAmount || 0) : 0;
        if (bBuf > 64 * 1024 || cBuf > 64 * 1024) {
          console.log(`[BP] ${id} browserBuffered=${(bBuf / 1024).toFixed(1)}KB ` +
                      `clientBuffered=${(cBuf / 1024).toFixed(1)}KB ` +
                      `upPaused=${!!conn._upPaused} ` +
                      `downPaused=${!!(client && client._downPaused)}`);
        }
      }
    }, 5000);
  }

  _isClientPaused(clientId) {
    const client = this.websocketServer.getClient(clientId);
    return !!(client && client._downPaused);
  }

  // ============================================================
  // ★ 下行背压：公网服务器 → 浏览器
  // ============================================================
  _updateDownBackpressure() {
    const byClient = new Map();
    for (const conn of this.wsConnections.values()) {
      if (!conn.browserWs || conn.browserWs.readyState !== WebSocket.OPEN) continue;
      const cur = byClient.get(conn.clientId) || 0;
      const buf = conn.browserWs.bufferedAmount || 0;
      if (buf > cur) byClient.set(conn.clientId, buf);
    }

    for (const [clientId, maxBuffered] of byClient) {
      const client = this.websocketServer.getClient(clientId);
      if (!client || !client.ws || client.ws.readyState !== WebSocket.OPEN) continue;
      if (!client.ws._socket || typeof client.ws._socket.pause !== 'function') continue;

      const shouldPause = maxBuffered > this.DOWN_HIGH_WATER;
      const shouldResume = maxBuffered < this.DOWN_LOW_WATER;

      if (shouldPause && !client._downPaused) {
        client._downPaused = true;
        try { client.ws._socket.pause(); } catch (e) {
          console.error(`[BP-DOWN] pause error: ${e.message}`);
        }
        console.log(`[BP-DOWN] PAUSE client=${clientId} maxBrowserBuffered=${(maxBuffered / 1024).toFixed(1)}KB`);
      } else if (shouldResume && client._downPaused) {
        client._downPaused = false;
        try { client.ws._socket.resume(); } catch (e) {
          console.error(`[BP-DOWN] resume error: ${e.message}`);
        }
        console.log(`[BP-DOWN] RESUME client=${clientId} maxBrowserBuffered=${(maxBuffered / 1024).toFixed(1)}KB`);
      }
    }
  }

  // ============================================================
  // ★ 上行背压：浏览器 → 公网服务器 → 隧道客户端
  //   当 client.ws.bufferedAmount 涨（隧道客户端收不动）→ 暂停 browserWs
  // ============================================================
  _updateUpBackpressure() {
    for (const conn of this.wsConnections.values()) {
      const { browserWs, clientId } = conn;
      if (!browserWs || browserWs.readyState !== WebSocket.OPEN) continue;

      const client = this.websocketServer.getClient(clientId);
      if (!client || !client.ws || client.ws.readyState !== WebSocket.OPEN) continue;

      const tunnelBuffered = client.ws.bufferedAmount || 0;

      const shouldPause = tunnelBuffered > this.UP_HIGH_WATER;
      const shouldResume = tunnelBuffered < this.UP_LOW_WATER;

      if (shouldPause && !conn._upPaused) {
        pauseBrowserWs(conn);
      } else if (shouldResume && conn._upPaused) {
        resumeBrowserWs(conn);
      }
    }
  }

  validateStatusCode(statusCode) {
    if (typeof statusCode === 'number' && statusCode >= 100 && statusCode <= 599) {
      return Math.floor(statusCode);
    }

    if (typeof statusCode === 'string') {
      const num = parseInt(statusCode, 10);
      if (!isNaN(num) && num >= 100 && num <= 599) {
        return num;
      }
    }

    console.warn(`无效的HTTP状态码: ${statusCode}, 使用默认值 200`);
    return 200;
  }

  sendBinaryMessage(ws, message) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      const buffer = BinaryCodec.encode(message);
      ws.send(buffer);
      return true;
    } catch (error) {
      console.error('发送二进制消息失败:', error);
      return false;
    }
  }

  start() {
    this.httpServer = http.createServer((req, res) => {
      this.handleRequest(req, res);
    });

    // 不再让 ws 库自动完成握手：先向隧道客户端确认 101，再决定给浏览器回什么状态码。
    this.wsBrowserServer = new WebSocket.Server({
      noServer: true,
    });

    this.httpServer.on('upgrade', (req, socket, head) => {
      this.handleHttpUpgrade(req, socket, head).catch((e) => {
        console.error(`WebSocket 升级处理异常: ${e.message}`);
        try { socket.destroy(); } catch (er) {}
      });
    });

    this.httpServer.listen(this.config.http.port, this.config.http.host, () => {
      console.log(`HTTP代理服务器运行在 http://${this.config.http.host}:${this.config.http.port}`);
      console.log(`WebSocket代理支持所有路径: ws://${this.config.http.host}:${this.config.http.port}/*`);
    });

    if (this.config.https && this.config.https.enabled) {
      try {
        const options = {
          key: fs.readFileSync(this.config.https.keyPath),
          cert: fs.readFileSync(this.config.https.certPath)
        };

        this.httpsServer = https.createServer(options, (req, res) => {
          this.handleRequest(req, res);
        });

        const wssHttps = new WebSocket.Server({
          noServer: true
        });

        this.httpsServer.on('upgrade', (req, socket, head) => {
          this.handleHttpUpgrade(req, socket, head).catch((e) => {
            console.error(`WebSocket 升级处理异常: ${e.message}`);
            try { socket.destroy(); } catch (er) {}
          });
        });

        this.httpsServer.listen(this.config.https.port, this.config.http.host, () => {
          console.log(`HTTPS代理服务器运行在 https://${this.config.http.host}:${this.config.https.port}`);
        });
      } catch (error) {
        console.error('启动HTTPS服务器失败:', error);
      }
    }

    this.startConnectionCleanup();
  }

  /**
   * 协议里 close code 只占 1 字节（1000 会被截断成 232），
   * 直接交给 ws.close() 会抛 RangeError。统一收敛为合法值。
   */
  static normalizeCloseCode(code, fallback = 1000) {
    const n = Number(code);
    if (!Number.isInteger(n)) return fallback;
    if (n === 1005 || n === 1006 || n === 1015) return fallback;
    if (n < 1000 || n > 4999) return fallback;
    return n;
  }

  handleWebSocketClose(wsRequestId, data) {
    const browserWs = this.wsBrowserMap.get(wsRequestId);
    const connection = this.wsConnections.get(wsRequestId);
    const path = connection?.path || 'unknown';

    const closeCode = data?.code;
    let closeReason = '';
    if (data?.reason) {
      closeReason = typeof data.reason === 'string' ? data.reason : JSON.stringify(data.reason);
    }

    console.log(`[${wsRequestId}] 收到隧道客户端关闭通知, 路径: ${path}, code: ${closeCode}, reason: ${closeReason}`);

    if (browserWs && browserWs.readyState === WebSocket.OPEN) {
      try {
        const code = ProxyServer.normalizeCloseCode(closeCode);
        browserWs.close(code, closeReason);
      } catch (error) {
        console.error(`[${wsRequestId}] 关闭浏览器连接失败:`, error.message);
        try {
          browserWs.terminate();
        } catch (e) {}
      }
    }

    this.cleanupWebSocketConnection(wsRequestId);
  }

  /**
   * 隧道客户端断开（或重连被新连接替换）时，服务端缓存的浏览器 WS 已经没有对端了。
   * 必须主动关掉：否则浏览器一直以为连接还在，永远不重连，
   * 内网应用也就再也收不到新的 WebSocket 握手请求。
   */
  closeBrowserSocketsForClient(clientId, reason = 'tunnel client disconnected') {
    let closed = 0;
    for (const [wsRequestId, conn] of this.wsConnections.entries()) {
      if (!conn || conn.clientId !== clientId) continue;
      const browserWs = this.wsBrowserMap.get(wsRequestId);
      try {
        if (browserWs && browserWs.readyState === WebSocket.OPEN) {
          // 1012 = Service Restart，浏览器收到后会重连
          browserWs.close(1012, reason);
        }
      } catch (e) {}
      try {
        this.cleanupWebSocketConnection(wsRequestId);
      } catch (e) {}
      closed++;
    }
    if (closed > 0) {
      console.log(`客户端 ${clientId} 断开，已关闭 ${closed} 个浏览器 WebSocket 连接，等待浏览器重连`);
    }
    return closed;
  }

  writeRawResponse(socket, status, message) {
    const body = message == null ? '' : String(message);
    const statusText = http.STATUS_CODES[status] || 'Error';
    const head =
      `HTTP/1.1 ${status} ${statusText}\r\n` +
      'Connection: close\r\n' +
      'Content-Type: text/plain; charset=utf-8\r\n' +
      `Content-Length: ${Buffer.byteLength(body)}\r\n` +
      '\r\n';
    try {
      socket.on('error', () => {});
      socket.end(head + body);
    } catch (e) {}
  }

  /**
   * 浏览器 Upgrade 的统一入口：先问隧道客户端，拿到 101 才完成握手。
   * 上游拒绝时直接把真实状态码写回浏览器（不再是"101 后立刻断开"）。
   */
  async handleHttpUpgrade(req, socket, head) {
    socket.on('error', () => {});

    const requestId = ++this.requestCounter;
    const host = req.headers.host;
    const requestPath = req.url;
    console.log(`[${requestId}] WebSocket 升级请求: ${requestPath}, Host: ${host}`);

    const clientInfo = this.websocketServer.getClientByDomain(host);
    if (!clientInfo) {
      console.error(`[${requestId}] 未找到域名对应的客户端: ${host}`);
      return this.writeRawResponse(socket, 404, `No tunnel registered for domain: ${host}`);
    }

    const { clientId, client } = clientInfo;
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      console.error(`[${requestId}] 隧道客户端未连接: ${clientId}`);
      return this.writeRawResponse(socket, 503, `Tunnel client ${clientId} is not connected`);
    }

    const wsRequestId = `ws-${Date.now()}-${Math.random().toString(36).substr(2, 10)}`;

    const requestData = {
      method: 'GET',
      url: requestPath,
      headers: {
        host: host,
        upgrade: 'websocket',
        connection: 'upgrade',
        'sec-websocket-key': req.headers['sec-websocket-key'] || '',
        'sec-websocket-version': req.headers['sec-websocket-version'] || '13',
        'user-agent': req.headers['user-agent'] || '',
        ...(req.headers['cookie'] ? { cookie: req.headers['cookie'] } : {}),
        ...(req.headers['origin'] ? { origin: req.headers['origin'] } : {}),
        ...(req.headers['sec-websocket-protocol'] ? { 'sec-websocket-protocol': req.headers['sec-websocket-protocol'] } : {}),
        ...(req.headers['authorization'] ? { authorization: req.headers['authorization'] } : {})
      },
      body: null,
      remoteAddress: req.socket.remoteAddress,
      timestamp: new Date().toISOString()
    };

    let response;
    try {
      response = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          this.upgradeWaiters.delete(wsRequestId);
          reject(new Error('upgrade timeout'));
        }, 10000);
        if (timer.unref) timer.unref();
        this.upgradeWaiters.set(wsRequestId, {
          resolve: (v) => { clearTimeout(timer); resolve(v); },
          reject: (e) => { clearTimeout(timer); reject(e); },
        });
        this.websocketServer.wsStreams.set(wsRequestId, {
          clientId, domain: host, lastDataTime: Date.now(), requestId: wsRequestId, upgraded: false,
        });
        this.sendBinaryMessage(client.ws, {
          type: 'http_request',
          requestId: wsRequestId,
          domain: host,
          method: requestData.method,
          url: requestData.url,
          headers: requestData.headers,
          body: null,
          remoteAddress: requestData.remoteAddress,
          timestamp: requestData.timestamp,
          isSSE: false,
          sseRequestId: null,
          isWebSocket: true,
          wsRequestId,
        });
      });
    } catch (error) {
      console.error(`[${wsRequestId}] 升级失败: ${error.message}`);
      try { this.websocketServer.wsStreams.delete(wsRequestId); } catch (e) {}
      return this.writeRawResponse(socket, 504, `Tunnel upgrade failed: ${error.message}`);
    }

    if (!response || response.status !== 101) {
      const status = response && response.status >= 400 && response.status <= 599 ? response.status : 502;
      console.error(`[${wsRequestId}] 上游拒绝升级: ${response && response.status}`);
      try { this.websocketServer.wsStreams.delete(wsRequestId); } catch (e) {}
      return this.writeRawResponse(socket, status, 'WebSocket upgrade rejected by upstream');
    }

    const browserWs = this.completeUpgrade(req, socket, head, response);
    if (!browserWs) return;

    this.setupBrowserSocket(browserWs, req, wsRequestId, clientId, host, requestPath);
  }

  /** 把隧道客户端返回的升级结果交给等待中的 handleHttpUpgrade()。 */
  resolveUpgrade(wsRequestId, status, headers) {
    const waiter = this.upgradeWaiters.get(wsRequestId);
    if (!waiter) return false;
    this.upgradeWaiters.delete(wsRequestId);
    waiter.resolve({ status, headers: headers || {} });
    return true;
  }

  /**
   * 手动完成 101 握手：转发上游的 101 响应头（如 Set-Cookie）和协商好的子协议。
   */
  completeUpgrade(req, socket, head, response) {
    try {
      const key = req.headers['sec-websocket-key'];
      if (!key) {
        this.writeRawResponse(socket, 400, 'Missing Sec-WebSocket-Key');
        return null;
      }

      const accept = crypto.createHash('sha1')
        .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
        .digest('base64');

      const upHeaders = (response && response.headers) || {};

      const offered = String(req.headers['sec-websocket-protocol'] || '')
        .split(',').map((s) => s.trim()).filter(Boolean);
      const selected = String(upHeaders['sec-websocket-protocol'] || '').trim();
      const protocol = selected && offered.includes(selected) ? selected : '';

      const HOP = new Set([
        'connection', 'upgrade', 'sec-websocket-accept', 'sec-websocket-protocol',
        'sec-websocket-extensions', 'content-length', 'transfer-encoding', 'keep-alive',
      ]);

      let extra = '';
      for (const [k, v] of Object.entries(upHeaders)) {
        if (HOP.has(k.toLowerCase())) continue;
        if (Array.isArray(v)) {
          for (const one of v) extra += `${k}: ${one}\r\n`;
        } else if (v !== undefined && v !== null) {
          extra += `${k}: ${v}\r\n`;
        }
      }

      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n` +
        (protocol ? `Sec-WebSocket-Protocol: ${protocol}\r\n` : '') +
        extra +
        '\r\n'
      );

      const ws = new WebSocket(null, protocol || undefined, {});
      ws.setSocket(socket, head, { maxPayload: 100 * 1024 * 1024, skipUTF8Validation: true });
      return ws;
    } catch (e) {
      console.error(`WebSocket 握手失败: ${e.message}`);
      try { socket.destroy(); } catch (er) {}
      return null;
    }
  }

  setupBrowserSocket(browserWs, req, wsRequestId, clientId, host, requestPath) {
    this.wsBrowserMap.set(wsRequestId, browserWs);
    this.wsConnections.set(wsRequestId, {
      clientId,
      domain: host,
      path: requestPath,
      createdAt: Date.now(),
      lastActivity: Date.now(),
      wsRequestId,
      browserWs,
      _upPaused: false
    });

    console.log(`[${wsRequestId}] 创建 WebSocket 隧道映射，路径: ${requestPath}`);

    browserWs.on('message', (data, isBinary) => {
      const payload = Buffer.isBuffer(data) ? data : Buffer.from(data);
      console.log(`[${wsRequestId}] 浏览器 -> 隧道 (${requestPath}): ${payload.length} 字节, binary: ${!!isBinary}`);

      const connection = this.wsConnections.get(wsRequestId);
      if (connection) {
        const tunnelClient = this.websocketServer.getClient(connection.clientId);
        if (tunnelClient && tunnelClient.ws.readyState === 1) {
          this.sendBinaryMessage(tunnelClient.ws, {
            type: 'websocket_data',
            wsRequestId: wsRequestId,
            data: payload,
            binary: !!isBinary
          });
          connection.lastActivity = Date.now();
        } else {
          console.warn(`[${wsRequestId}] 隧道客户端不可用，关闭连接`);
          browserWs.close();
          this.cleanupWebSocketConnection(wsRequestId);
        }
      }
    });

    browserWs.on('close', (code, reason) => {
      const reasonStr = Buffer.isBuffer(reason) ? reason.toString() : (reason || '');
      console.log(`[${wsRequestId}] 浏览器 WebSocket 关闭: ${code} - ${reasonStr}, 路径: ${requestPath}`);

      const connection = this.wsConnections.get(wsRequestId);
      if (connection) {
        const tunnelClient = this.websocketServer.getClient(connection.clientId);
        if (tunnelClient && tunnelClient.ws.readyState === 1) {
          let closeCode = code;
          if (!closeCode || closeCode === 1005 || closeCode === 1006) {
            closeCode = 1000;
          }
          this.sendBinaryMessage(tunnelClient.ws, {
            type: 'websocket_close',
            wsRequestId: wsRequestId,
            code: closeCode,
            reason: reasonStr,
            // 注意：二进制编码按字符串写 timestamp，数字会触发 Buffer.from(number) 抛错
            timestamp: new Date().toISOString()
          });
        }
      }

      this.cleanupWebSocketConnection(wsRequestId);
    });

    browserWs.on('error', (error) => {
      console.error(`[${wsRequestId}] 浏览器 WebSocket 错误:`, error.message);
      this.cleanupWebSocketConnection(wsRequestId);
    });
  }
  handleWebSocketData(wsRequestId, data, isBinary) {
    const browserWs = this.wsBrowserMap.get(wsRequestId);
    const connection = this.wsConnections.get(wsRequestId);
    const path = connection?.path || 'unknown';

    console.log(`[${wsRequestId}] 隧道 -> 浏览器, 路径: ${path}, browserWs存在: ${!!browserWs}, 数据长度: ${data?.length || 0}, binary: ${isBinary}`);

    if (!browserWs) {
      console.warn(`[${wsRequestId}] 浏览器 WebSocket 不存在`);
      return;
    }

    if (browserWs.readyState !== WebSocket.OPEN) {
      console.warn(`[${wsRequestId}] 浏览器 WebSocket 未就绪: ${browserWs.readyState}`);
      this.cleanupWebSocketConnection(wsRequestId);
      return;
    }

    try {
      // 新协议给原始 Buffer；旧客户端可能仍送 base64 字符串
      const dataBuffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'base64');

      let sendBinary = isBinary;
      if (typeof sendBinary !== 'boolean') {
        // 旧客户端没有 binary 标志：能按 UTF-8 还原就发文本帧，否则发二进制
        const asText = dataBuffer.toString('utf8');
        sendBinary = !Buffer.from(asText, 'utf8').equals(dataBuffer);
      }

      console.log(`[${wsRequestId}] 转发到浏览器 (${path}): ${dataBuffer.length} 字节, binary: ${sendBinary}`);
      browserWs.send(sendBinary ? dataBuffer : dataBuffer.toString('utf8'));

      if (connection) {
        connection.lastActivity = Date.now();
      }
    } catch (error) {
      console.error(`[${wsRequestId}] 转发数据失败:`, error.message);
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }

  handleWebSocketUpgraded(wsRequestId, upgradeInfo) {
    const connection = this.wsConnections.get(wsRequestId);
    const path = connection?.path || 'unknown';
    console.log(`[${wsRequestId}] WebSocket 升级确认: ${upgradeInfo?.status || 'unknown'}, 路径: ${path}`);
    if (connection) {
      connection.upgraded = true;
    }
  }

  handleWebSocketEnd(wsRequestId) {
    const connection = this.wsConnections.get(wsRequestId);
    const path = connection?.path || 'unknown';
    console.log(`[${wsRequestId}] WebSocket 结束, 路径: ${path}`);
    this.cleanupWebSocketConnection(wsRequestId);
  }

  handleWebSocketError(wsRequestId, error) {
    const connection = this.wsConnections.get(wsRequestId);
    const path = connection?.path || 'unknown';
    console.error(`[${wsRequestId}] WebSocket 错误, 路径: ${path}:`, error);
    this.cleanupWebSocketConnection(wsRequestId);
  }

  cleanupWebSocketConnection(wsRequestId) {
    const browserWs = this.wsBrowserMap.get(wsRequestId);
    const conn = this.wsConnections.get(wsRequestId);

    // ★ 清理上行背压状态
    if (conn) {
      conn._upPaused = false;
    }

    if (browserWs && browserWs.readyState === WebSocket.OPEN) {
      try {
        browserWs.close();
      } catch (error) {}
    }
    this.wsBrowserMap.delete(wsRequestId);
    this.wsConnections.delete(wsRequestId);
    console.log(`[${wsRequestId}] WebSocket 连接已清理`);
  }

  async handleRequest(req, res) {
    // SSE 的 sseRequestId 会走二进制协议 encodeString()，必须是字符串；
    // 数字会在 Buffer.from(number) 处抛 ERR_INVALID_ARG_TYPE，请求根本发不到隧道客户端。
    const requestId = `req-${++this.requestCounter}`;
    const startTime = Date.now();

    try {
      const host = req.headers.host;
      if (!host) {
        return this.sendError(res, 400, 'Missing Host header');
      }

      if (req.headers.upgrade === 'websocket') {
        return;
      }

      console.log(`[${requestId}] 处理请求: ${req.method} ${req.url} (Host: ${host})`);

      const clientInfo = this.websocketServer.getClientByDomain(host);
      if (!clientInfo) {
        return this.sendError(res, 404, `No tunnel registered for domain: ${host}`);
      }

      const { clientId, client } = clientInfo;

      if (!client || client.ws.readyState !== WebSocket.OPEN) {
        return this.sendError(res, 503, `Client ${clientId} is not connected`);
      }

      const wantsSSE = this.isSSERequest(req);

      // SSE 已并入流式通道：字节原样透传，event/id/retry/注释/多行 data 都不会被改写。
      // 只有服务端关闭流式或客户端不具备流式能力时，才回退到旧的 SSE 旁路。
      if (this.isStreamingEnabled() && this.clientSupportsStreaming(clientId)) {
        await this.handleStreamingRequest(requestId, req, res, clientId, host, { sse: wantsSSE });
      } else if (wantsSSE) {
        await this.handleSSERequest(requestId, req, res, clientId, host);
      } else {
        await this.handleNormalRequest(requestId, req, res, clientId, host, startTime);
      }

    } catch (error) {
      if (error && error.code === 'BODY_TOO_LARGE') {
        console.warn(`[${requestId}] 请求体超限，返回 413`);
        if (!res.headersSent) this.sendError(res, 413, 'Payload Too Large');
        else { try { res.end(); } catch (e) {} }
        // 先让 413 写回浏览器，再断开请求流
        res.once('finish', () => { try { req.destroy(); } catch (e) {} });
        return;
      }
      console.error(`[${requestId}] 处理请求失败:`, error.message);
      if (!res.headersSent) {
        this.sendError(res, 502, 'Bad Gateway: ' + error.message);
      }
    }
  }

  async handleNormalRequest(requestId, req, res, clientId, domain, startTime) {
    const requestData = await this.buildRequestData(req, domain);
    const response = await this.websocketServer.sendHttpRequest(clientId, requestData);
    this.sendResponse(res, response);

    const endTime = Date.now();
    console.log(`[${requestId}] 请求完成: ${req.method} ${req.url} -> ${response.status} (${endTime - startTime}ms)`);
  }

  // ==================== 流式传输（双向） ====================
  // 上行：浏览器 --chunk--> 服务端 --stream_chunk--> 客户端 --> 本地应用
  // 下行：本地应用 --chunk--> 客户端 --stream_chunk--> 服务端 --> 浏览器
  // 每个方向独立信用窗口（默认 1MB），接收方用 stream_ack 归还额度。

  isStreamingEnabled() {
    return !!(this.config && this.config.streaming && this.config.streaming.enabled);
  }

  clientSupportsStreaming(clientId) {
    const client = this.websocketServer.getClient(clientId);
    return !!(client && client.capabilities && client.capabilities.stream);
  }

  findStreamByRequestId(requestId) {
    for (const st of this.streams.values()) {
      if (st.requestId === requestId) return st;
    }
    return null;
  }

  /**
   * 流式请求也可能收到"经典"整包响应（客户端判断响应体较小，走缓冲路径）。
   * 这里把它接住并清理流状态，避免流悬空到空闲超时。
   */
  tryHandleStreamResponse(message) {
    const st = this.findStreamByRequestId(message.requestId);
    if (!st) return false;
    if (!st.download.headersSent) {
      this.sendResponse(st.res, {
        status: message.status,
        headers: message.headers || {},
        body: message.body,
      });
    }
    if (st.timer) clearTimeout(st.timer);
    this.streams.delete(st.streamId);
    try { st.req.destroy(); } catch (e) {}
    console.log(`[${st.requestId}] 流式请求以小响应路径完成 (${st.streamId})`);
    return true;
  }

  streamOptions() {
    const s = (this.config && this.config.streaming) || {};
    return {
      chunkSize: s.chunkSize || 256 * 1024,
      windowSize: s.windowSize || 1024 * 1024,
      idleTimeoutMs: s.idleTimeoutMs || 60000,
    };
  }

  touchStream(state) {
    state.lastActivity = Date.now();
    if (state.timer) clearTimeout(state.timer);
    const idle = this.streamOptions().idleTimeoutMs;
    state.timer = setTimeout(() => {
      this.cancelStream(state, `idle timeout ${idle}ms`);
    }, idle);
    if (state.timer.unref) state.timer.unref();
  }

  /**
   * SSE 心跳：静默超过 15s 写一行注释，并续期流空闲计时，
   * 避免被 streaming.idleTimeoutMs（默认 60s）掐断，也避免中间层判定空闲。
   */
  startSSEPing(state) {
    this.stopSSEPing(state);
    const interval = 15000;
    state.ssePing = setInterval(() => {
      if (!this.streams.has(state.streamId) || state.download.ended) return this.stopSSEPing(state);
      if (Date.now() - (state.lastDownWrite || 0) < interval) return;
      try {
        if (!state.res.writableEnded) state.res.write(': ping\n\n');
      } catch (e) {
        return this.cancelStream(state, 'sse ping failed');
      }
      this.touchStream(state);
    }, interval);
    if (state.ssePing.unref) state.ssePing.unref();
  }

  stopSSEPing(state) {
    if (state && state.ssePing) {
      clearInterval(state.ssePing);
      state.ssePing = null;
    }
  }
  getStreamClient(state) {
    const client = this.websocketServer.getClient(state.clientId);
    return client && client.ws && client.ws.readyState === WebSocket.OPEN ? client.ws : null;
  }

  async handleStreamingRequest(requestId, req, res, clientId, domain, opts = {}) {
    const opt = this.streamOptions();
    const client = this.websocketServer.getClient(clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      return this.sendError(res, 503, `Client ${clientId} is not connected`);
    }

    const streamId = `st-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const state = {
      streamId,
      requestId,
      clientId,
      domain,
      req,
      res,
      windowSize: opt.windowSize,
      upload: { sent: 0, acked: 0, ended: false, paused: false },
      download: { received: 0, acked: 0, headersSent: false, ended: false, pendingAck: 0 },
      lastActivity: Date.now(),
      timer: null,
      sse: !!(opts && opts.sse),     // SSE：保留事件流语义，禁空闲超时并加心跳
      ssePing: null,
      lastDownWrite: 0,
    };
    this.streams.set(streamId, state);
    this.touchStream(state);

    res.on('close', () => {
      if (!state.download.ended) this.cancelStream(state, 'browser closed');
    });

    const headers = this.sanitizeHeaders(req.headers || {});
    if (req.headers['content-length']) headers['content-length'] = req.headers['content-length'];
    if (req.headers['transfer-encoding']) headers['transfer-encoding'] = req.headers['transfer-encoding'];

    console.log(`[${requestId}] 流式请求开始: ${req.method} ${req.url} (stream ${streamId})`);
    this.sendBinaryMessage(client.ws, {
      type: 'stream_open',
      streamId,
      requestId,
      direction: 'up',
      method: req.method,
      url: req.url,
      headers,
    });

    req.on('data', (chunk) => {
      if (!this.streams.has(streamId)) return;
      const ws = this.getStreamClient(state);
      if (!ws) return this.cancelStream(state, 'client gone');

      state.upload.sent += chunk.length;
      const limit = this.maxBodySize(domain);
      if (limit && state.upload.sent > limit) {
        console.warn(`[${requestId}] 流式上传超限 (${state.upload.sent} > ${limit})，返回 413`);
        if (ws) this.sendBinaryMessage(ws, { type: 'stream_cancel', streamId, reason: 'body too large' });
        this.streams.delete(streamId);
        if (state.timer) clearTimeout(state.timer);
        if (!state.res.headersSent) {
          try {
            state.res.writeHead(413, { 'content-type': 'text/plain; charset=utf-8' });
            state.res.end('Payload Too Large');
            state.res.once('finish', () => { try { req.destroy(); } catch (e) {} });
          } catch (e) {}
        }
        return;
      }

      this.touchStream(state);
      this.sendBinaryMessage(ws, { type: 'stream_chunk', streamId, data: chunk });
      if (state.upload.sent - state.upload.acked >= state.windowSize) {
        state.upload.paused = true;
        req.pause();
      }
    });

    req.on('end', () => {
      if (!this.streams.has(streamId)) return;
      state.upload.ended = true;
      this.touchStream(state);
      const ws = this.getStreamClient(state);
      if (ws) this.sendBinaryMessage(ws, { type: 'stream_end', streamId });
    });

    req.on('error', (e) => this.cancelStream(state, `request error: ${e.message}`));
  }

  handleStreamMessage(message) {
    switch (message.type) {
      case 'stream_open': return this.onDownStreamOpen(message);
      case 'stream_chunk': return this.onDownStreamChunk(message);
      case 'stream_end': return this.onDownStreamEnd(message);
      case 'stream_ack': return this.onUpStreamAck(message);
      case 'stream_cancel': return this.onStreamCancel(message);
      default: return undefined;
    }
  }

  onDownStreamOpen(message) {
    const st = this.streams.get(message.streamId);
    if (!st || message.direction === 'up') return;
    st.download.headersSent = true;
    this.touchStream(st);

    const status = this.validateStatusCode(message.status);
    const HOP_BY_HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'upgrade', 'te', 'trailer', 'proxy-connection']);
    try {
      st.res.statusCode = status;
      for (const [k, v] of Object.entries(message.headers || {})) {
        if (HOP_BY_HOP.has(k.toLowerCase())) continue;
        if (Array.isArray(v)) {
          if (v.length) st.res.setHeader(k, v);
        } else if (v !== undefined && v !== null) {
          st.res.setHeader(k, String(v));
        }
      }
      // 分块下发：去掉长度相关/逐跳头，显式声明 chunked
      // （只 remove 不够：Node 在某些组合下会退化成 close-delimited，客户端可能读不满）
      st.res.removeHeader('content-length');
      st.res.setHeader('Transfer-Encoding', 'chunked');
      if (!st.res.getHeader('access-control-allow-origin')) {
        const origin = (st.res.req && st.res.req.headers && st.res.req.headers.origin) || '';
        st.res.setHeader('Access-Control-Allow-Origin', origin || '*');
        if (origin) st.res.setHeader('Vary', 'Origin');
      }

      // SSE：保留上游 content-type，禁缓存/禁代理缓冲，并立刻 flush 头部
      const ctype = String(st.res.getHeader('content-type') || '');
      if (st.sse || ctype.includes('text/event-stream')) {
        st.sse = true;
        if (!st.res.getHeader('cache-control')) st.res.setHeader('Cache-Control', 'no-cache');
        st.res.setHeader('X-Accel-Buffering', 'no');
        st.res.flushHeaders();
        this.startSSEPing(st);
      }
    } catch (e) {
      console.error(`[流式] ${st.streamId} 写响应头失败: ${e.message}`);
      this.cancelStream(st, 'bad headers');
    }
  }

  onDownStreamChunk(message) {
    const st = this.streams.get(message.streamId);
    if (!st || !st.download.headersSent) return;

    const buf = message.data || Buffer.alloc(0);
    st.download.received += buf.length;
    st.lastDownWrite = Date.now();
    this.touchStream(st);

    let ok = true;
    try {
      ok = st.res.write(buf);
    } catch (e) {
      return this.cancelStream(st, 'write failed');
    }

    if (ok) {
      st.download.acked += buf.length;
      const ws = this.getStreamClient(st);
      if (ws && buf.length) {
        this.sendBinaryMessage(ws, { type: 'stream_ack', streamId: st.streamId, credit: buf.length });
      }
    } else {
      st.download.pendingAck += buf.length;
      st.res.once('drain', () => {
        const pending = st.download.pendingAck;
        st.download.pendingAck = 0;
        st.download.acked += pending;
        const ws = this.getStreamClient(st);
        if (ws && pending) {
          this.sendBinaryMessage(ws, { type: 'stream_ack', streamId: st.streamId, credit: pending });
        }
        this.touchStream(st);
      });
    }
  }

  onDownStreamEnd(message) {
    const st = this.streams.get(message.streamId);
    if (!st) return;
    st.download.ended = true;
    this.stopSSEPing(st);
    if (st.timer) clearTimeout(st.timer);
    try { st.res.end(); } catch (e) {}
    this.streams.delete(st.streamId);
    console.log(`[${st.requestId}] 流式完成 (上行 ${st.upload.sent}B, 下行 ${st.download.received}B)`);
  }

  onUpStreamAck(message) {
    const st = this.streams.get(message.streamId);
    if (!st) return;
    st.upload.acked += message.credit || 0;
    this.touchStream(st);
    if (st.upload.paused && st.upload.sent - st.upload.acked < st.windowSize) {
      st.upload.paused = false;
      try { st.req.resume(); } catch (e) {}
    }
  }

  onStreamCancel(message) {
    const st = this.streams.get(message.streamId);
    if (!st) return;
    this.cancelStream(st, message.reason || 'peer cancelled');
  }

  cancelStream(state, reason) {
    if (!this.streams.has(state.streamId)) return;
    this.streams.delete(state.streamId);
    this.stopSSEPing(state);
    if (state.timer) clearTimeout(state.timer);
    try {
      const ws = this.getStreamClient(state);
      if (ws) this.sendBinaryMessage(ws, { type: 'stream_cancel', streamId: state.streamId, reason: String(reason || '') });
    } catch (e) {}
    try {
      if (!state.download.ended) {
        if (!state.res.headersSent) {
          // 响应头还没发：返回规范的 502，避免直接 RST 被 nginx 记成 connection reset
          state.res.writeHead(502, { 'content-type': 'text/plain; charset=utf-8' });
          state.res.end(`Bad Gateway: ${reason}`);
        } else {
          state.res.destroy();
        }
      }
    } catch (e) {}
    try { state.req.destroy(); } catch (e) {}
    console.warn(`[${state.requestId}] 流式取消: ${reason}`);
  }

  async handleSSERequest(requestId, req, res, clientId, domain) {
    console.log(`[${requestId}] 处理SSE请求: ${req.url}`);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept'
    });

    res.write(': SSE connection established via tunnel proxy\n\n');

    const requestData = await this.buildRequestData(req, domain);

    this.sseConnections.set(requestId, {
      res,
      clientId,
      domain,
      startTime: Date.now(),
      lastActivity: Date.now(),
      requestData
    });

    req.on('close', () => {
      console.log(`[${requestId}] SSE客户端断开连接`);
      this.cleanupSSEConnection(requestId);
    });

    req.on('error', (error) => {
      console.error(`[${requestId}] SSE连接错误:`, error);
      this.cleanupSSEConnection(requestId);
    });

    try {
      const response = await this.websocketServer.sendHttpRequest(clientId, {
        ...requestData,
        isSSE: true,
        sseRequestId: requestId
      });

      if (response.body) {
        const bodyBuffer = Buffer.isBuffer(response.body) ? response.body : Buffer.from(response.body, 'utf8');
        res.write(bodyBuffer);
      }

    } catch (error) {
      console.error(`[${requestId}] 发送SSE初始请求失败:`, error);
      res.write(`event: error\ndata: ${JSON.stringify({ error: error.message })}\n\n`);
      this.cleanupSSEConnection(requestId);
    }
  }

  isSSERequest(req) {
    const acceptHeader = req.headers.accept || '';
    return acceptHeader.includes('text/event-stream');
  }

  /**
   * 请求体上限。支持按域名覆盖：
   *   limits.maxBodySize            全局默认
   *   limits.maxBodySizeByDomain    { 'files.example.com': 1073741824 }
   * 0 表示不限制。
   */
  maxBodySize(domain) {
    const l = (this.config && this.config.limits) || {};
    const byDomain = l.maxBodySizeByDomain || {};
    if (domain && Object.prototype.hasOwnProperty.call(byDomain, domain)) {
      return Number(byDomain[domain]) || 0;
    }
    return Number(l.maxBodySize) || 0;
  }

  async buildRequestData(req, domain) {
    const limit = this.maxBodySize(domain);
    return new Promise((resolve, reject) => {
      const chunks = [];
      let size = 0;
      req.on('data', (chunk) => {
        size += chunk.length;
        if (limit && size > limit) {
          const err = new Error(`request body exceeds limit ${limit} bytes`);
          err.code = 'BODY_TOO_LARGE';
          req.pause();
          reject(err);
          return;
        }
        chunks.push(chunk);
      });
      req.on('end', () => {
        const body = Buffer.concat(chunks);
        resolve({
          method: req.method,
          url: req.url,
          headers: this.sanitizeHeaders(req.headers),
          body: body,
          remoteAddress: req.socket.remoteAddress,
          timestamp: new Date().toISOString()
        });
      });
      req.on('error', reject);
    });
  }

  sanitizeHeaders(headers) {
    const sanitized = { ...headers };
    delete sanitized['proxy-connection'];
    delete sanitized['proxy-authenticate'];
    delete sanitized['proxy-authorization'];
    delete sanitized['content-length'];

    sanitized['x-forwarded-for'] = headers['x-forwarded-for']
      ? `${headers['x-forwarded-for']}, ${headers['x-real-ip'] || headers['remote-address'] || 'unknown'}`
      : headers['x-real-ip'] || headers['remote-address'] || 'unknown';

    sanitized['x-forwarded-proto'] = headers['x-forwarded-proto'] || 'http';
    sanitized['x-forwarded-host'] = headers['host'] || '';

    return sanitized;
  }

  applyCorsHeaders(res, headers) {
    const origin = (res.req && res.req.headers && res.req.headers.origin) || '';
    if (!headers['access-control-allow-origin']) {
      if (origin) {
        // 回显 Origin：既满足带凭据的跨域，也避免 `*` + credentials 的非法组合
        headers['access-control-allow-origin'] = origin;
        const v = headers['vary'];
        if (!v) headers['vary'] = 'Origin';
        else if (!String(v).toLowerCase().includes('origin')) headers['vary'] = `${v}, Origin`;
      } else {
        headers['access-control-allow-origin'] = '*';
      }
    }
    if (!headers['access-control-allow-methods']) {
      headers['access-control-allow-methods'] = 'GET, POST, PUT, DELETE, OPTIONS, PATCH';
    }
    if (!headers['access-control-allow-headers']) {
      headers['access-control-allow-headers'] = 'Content-Type, Authorization, X-Requested-With, Accept, Origin, X-Session-ID, X-Request-ID';
    }
    if (!headers['access-control-allow-credentials']) {
      headers['access-control-allow-credentials'] = 'true';
    }
    if (!headers['access-control-max-age']) {
      headers['access-control-max-age'] = '86400';
    }
  }

  setResponseHeader(res, key, value) {
    if (Array.isArray(value)) res.setHeader(key, value.map(String));
    else if (value !== undefined && value !== null) res.setHeader(key, String(value));
  }

  sendResponse(res, response) {
    try {
      let statusCode = this.validateStatusCode(response.status);
      statusCode = Math.floor(statusCode);

      if (statusCode < 100 || statusCode > 599) {
        console.warn(`状态码 ${statusCode} 超出有效范围，使用 200`);
        statusCode = 200;
      }

      const headers = response.headers || {};

      // CORS：有 Origin 就回显（允许带凭据），没有才用 *
      this.applyCorsHeaders(res, headers);

      if (statusCode === 204 || statusCode === 304) {
        res.statusCode = statusCode;

        const allowedHeaders = [
          'access-control-allow-origin',
          'access-control-allow-methods',
          'access-control-allow-headers',
          'access-control-allow-credentials',
          'access-control-max-age',
          'access-control-expose-headers',
          'cache-control',
          'etag',
          'last-modified',
          'vary',
          'date',
          'expires',
          'x-'
        ];

        for (const [key, value] of Object.entries(headers)) {
          if (value === undefined || value === null) continue;
          const lowerKey = key.toLowerCase();
          if (lowerKey.startsWith('content-')) continue;

          const isAllowed = allowedHeaders.some(prefix => {
            if (prefix.endsWith('*')) {
              return lowerKey.startsWith(prefix.slice(0, -1));
            }
            return lowerKey === prefix;
          });

          if (isAllowed) {
            this.setResponseHeader(res, key, value);
          }
        }

        if (!res.getHeader('access-control-allow-origin')) {
          res.setHeader('Access-Control-Allow-Origin', '*');
        }
        if (!res.getHeader('access-control-allow-methods')) {
          res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
        }
        if (!res.getHeader('access-control-allow-headers')) {
          res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, Accept, Origin');
        }
        if (!res.getHeader('access-control-allow-credentials')) {
          res.setHeader('Access-Control-Allow-Credentials', 'true');
        }
        if (!res.getHeader('access-control-max-age')) {
          res.setHeader('Access-Control-Max-Age', '86400');
        }

        console.log(`[sendResponse] ${statusCode} 响应，已设置 CORS 头`);
        res.end();
        return;
      }

      res.statusCode = statusCode;

      if (response.body) {
        let bodyBuffer;
        if (Buffer.isBuffer(response.body)) {
          bodyBuffer = response.body;
        } else if (typeof response.body === 'string') {
          bodyBuffer = Buffer.from(response.body, 'utf8');
        } else if (response.body && typeof response.body === 'object') {
          bodyBuffer = Buffer.from(JSON.stringify(response.body), 'utf8');
        } else {
          bodyBuffer = Buffer.alloc(0);
        }

        if (bodyBuffer.length > 0) {
          headers['content-length'] = bodyBuffer.length.toString();
        } else {
          delete headers['content-length'];
        }

        for (const [key, value] of Object.entries(headers)) {
          this.setResponseHeader(res, key, value);
        }

        if (!res.getHeader('access-control-allow-origin')) {
          res.setHeader('Access-Control-Allow-Origin', '*');
        }
        if (!res.getHeader('access-control-allow-methods')) {
          res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
        }

        if (bodyBuffer.length === 0) {
          res.end();
        } else {
          res.end(bodyBuffer);
        }
      } else {
        delete headers['content-length'];

        for (const [key, value] of Object.entries(headers)) {
          this.setResponseHeader(res, key, value);
        }

        if (!res.getHeader('access-control-allow-origin')) {
          res.setHeader('Access-Control-Allow-Origin', '*');
        }
        if (!res.getHeader('access-control-allow-methods')) {
          res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
        }

        res.end();
      }
    } catch (error) {
      console.error('发送响应失败:', error);
      if (!res.headersSent) {
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
        this.sendError(res, 500, 'Internal Server Error');
      }
    }
  }

  sendError(res, statusCode, message) {
    const validStatusCode = this.validateStatusCode(statusCode);
    if (!res.headersSent) {
      res.writeHead(validStatusCode, {
        'Content-Type': 'text/plain',
        'Cache-Control': 'no-cache'
      });
      res.end(message);
    }
  }

  handleSSEData(requestId, data) {
    const sseConnection = this.sseConnections.get(requestId);
    if (!sseConnection) {
      console.warn(`[${requestId}] SSE连接不存在或已关闭`);
      return;
    }

    if (!sseConnection.res.writable || sseConnection.res.finished) {
      console.warn(`[${requestId}] SSE响应不可写或已结束`);
      this.cleanupSSEConnection(requestId);
      return;
    }

    try {
      sseConnection.lastActivity = Date.now();

      if (typeof data === 'string') {
        sseConnection.res.write('data: ');
        sseConnection.res.write(data);
        sseConnection.res.write('\n\n');
      } else if (Buffer.isBuffer(data)) {
        sseConnection.res.write('data: ');
        sseConnection.res.write(data);
        sseConnection.res.write('\n\n');
      } else if (typeof data === 'object') {
        const sseMessage = this.formatSSEMessage(data);
        sseConnection.res.write(sseMessage);
      }

    } catch (error) {
      console.error(`[${requestId}] 发送SSE数据失败:`, error);
      if (error.code === 'ECONNRESET' || error.message.includes('aborted')) {
        console.log(`[${requestId}] 客户端断开连接，清理SSE连接`);
      }
      this.cleanupSSEConnection(requestId);
    }
  }

  formatSSEMessage(data) {
    let message = '';
    if (data.event) message += `event: ${data.event}\n`;
    if (data.id) message += `id: ${data.id}\n`;
    if (data.retry) message += `retry: ${data.retry}\n`;
    if (data.data !== undefined) {
      const dataStr = typeof data.data === 'string' ? data.data : JSON.stringify(data.data);
      const lines = dataStr.split('\n');
      lines.forEach(line => {
        message += `data: ${line}\n`;
      });
    }
    message += '\n';
    return message;
  }

  cleanupSSEConnection(requestId) {
    const sseConnection = this.sseConnections.get(requestId);
    if (sseConnection) {
      try {
        if (sseConnection.res.writable && !sseConnection.res.finished) {
          sseConnection.res.write(': SSE connection closed\n\n');
          sseConnection.res.end();
        }
      } catch (error) {
        console.warn(`[${requestId}] 结束SSE连接时出错:`, error.message);
      }

      this.sseConnections.delete(requestId);
      console.log(`[${requestId}] SSE连接已清理`);

      if (this.websocketServer && this.websocketServer.handleSSEConnectionClose) {
        this.websocketServer.handleSSEConnectionClose(requestId);
      }
    }
  }

  startConnectionCleanup() {
    setInterval(() => {
      const now = Date.now();
      const timeout = 30 * 60 * 1000;

      for (const [wsRequestId, connection] of this.wsConnections.entries()) {
        if (now - connection.lastActivity > timeout) {
          console.log(`[${wsRequestId}] WebSocket连接超时，清理，路径: ${connection.path}`);
          this.cleanupWebSocketConnection(wsRequestId);
        }
      }
    }, 60000);
  }

  getWebSocketConnections() {
    return Array.from(this.wsConnections.entries()).map(([id, conn]) => ({
      id,
      clientId: conn.clientId,
      domain: conn.domain,
      path: conn.path,
      duration: Date.now() - conn.createdAt,
      idleTime: Date.now() - conn.lastActivity,
      upgraded: conn.upgraded || false,
      // ★ 背压状态
      upPaused: conn._upPaused || false,
      downPaused: this._isClientPaused(conn.clientId)
    }));
  }

  stop() {
    console.log('正在关闭代理服务器...');

    // ★ 清理定时器
    if (this._downBpTimer) clearInterval(this._downBpTimer);
    if (this._upBpTimer) clearInterval(this._upBpTimer);
    if (this._bpLogTimer) clearInterval(this._bpLogTimer);

    for (const requestId of this.sseConnections.keys()) {
      this.cleanupSSEConnection(requestId);
    }

    for (const wsRequestId of this.wsConnections.keys()) {
      this.cleanupWebSocketConnection(wsRequestId);
    }

    if (this.httpServer) {
      this.httpServer.close();
    }
    if (this.httpsServer) {
      this.httpsServer.close();
    }
  }
}

module.exports = ProxyServer;