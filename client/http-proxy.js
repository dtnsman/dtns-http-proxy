// http-proxy.js - HTTP代理
const http = require('http');
const https = require('https');
const url = require('url');
const EventEmitter = require('events');
const crypto = require('crypto');
const WebSocket = require('ws');
const { BinaryCodec } = require('./protocol');

class HttpProxy extends EventEmitter {
  constructor(localConfig, tunnelConfig = {}) {
    super();
    this.localConfig = localConfig;
    this.tunnelConfig = tunnelConfig;
    
    // 关闭 keep-alive：上游（本地应用）的 keepAliveTimeout 通常只有 5s，
    // 低频请求（如上传）容易复用到对端刚关闭的 socket，表现为 socket hang up。
    // 走本机回环，新建连接的开销可以忽略。
    this.httpAgent = new http.Agent({
      keepAlive: false,
      maxSockets: 200,
      timeout: 600000
    });
    
    this.httpsAgent = new https.Agent({
      keepAlive: false,
      maxSockets: 200,
      timeout: 600000
    });
    
    this.sseConnections = new Map();
    this.wsConnections = new Map();
    // 流式传输：streamId -> 状态（上行写 req、下行读 res，各自窗口）
    this.streams = new Map();
  }

  /**
   * 协议里 close code 只占 1 字节（1000 会被截断成 232），
   * 直接把它交给 ws.close() 会抛 RangeError，连接反而关不掉。
   * 统一收敛成合法值，避免"收到关闭通知但本地 WS 一直不断"。
   */
  static normalizeCloseCode(code, fallback = 1000) {
    const n = Number(code);
    if (!Number.isInteger(n)) return fallback;
    // 1005/1006/1015 是保留码，不能出现在 close frame 里
    if (n === 1005 || n === 1006 || n === 1015) return fallback;
    if (n < 1000 || n > 4999) return fallback;
    return n;
  }
  
  async forwardRequest(requestData, wsClient = null) {
    const { 
      method, 
      url: requestUrl, 
      headers, 
      body: base64Body,
      remoteAddress,
      isSSE = false,
      sseRequestId = null,
      isWebSocket = false,
      wsRequestId = null
    } = requestData;
    
    const parsedUrl = new URL(requestUrl, `${this.localConfig.protocol}://${this.localConfig.host}:${this.localConfig.port}`);
    
    const isUpgradeToWebSocket = headers['upgrade'] && 
                                  headers['upgrade'].toLowerCase() === 'websocket';
    
    if (isUpgradeToWebSocket || isWebSocket) {
      console.log(`[HttpProxy] 检测到 WebSocket 请求，路径: ${parsedUrl.pathname}`);
      return this.handleWebSocketUpgrade(requestData, wsClient, parsedUrl);
    }
    
    // SSE 已并入流式通道：只有服务端显式标记 isSSE（旧服务端 / 无流式能力的回退路径）
    // 才走旁路；否则按普通请求处理，由流式通道原样透传事件流。
    if (isSSE) {
      return this.handleSSERequest(requestData, wsClient, parsedUrl);
    }
    
    return this.handleHttpRequest(requestData, wsClient, parsedUrl);
  }
  
  async handleHttpRequest(requestData, wsClient, parsedUrl) {
    return new Promise((resolve, reject) => {
      const { method, headers, body: base64Body } = requestData;
      
      let newHeaders = this.prepareHeaders(headers, parsedUrl);
      
      // body 直接作为 Buffer
      let bodyBuffer;
      if (base64Body) {
        if (Buffer.isBuffer(base64Body)) {
          bodyBuffer = base64Body;
          newHeaders['Content-Length'] = bodyBuffer.length;
        } else {
          bodyBuffer = Buffer.from(base64Body, 'utf8');
          newHeaders['Content-Length'] = bodyBuffer.length;
        }
      }

      const options = {
        hostname: parsedUrl.hostname || this.localConfig.host,
        port: parsedUrl.port || this.localConfig.port,
        path: parsedUrl.pathname + parsedUrl.search,
        method: method,
        headers: newHeaders,
        agent: parsedUrl.protocol === 'https:' ? this.httpsAgent : this.httpAgent,
        timeout: 600000
      };
      
      if (this.tunnelConfig.auth && this.tunnelConfig.auth.enabled) {
        this.applyAuthentication(options);
      }
      
      const protocol = parsedUrl.protocol === 'https:' ? https : http;
      
      const req = protocol.request(options, (res) => {
        const buildRespHeaders = () => this.rewriteResponseHeaders(this.filterResponseHeaders(res.headers), headers, parsedUrl);
        const isSSEResponse = res.headers['content-type'] && 
                              res.headers['content-type'].includes('text/event-stream');
        
        if (isSSEResponse) {
          this.handleSSEResponse(null, res, wsClient, requestData);
          resolve({
            status: res.statusCode,
            headers: buildRespHeaders(),
            body: null,
            isSSE: true
          });
        } else {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          
          // http-proxy.js - handleHttpRequest 方法中

          res.on('end', () => {
            const body = Buffer.concat(chunks);
            console.log(`[HttpProxy] 响应体大小: ${body.length} 字节, 状态码: ${res.statusCode}`);
            
            // ========== 304 状态码：body 必须为空 ==========
            if (res.statusCode === 304 || res.statusCode === 204) {
              resolve({
                status: res.statusCode,
                headers: buildRespHeaders(),
                body: null  // 304/204 body 为空
              });
            } else if (body.length === 0) {
              // 其他状态码但 body 为空
              resolve({
                status: res.statusCode,
                headers: buildRespHeaders(),
                body: null
              });
            } else {
              resolve({
                status: res.statusCode,
                headers: buildRespHeaders(),
                body: body
              });
            }
          });
          res.on('error', reject);
        }
      });

      req.setTimeout(600000, () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });

      req.on('socket', (socket) => {
        socket.setTimeout(600000);
      });
      
      req.on('error', (error) => {
        console.error(`请求错误: ${error.message}`);
        reject(error);
      });
      
      req.on('timeout', () => {
        console.error(`请求超时: ${parsedUrl.toString()}`);
        req.destroy();
        reject(new Error('Request timeout'));
      });
      
      if (bodyBuffer && bodyBuffer.length > 0) {
        req.write(bodyBuffer);
      }
      
      req.end();
    });
  }

  async handleWebSocketUpgrade(requestData, wsClient, parsedUrl) {
    const { headers, method, url: requestUrl, wsRequestId } = requestData;
    const requestId = wsRequestId || `ws-${Date.now()}-${Math.random().toString(36).substr(2, 10)}`;
    
    const targetPath = parsedUrl.pathname + parsedUrl.search;
    
    console.log(`[WebSocket] 处理升级请求: ${requestId}`);
    console.log(`[WebSocket] 目标路径: ${targetPath}`);
    
    return new Promise((resolve, reject) => {
      const targetUrl = `ws://${parsedUrl.hostname}:${parsedUrl.port}${targetPath}`;
      console.log(`[WebSocket] 连接本地 WS 服务: ${targetUrl}`);
      
      const wsOptions = {
        headers: {}
      };
      
      if (headers['user-agent'] && headers['user-agent'] !== 'undefined') {
        wsOptions.headers['User-Agent'] = headers['user-agent'];
      }
      // 关键：浏览器升级 WebSocket 时会带 Cookie 做鉴权，原来这里没有转发，
      // 上游一律回 401，页面就一直停在"重新连接中"。
      if (headers['cookie'] && headers['cookie'] !== 'undefined') {
        wsOptions.headers['Cookie'] = headers['cookie'];
      }
      // 子协议必须作为 ws 构造参数传入（放 header 里会被库判定为'未请求却收到子协议'）
      const offeredProtocols = String(headers['sec-websocket-protocol'] || '')
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s && s !== 'undefined');
      if (offeredProtocols.length) {
        wsOptions.protocols = offeredProtocols;
      }
      // 与 HTTP 路径同一套策略：默认保留原始 Host / Origin，按隧道开关决定是否改写
      const tunnelCfg = this.tunnelConfig || {};
      const rewriteOrigin = tunnelCfg.rewriteOrigin === true;
      const rewriteHost = tunnelCfg.rewriteHost === true;
      const upstreamAuthority = `${parsedUrl.hostname}:${parsedUrl.port}`;

      // Host：默认透传浏览器原始 Host（ws 库不会覆盖已提供的 Host）
      if (headers['host'] && headers['host'] !== 'undefined' && !rewriteHost) {
        wsOptions.headers['Host'] = headers['host'];
      }
      // Origin：默认透传；开启 rewriteOrigin 时才改写成上游地址
      if (headers['origin'] && headers['origin'] !== 'undefined') {
        wsOptions.headers['Origin'] = rewriteOrigin
          ? `http://${upstreamAuthority}`
          : headers['origin'];
      }
      
      const localWs = new WebSocket(targetUrl, offeredProtocols.length ? offeredProtocols : undefined, wsOptions);
      
      const connection = {
        localWs,
        wsClient,
        wsRequestId: requestId,
        createdAt: Date.now(),
        lastActivity: Date.now()
      };
      this.wsConnections.set(requestId, connection);
      
      // 记录上游 101 的响应头（子协议、Set-Cookie 等），稍后原样回传给服务端
      let opened = false;
      let upgradeHeaders = null;
      localWs.on('upgrade', (res) => {
        upgradeHeaders = this.filterResponseHeaders(res.headers || {});
      });

      localWs.on('open', () => {
        opened = true;
        console.log(`[WebSocket] 本地 WS 连接成功: ${requestId}`);
        
        if (wsClient && wsClient.readyState === 1) {
          wsClient.send(JSON.stringify({
            type: 'websocket_upgraded',
            wsRequestId: requestId,
            status: 101
          }));
        }
        
        resolve({
          status: 101,
          headers: upgradeHeaders || { 'upgrade': 'websocket', 'connection': 'upgrade' },
          isWebSocket: true,
          wsRequestId: requestId
        });
      });
      
      // 上游明确拒绝升级（401/403/404）：把真实状态码回传给服务端，
      // 而不是让它等到超时或退化成 502。
      localWs.on('unexpected-response', (req2, res2) => {
        console.log(`[WebSocket] 上游拒绝升级: ${requestId} -> ${res2.statusCode}`);
        try { res2.resume(); } catch (e) {}
        this.cleanupWebSocketConnection(requestId);
        resolve({
          status: res2.statusCode || 502,
          headers: this.filterResponseHeaders(res2.headers || {}),
          isWebSocket: true,
          wsRequestId: requestId,
        });
      });
      localWs.on('message', (data, isBinary) => {
        const payload = Buffer.isBuffer(data) ? data : Buffer.from(data);
        console.log(`[WebSocket] 本地 WS → 服务端: ${requestId}, 数据长度: ${payload.length}, binary: ${!!isBinary}`);
        
        if (wsClient && wsClient.readyState === 1) {
          try {
            // 直接走二进制帧传原始字节：不再 base64 + JSON.stringify，
            // 上行带宽不再膨胀 1/3，也省掉两次 JSON 编解码。
            wsClient.send(BinaryCodec.encode({
              type: 'websocket_data',
              wsRequestId: requestId,
              data: payload,
              binary: !!isBinary
            }));
          } catch (error) {
            console.error(`[WebSocket] 发送数据到隧道失败: ${requestId} - ${error.message}`);
          }
          connection.lastActivity = Date.now();
        }
      });
      
      localWs.on('close', (code, reason) => {
        let reasonStr = '';
        if (reason) {
          reasonStr = Buffer.isBuffer(reason) ? reason.toString() : String(reason);
        }
        
        console.log(`[WebSocket] 本地 WS 关闭: ${requestId} - ${code}, reason: ${reasonStr}`);
        
        if (wsClient && wsClient.readyState === 1) {
          let closeCode = code;
          if (!closeCode || closeCode === 1005 || closeCode === 1006) {
            closeCode = 1000;
          }
          wsClient.send(JSON.stringify({
            type: 'websocket_close',
            wsRequestId: requestId,
            code: closeCode,
            reason: reasonStr,
            timestamp: Date.now()
          }));
        }
        
        this.cleanupWebSocketConnection(requestId);
      });
      
      localWs.on('error', (error) => {
        console.error(`[WebSocket] 本地 WS 错误: ${requestId} - ${error.message}`);

        if (!opened) {
          // 握手阶段失败：只走 reject，避免先发 websocket_error 把服务端 pending 清掉
          this.cleanupWebSocketConnection(requestId);
          reject(new Error(`local websocket failed: ${error.message}`));
          return;
        }

        if (wsClient && wsClient.readyState === 1) {
          wsClient.send(JSON.stringify({
            type: 'websocket_error',
            wsRequestId: requestId,
            error: error.message,
            timestamp: Date.now()
          }));
        }

        this.cleanupWebSocketConnection(requestId);
        // 让服务端立刻拿到失败结果，而不是等 10 秒超时
        reject(new Error(`local websocket failed: ${error.message}`));
      });
      
      const timeout = setTimeout(() => {
        if (localWs.readyState !== WebSocket.OPEN) {
          console.error(`[WebSocket] 连接超时: ${requestId}`);
          localWs.close();
          reject(new Error('WebSocket connection timeout'));
        }
      }, 10000);
      
      localWs.once('open', () => {
        clearTimeout(timeout);
      });
    });
  }

  handleWebSocketData(wsRequestId, data, isBinary) {
    const connection = this.wsConnections.get(wsRequestId);
    if (!connection || !connection.localWs) {
      console.warn(`[WebSocket] 连接不存在: ${wsRequestId}`);
      return;
    }
    
    if (connection.localWs.readyState === WebSocket.OPEN) {
      try {
        // 新协议下 data 是原始 Buffer；旧协议/旧服务端仍可能给 base64 字符串
        const dataBuffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'base64');
        console.log(`[WebSocket] 服务端 → 本地 WS: ${wsRequestId}, 数据长度: ${dataBuffer.length}, binary: ${isBinary}`);

        let sendBinary = isBinary;
        if (typeof sendBinary !== 'boolean') {
          // 对端没有 binary 标志（旧版本）：能按 UTF-8 还原就发文本帧，否则发二进制
          const asText = dataBuffer.toString('utf8');
          sendBinary = !Buffer.from(asText, 'utf8').equals(dataBuffer);
        }
        connection.localWs.send(sendBinary ? dataBuffer : dataBuffer.toString('utf8'));
        connection.lastActivity = Date.now();
      } catch (error) {
        console.error(`[WebSocket] 发送数据到本地 WS 失败: ${wsRequestId} - ${error.message}`);
      }
    } else {
      console.warn(`[WebSocket] 本地 WS 未就绪: ${wsRequestId}, state: ${connection.localWs.readyState}`);
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }

  closeWebSocketConnection(wsRequestId) {
    const connection = this.wsConnections.get(wsRequestId);
    if (connection) {
      if (connection.localWs && connection.localWs.readyState === WebSocket.OPEN) {
        try {
          connection.localWs.close(1000, 'Client closed');
        } catch (error) {
          console.error(`[WebSocket] 关闭本地 WS 失败: ${wsRequestId}`, error.message);
          try {
            connection.localWs.terminate();
          } catch (e) {}
        }
      }
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }

  cleanupWebSocketConnection(wsRequestId) {
    const connection = this.wsConnections.get(wsRequestId);
    if (connection) {
      try {
        if (connection.localWs) {
          if (connection.localWs.readyState === WebSocket.OPEN || 
              connection.localWs.readyState === WebSocket.CONNECTING) {
            connection.localWs.terminate();
          }
        }
      } catch (error) {
        console.error(`[WebSocket] 清理连接失败 ${wsRequestId}:`, error.message);
      }
      this.wsConnections.delete(wsRequestId);
      console.log(`[WebSocket] 连接已清理: ${wsRequestId}`);
    }
  }

  handleWebSocketClose(wsRequestId, message) {
    const { code, reason } = message;
    
    let reasonStr = '';
    if (reason) {
      reasonStr = typeof reason === 'string' ? reason : JSON.stringify(reason);
    }
    
    console.log(`[WebSocket] 收到服务端关闭通知: ${wsRequestId}, code: ${code}, reason: ${reasonStr}`);
    
    const connection = this.wsConnections.get(wsRequestId);
    if (connection && connection.localWs) {
      if (connection.localWs.readyState === WebSocket.OPEN) {
        const closeCode = HttpProxy.normalizeCloseCode(code);
        try {
          connection.localWs.close(closeCode, reasonStr);
        } catch (e) {
          try { connection.localWs.terminate(); } catch (er) {}
        }
      }
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }

  handleWebSocketEnd(message) {
    const { wsRequestId, code, reason } = message;
    
    let reasonStr = '';
    if (reason) {
      reasonStr = typeof reason === 'string' ? reason : JSON.stringify(reason);
    }
    
    console.log(`[WebSocket] 收到服务端结束通知: ${wsRequestId}, code: ${code}`);
    
    const connection = this.wsConnections.get(wsRequestId);
    if (connection && connection.localWs) {
      if (connection.localWs.readyState === WebSocket.OPEN) {
        const closeCode = HttpProxy.normalizeCloseCode(code);
        try {
          connection.localWs.close(closeCode, reasonStr || 'Server ended');
        } catch (e) {
          try { connection.localWs.terminate(); } catch (er) {}
        }
      }
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }

  handleWebSocketError(message) {
    const { wsRequestId, error } = message;
    console.error(`[WebSocket] 收到服务端错误通知: ${wsRequestId}, error: ${error}`);
    
    const connection = this.wsConnections.get(wsRequestId);
    if (connection && connection.localWs) {
      if (connection.localWs.readyState === WebSocket.OPEN) {
        connection.localWs.close(1011, error);
      }
      this.cleanupWebSocketConnection(wsRequestId);
    }
  }
  
  async handleSSERequest(requestData, wsClient, parsedUrl) {
    return new Promise((resolve, reject) => {
      const { method, headers, body: base64Body, sseRequestId } = requestData;
      const requestId = sseRequestId || `sse-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      
      let newHeaders = this.prepareHeaders(headers, parsedUrl);
      delete newHeaders['content-length'];
      
      const options = {
        hostname: parsedUrl.hostname || this.localConfig.host,
        port: parsedUrl.port || this.localConfig.port,
        path: parsedUrl.pathname + parsedUrl.search,
        method: method,
        headers: newHeaders,
        agent: parsedUrl.protocol === 'https:' ? this.httpsAgent : this.httpAgent,
        timeout: 0
      };
      
      const protocol = parsedUrl.protocol === 'https:' ? https : http;
      
      const req = protocol.request(options, (res) => {
        const buildRespHeaders = () => this.rewriteResponseHeaders(this.filterResponseHeaders(res.headers), headers, parsedUrl);
        const isSSEResponse = res.headers['content-type'] && 
                              res.headers['content-type'].includes('text/event-stream');
        
        if (isSSEResponse) {
          this.handleSSEResponse(requestId, res, wsClient, requestData);
          resolve({
            status: res.statusCode,
            headers: buildRespHeaders(),
            body: null,
            isSSE: true,
            sseRequestId: requestId
          });
        } else {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          res.on('end', () => {
            const body = Buffer.concat(chunks);
            resolve({
              status: res.statusCode,
              headers: buildRespHeaders(),
              body: body
            });
          });
          res.on('error', reject);
        }
      });
      
      req.on('error', (error) => {
        console.error(`SSE请求错误: ${error.message}`);
        reject(error);
      });
      
      if (base64Body) {
        const body = Buffer.from(base64Body, 'base64');
        req.write(body);
      }
      
      req.end();
    });
  }
  
  handleSSEResponse(sseRequestId, res, wsClient, requestData) {
    if (!wsClient) {
      console.error('SSE响应需要WebSocket客户端');
      return;
    }
    
    console.log(`处理SSE响应: ${sseRequestId}`);
    
    this.sseConnections.set(sseRequestId, {
      res,
      wsClient,
      startTime: Date.now(),
      lastDataTime: Date.now(),
      requestData
    });
    
    res.setEncoding('utf8');
    
    let buffer = '';
    res.on('data', (chunk) => {
      const connection = this.sseConnections.get(sseRequestId);
      if (connection) {
        connection.lastDataTime = Date.now();
      }
      
      buffer += chunk;
      
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      
      let event = null;
      let data = '';
      let id = null;
      let retry = null;
      
      for (const line of lines) {
        if (line.startsWith('event:')) {
          event = line.substring(6).trim();
        } else if (line.startsWith('data:')) {
          data += line.substring(5).trim() + '\n';
        } else if (line.startsWith('id:')) {
          id = line.substring(3).trim();
        } else if (line.startsWith('retry:')) {
          retry = parseInt(line.substring(6).trim());
        } else if (line.trim() === '') {
          if (data) {
            const sseData = {
              type: 'sse_data',
              sseRequestId,
              data: data.trim(),
              event,
              id,
              retry,
              timestamp: new Date().toISOString()
            };
            
            try {
              if (wsClient.readyState === 1) {
                wsClient.send(JSON.stringify(sseData));
              } else {
                console.warn(`WebSocket连接未就绪，清理SSE连接: ${sseRequestId}`);
                this.cleanupSSEConnection(sseRequestId);
                return;
              }
            } catch (error) {
              console.error(`发送SSE数据失败: ${error.message}`);
              this.cleanupSSEConnection(sseRequestId);
              return;
            }
          }
          
          event = null;
          data = '';
          id = null;
          retry = null;
        }
      }
    });
    
    res.on('end', () => {
      console.log(`SSE连接正常结束: ${sseRequestId}`);
      this.cleanupSSEConnection(sseRequestId);
      
      if (wsClient.readyState === 1) {
        wsClient.send(JSON.stringify({
          type: 'sse_end',
          sseRequestId,
          timestamp: new Date().toISOString()
        }));
      }
    });
    
    res.on('error', (error) => {
      console.error(`SSE连接错误: ${sseRequestId}`, error);
      this.cleanupSSEConnection(sseRequestId);
      
      if (wsClient.readyState === 1) {
        wsClient.send(JSON.stringify({
          type: 'sse_error',
          sseRequestId,
          error: error.message,
          timestamp: new Date().toISOString()
        }));
      }
    });
    
    res.on('close', () => {
      console.log(`SSE请求连接关闭: ${sseRequestId}`);
      this.cleanupSSEConnection(sseRequestId);
    });
    
    this.startSSEHeartbeat(sseRequestId, wsClient);
  }
  
  startSSEHeartbeat(sseRequestId, wsClient) {
    const heartbeatInterval = setInterval(() => {
      const connection = this.sseConnections.get(sseRequestId);
      if (!connection) {
        clearInterval(heartbeatInterval);
        return;
      }
      
      if (wsClient.readyState !== 1) {
        console.log(`WebSocket连接关闭，清理SSE连接: ${sseRequestId}`);
        this.cleanupSSEConnection(sseRequestId);
        clearInterval(heartbeatInterval);
        return;
      }
    }, 30000);
  }
  
  /**
   * 按隧道开关改写响应头：
   *   rewriteLocation     : 把指向上游地址的绝对 Location 改回公网地址
   *   rewriteCookieDomain : 把 Set-Cookie 里的 Domain=上游主机/127.0.0.1 改成公网域名
   * 默认都不改写，保持与 nginx 的 proxy_redirect / proxy_cookie_domain 一致（显式开启才动）。
   */
  rewriteResponseHeaders(headers, requestHeaders, parsedUrl) {
    const cfg = this.tunnelConfig || {};
    if (!headers) return headers;

    const publicHost = (requestHeaders && requestHeaders['host']) || '';
    const publicProto = (requestHeaders && requestHeaders['x-forwarded-proto']) || 'http';
    const upstreamAuthority = `${parsedUrl.hostname || this.localConfig.host}:${parsedUrl.port || this.localConfig.port}`;

    if (cfg.rewriteLocation && headers['location'] && publicHost) {
      try {
        const loc = new URL(String(headers['location']), `${parsedUrl.protocol}//${upstreamAuthority}`);
        if (loc.host === upstreamAuthority) {
          headers['location'] = `${publicProto}://${publicHost}${loc.pathname}${loc.search}${loc.hash}`;
        }
      } catch (e) {}
    }

    if (cfg.rewriteCookieDomain && publicHost) {
      const publicHostname = publicHost.split(':')[0];
      const upstreamHostname = parsedUrl.hostname || this.localConfig.host;
      const esc = upstreamHostname.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const fix = (v) => String(v)
        .replace(new RegExp(`;\\s*Domain=${esc}`, 'gi'), `; Domain=${publicHostname}`)
        .replace(/;\s*Domain=127\.0\.0\.1/gi, `; Domain=${publicHostname}`)
        .replace(/;\s*Domain=localhost/gi, `; Domain=${publicHostname}`);
      const sc = headers['set-cookie'];
      if (Array.isArray(sc)) headers['set-cookie'] = sc.map(fix);
      else if (typeof sc === 'string') headers['set-cookie'] = fix(sc);
    }

    return headers;
  }

  prepareHeaders(headers, parsedUrl) {
    const prepared = { ...headers };

    // 缓存相关头默认**透传**（与 nginx 一致）。
    // 以前无条件删除 cache-control / if-none-match / if-modified-since，会导致永远 200、没有 304。
    // 需要强制刷新时，给隧道加 forceRevalidate: true。
    if (this.tunnelConfig && this.tunnelConfig.forceRevalidate) {
      delete prepared['if-none-match'];
      delete prepared['if-modified-since'];
      delete prepared['cache-control'];
    }

    delete prepared['host'];
    delete prepared['connection'];
    delete prepared['upgrade'];

    // 与 nginx 的语义保持一致：默认**保留**浏览器原始 Host / Origin，只补 X-Forwarded-*；
    // 只有隧道显式配置 rewriteHost / rewriteOrigin 时才改写成上游地址。
    //
    // 原因：两类上游的需求正好相反——
    //   * 同源校验型（如 dsh-web）：要求 Origin 与 Host 一致，改写能救活；
    //   * 回显型 CORS（如 dsh.bld1 的 10998）：把 Origin 回显进 ACAO，改写就会让浏览器判跨域。
    // 所以默认不改，谁需要谁在 config 里打开。
    const tunnelCfg = this.tunnelConfig || {};
    const rewriteOrigin = tunnelCfg.rewriteOrigin === true;
    const rewriteHost = tunnelCfg.rewriteHost === true;

    const upstreamAuthority =
      `${parsedUrl.hostname || this.localConfig.host}:${parsedUrl.port || this.localConfig.port}`;
    const upstreamOrigin = `${parsedUrl.protocol}//${upstreamAuthority}`;

    if (headers['origin']) {
      prepared['origin'] = rewriteOrigin ? upstreamOrigin : headers['origin'];
    }

    if (headers['referer'] && rewriteOrigin) {
      try {
        const ref = new URL(headers['referer']);
        prepared['referer'] = upstreamOrigin + ref.pathname + ref.search;
      } catch (e) {
        delete prepared['referer'];
      }
    }

    if (headers.accept && headers.accept.includes('text/event-stream')) {
      prepared['accept'] = 'text/event-stream';
      prepared['cache-control'] = 'no-cache';
    }
    
     // 设置转发头
    prepared['x-forwarded-for'] = headers['x-forwarded-for'] 
      ? `${headers['x-forwarded-for']}, ${headers['remote-address'] || 'unknown'}`
      : headers['remote-address'] || 'unknown';
    prepared['x-forwarded-proto'] = headers['x-forwarded-proto'] || 'http';
    prepared['x-forwarded-host'] = headers['host'] || '';
    // 默认保留原始 Host（等价 nginx 的 proxy_set_header Host $host）
    prepared['host'] = rewriteHost ? upstreamAuthority : (headers['host'] || upstreamAuthority);

    
    return prepared;
  }
  
  prepareWebSocketHeaders(headers, parsedUrl) {
    const prepared = {};
    
    if (headers['host']) {
      prepared['host'] = headers['host'];
    } else {
      prepared['host'] = `${parsedUrl.hostname || this.localConfig.host}:${parsedUrl.port || this.localConfig.port}`;
    }
    
    prepared['upgrade'] = 'websocket';
    prepared['connection'] = 'upgrade';
    
    if (headers['sec-websocket-key'] && headers['sec-websocket-key'] !== 'undefined') {
      prepared['sec-websocket-key'] = headers['sec-websocket-key'];
    } else {
      prepared['sec-websocket-key'] = crypto.randomBytes(16).toString('base64');
    }
    
    if (headers['sec-websocket-version'] && headers['sec-websocket-version'] !== 'undefined') {
      prepared['sec-websocket-version'] = headers['sec-websocket-version'];
    } else {
      prepared['sec-websocket-version'] = '13';
    }
    
    if (headers['sec-websocket-protocol'] && headers['sec-websocket-protocol'] !== 'undefined') {
      prepared['sec-websocket-protocol'] = headers['sec-websocket-protocol'];
    }
    
    if (headers['sec-websocket-extensions'] && headers['sec-websocket-extensions'] !== 'undefined') {
      prepared['sec-websocket-extensions'] = headers['sec-websocket-extensions'];
    }
    
    return prepared;
  }
  
  applyAuthentication(options) {
    const { username, password } = this.tunnelConfig.auth;
    if (username && password) {
      const auth = Buffer.from(`${username}:${password}`).toString('base64');
      options.headers['authorization'] = `Basic ${auth}`;
    }
  }
  
  // http-proxy.js - filterResponseHeaders 方法

  filterResponseHeaders(headers) {
    const filtered = { ...headers };
    
    delete filtered['connection'];
    delete filtered['upgrade'];
    delete filtered['transfer-encoding'];
    
    // ========== 改进7：保留 CORS 头 ==========
    // 保留所有 Access-Control-* 头
    const corsHeaders = Object.keys(headers).filter(key => 
      key.toLowerCase().startsWith('access-control-')
    );
    // 它们已经被复制到 filtered 中了
    
    if (!filtered['content-type']) {
      filtered['content-type'] = 'application/octet-stream';
    }
    
    return filtered;
  }
  
  cleanupSSEConnection(sseRequestId) {
    const connection = this.sseConnections.get(sseRequestId);
    if (connection) {
      try {
        if (connection.res && !connection.res.destroyed) {
          connection.res.destroy();
        }
      } catch (error) {}
      this.sseConnections.delete(sseRequestId);
      console.log(`SSE连接已清理: ${sseRequestId}`);
    }
  }
  
  // ==================== 流式传输（双向） ====================
  // 上行：服务端 --stream_chunk--> 这里 --req.write--> 本地应用
  // 下行：本地应用 --res--> 这里 --stream_chunk--> 服务端
  // 两个方向各自维护信用窗口，接收方用 stream_ack 归还额度。

  streamOptions() {
    const s = (this.tunnelConfig && this.tunnelConfig.streaming) || {};
    return {
      windowSize: s.windowSize || 1024 * 1024,
      responseThreshold: s.responseThreshold || 1024 * 1024,
      idleTimeoutMs: s.idleTimeoutMs || 60000,
    };
  }

  handleStreamMessage(message, wsClient) {
    switch (message.type) {
      case 'stream_open': return this.onStreamOpenUp(message, wsClient);
      case 'stream_chunk': return this.onStreamChunkUp(message);
      case 'stream_end': return this.onStreamEndUp(message);
      case 'stream_ack': return this.onStreamAckDown(message);
      case 'stream_cancel': return this.onStreamCancel(message);
      default: return undefined;
    }
  }

  onStreamOpenUp(message, wsClient) {
    if (message.direction && message.direction !== 'up') return;

    const opt = this.streamOptions();
    const parsedUrl = new URL(
      message.url,
      `${this.localConfig.protocol}://${this.localConfig.host}:${this.localConfig.port}`
    );
    const headers = this.prepareHeaders(message.headers || {}, parsedUrl);

    const st = {
      streamId: message.streamId,
      requestId: message.requestId,
      domain: (message.headers || {}).host,
      wsClient,
      windowSize: opt.windowSize,
      requestHeaders: message.headers || {},
      parsedUrl,
      upload: { received: 0, acked: 0, pendingAck: 0 },
      download: { sent: 0, acked: 0, paused: false },
      timer: null,
      // SSE 事件流可能长时间静默，不能按普通流的 60s 空闲超时掐断
      sse: String((message.headers || {}).accept || '').includes('text/event-stream'),
    };
    this.streams.set(st.streamId, st);
    this.touchStream(st);

    const protocol = parsedUrl.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsedUrl.hostname || this.localConfig.host,
      port: parsedUrl.port || this.localConfig.port,
      path: parsedUrl.pathname + parsedUrl.search,
      method: message.method || 'GET',
      headers,
      agent: parsedUrl.protocol === 'https:' ? this.httpsAgent : this.httpAgent,
      timeout: 0,
    };

    try {
      st.req = protocol.request(options, (res) => this.handleStreamResponse(st, res));
    } catch (e) {
      return this.cancelStream(st, `invalid request: ${e.message}`, true);
    }

    st.req.on('error', (e) => this.cancelStream(st, `upstream error: ${e.message}`, true));
    console.log(`[流式] 上行开始: ${message.method} ${message.url} (${st.streamId})`);
  }

  onStreamChunkUp(message) {
    const st = this.streams.get(message.streamId);
    if (!st || !st.req) return;

    const buf = message.data || Buffer.alloc(0);
    st.upload.received += buf.length;
    this.touchStream(st);

    let ok = true;
    try {
      ok = st.req.write(buf);
    } catch (e) {
      return this.cancelStream(st, 'write failed', true);
    }

    if (ok) {
      st.upload.acked += buf.length;
      this.ackUp(st, buf.length);
    } else {
      st.upload.pendingAck += buf.length;
      st.req.once('drain', () => {
        const pending = st.upload.pendingAck;
        st.upload.pendingAck = 0;
        st.upload.acked += pending;
        this.ackUp(st, pending);
        this.touchStream(st);
      });
    }
  }

  ackUp(st, credit) {
    if (!credit || !st.wsClient || st.wsClient.readyState !== 1) return;
    try {
      st.wsClient.send(BinaryCodec.encode({ type: 'stream_ack', streamId: st.streamId, credit }));
    } catch (e) {}
  }

  onStreamEndUp(message) {
    const st = this.streams.get(message.streamId);
    if (!st || !st.req) return;
    console.log(`[流式] 上行结束: ${st.streamId} (${st.upload.received}B)`);
    try { st.req.end(); } catch (e) {}
  }

  handleStreamResponse(st, res) {
    const opt = this.streamOptions();
    const cl = parseInt(res.headers['content-length'] || '', 10);
    const buildRespHeaders = () => this.rewriteResponseHeaders(
      this.filterResponseHeaders(res.headers),
      st.requestHeaders || {},
      st.parsedUrl || {
        hostname: this.localConfig.host,
        port: this.localConfig.port,
        protocol: this.localConfig.protocol,
      }
    );

    // 小响应走经典路径：缓冲后整帧回传，保留可重试/可改状态码的语义
    if (Number.isFinite(cl) && cl >= 0 && cl <= opt.responseThreshold) {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        this.cleanupStream(st);
        const body = Buffer.concat(chunks);
        try {
          st.wsClient.send(BinaryCodec.encode({
            type: 'http_response',
            requestId: st.requestId,
            domain: st.domain,
            status: res.statusCode,
            headers: buildRespHeaders(),
            body,
            isSSE: false,
            isWebSocket: false,
            sseRequestId: null,
            wsRequestId: null,
            // 与 sendMessage() 对齐：v2 服务端用 2 字节状态码，否则 401 会被截成 145
            ...(BinaryCodec.useV2Status ? { _version: 2 } : {}),
          }));
        } catch (e) {
          console.error(`[流式] 小响应回传失败: ${e.message}`);
        }
      });
      res.on('error', () => this.cancelStream(st, 'response error', true));
      return;
    }

    // 大响应 / 未知长度：分块下发
    console.log(`[流式] 下行开始: ${st.streamId} status=${res.statusCode} cl=${res.headers['content-length'] || 'none'}`);
    try {
      st.wsClient.send(BinaryCodec.encode({
        type: 'stream_open',
        streamId: st.streamId,
        requestId: st.requestId,
        direction: 'down',
        status: res.statusCode,
        headers: buildRespHeaders(),
      }));
    } catch (e) {
      return this.cancelStream(st, 'send open failed', true);
    }

    st.res = res;

    res.on('data', (chunk) => {
      if (!this.streams.has(st.streamId)) return;
      st.download.sent += chunk.length;
      try {
        st.wsClient.send(BinaryCodec.encode({ type: 'stream_chunk', streamId: st.streamId, data: chunk }));
      } catch (e) {
        return this.cancelStream(st, 'send chunk failed', true);
      }
      if (st.download.sent - st.download.acked >= st.windowSize) {
        st.download.paused = true;
        res.pause();
      }
    });

    res.on('end', () => {
      if (!this.streams.has(st.streamId)) return;
      try { st.wsClient.send(BinaryCodec.encode({ type: 'stream_end', streamId: st.streamId })); } catch (e) {}
      console.log(`[流式] 下行结束: ${st.streamId} (${st.download.sent}B)`);
      this.cleanupStream(st);
    });

    res.on('error', () => this.cancelStream(st, 'response error', true));
  }

  onStreamAckDown(message) {
    const st = this.streams.get(message.streamId);
    if (!st) return;
    st.download.acked += message.credit || 0;
    this.touchStream(st);
    if (st.download.paused && st.res && st.download.sent - st.download.acked < st.windowSize) {
      st.download.paused = false;
      try { st.res.resume(); } catch (e) {}
    }
  }

  onStreamCancel(message) {
    const st = this.streams.get(message.streamId);
    if (!st) return;
    this.cancelStream(st, message.reason || 'peer cancelled', false);
  }

  cancelStream(st, reason, notify) {
    if (!this.streams.has(st.streamId)) return;
    if (notify && st.wsClient && st.wsClient.readyState === 1) {
      try {
        st.wsClient.send(BinaryCodec.encode({
          type: 'stream_cancel',
          streamId: st.streamId,
          reason: String(reason || ''),
        }));
      } catch (e) {}
    }
    try { if (st.req) st.req.destroy(); } catch (e) {}
    try { if (st.res) st.res.destroy(); } catch (e) {}
    this.cleanupStream(st);
    console.warn(`[流式] 取消 ${st.streamId}: ${reason}`);
  }

  cleanupStream(st) {
    if (st.timer) clearTimeout(st.timer);
    this.streams.delete(st.streamId);
  }

  touchStream(st) {
    if (st.sse) {
      // SSE：生命周期由服务端心跳与浏览器断开驱动，不做空闲超时
      if (st.timer) { clearTimeout(st.timer); st.timer = null; }
      return;
    }
    const idle = this.streamOptions().idleTimeoutMs;
    if (st.timer) clearTimeout(st.timer);
    st.timer = setTimeout(() => {
      this.cancelStream(st, `idle timeout ${idle}ms`, true);
    }, idle);
    if (st.timer.unref) st.timer.unref();
  }

  destroy() {
    for (const sseRequestId of this.sseConnections.keys()) {
      this.cleanupSSEConnection(sseRequestId);
    }
    
    for (const wsRequestId of this.wsConnections.keys()) {
      this.cleanupWebSocketConnection(wsRequestId);
    }
    
    this.httpAgent.destroy();
    this.httpsAgent.destroy();
  }
}

module.exports = HttpProxy;