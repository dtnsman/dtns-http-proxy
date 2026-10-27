// websocket-server.js - WebSocket服务器
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { BinaryCodec } = require('./protocol');

class WebSocketServer {
  constructor(port, path, proxyServer) {
    this.wsPort = port;
    this.wsPath = path;
    this.proxyServer = proxyServer;
    this.wss = new WebSocket.Server({ port, path });
    this.clients = new Map();
    this.pendingRequests = new Map();
    this.sseStreams = new Map();
    this.wsStreams = new Map();
    
    this.setupWebSocket();
    this.startCleanupInterval();
    this.startSSEStreamCleanup();
  }
  
  setupWebSocket() {
    this.wss.on('connection', (ws, req) => {
      console.log('新的WebSocket连接建立');
      
      const url = new URL(req.url, `http://${req.headers.host}`);
      const clientId = url.searchParams.get('clientId') || this.generateClientId();
      
      console.log(`客户端 ${clientId} 已连接`);
      
      // 同一 clientId 重复连接：先关掉旧连接，避免它变成孤儿 socket
      const previous = this.clients.get(clientId);
      if (previous && previous.ws && previous.ws !== ws) {
        const wasOpen = previous.ws.readyState === WebSocket.OPEN;
        if (wasOpen) {
          console.log(`客户端 ${clientId} 重复连接，关闭旧连接`);
          try { previous.ws.close(4000, 'replaced by new connection'); } catch (e) {}
        }
        // 旧连接已经 CLOSED = 进程真的没了，缓存的浏览器 WS 必然失效，立即关掉让浏览器重连。
        // 旧连接还 OPEN 则可能是同一个 clientId 的双进程互踢（会每秒重连很多次），
        // 此时不动浏览器，交给客户端“收到未知 ws 数据时回 websocket_close”的自愈逻辑，
        // 否则每次互踢都把所有浏览器连接掐一遍，等于放大成重连风暴。
        if (!wasOpen) {
          this.closeOrphanBrowserSockets(clientId, 'tunnel client reconnected');
        }
      }
      this.clients.set(clientId, {
        ws,
        tunnels: new Map(),
        lastHeartbeat: Date.now(),
        metadata: {
          ip: req.socket.remoteAddress,
          userAgent: req.headers['user-agent'],
          connectedAt: new Date().toISOString()
        },
        stats: {
          totalRequests: 0,
          totalResponses: 0,
          totalErrors: 0,
          bytesSent: 0,
          bytesReceived: 0
        }
      });
      
      this.sendBinaryMessage(ws, {
        type: 'welcome',
        clientId,
        timestamp: new Date().toISOString(),
        // 声明支持的协议版本：客户端据此决定用 1 字节还是 2 字节状态码
        protocolVersion: 2,
        message: 'Connection established. You can now register tunnels.'
      });
      
      ws.on('message', (data) => {
        this.handleMessage(clientId, data);
      });
      
      ws.on('close', () => {
        console.error('websocket-close:',clientId)
        this.handleDisconnect(clientId, ws);
      });
      
      ws.on('error', (error) => {
        console.error(`客户端 ${clientId} WebSocket错误:`, error);
        this.handleDisconnect(clientId, ws);
      });
    });
  }
  
  sendBinaryMessage(ws, message) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    // 只有对端声明了多值头能力才用换行编码数组，否则退回逗号拼接（旧客户端）。
    if (message && message.headers && ws.__multiValueHeaders === true) {
      message.multiValue = true;
    }
    try {
      const buffer = BinaryCodec.encode(message);
      ws.send(buffer);
      return true;
    } catch (error) {
      console.error('发送二进制消息失败:', error);
      return false;
    }
  }

  sendMessage(ws, message) {
    return this.sendBinaryMessage(ws, message);
  }
  
  /** 通知 proxy-server 关掉该客户端遗留的浏览器 WS，触发浏览器重新握手。 */
  closeOrphanBrowserSockets(clientId, reason) {
    if (this.proxyServer && typeof this.proxyServer.closeBrowserSocketsForClient === 'function') {
      try {
        this.proxyServer.closeBrowserSocketsForClient(clientId, reason);
      } catch (e) {
        console.error(`关闭客户端 ${clientId} 的浏览器 WebSocket 失败:`, e.message);
      }
    }
  }

  generateClientId() {
    return `client-${uuidv4().substring(0, 8)}`;
  }
  
  handleMessage(clientId, data) {
    try {
      let message;
      
      if (Buffer.isBuffer(data)) {
        if (data.length >= 4 && data.slice(0, 4).toString() === 'DTNS') {
          message = BinaryCodec.decode(data);
        } else {
          message = JSON.parse(data.toString());
        }
      } else {
        message = JSON.parse(data.toString());
      }
      
      const client = this.clients.get(clientId);
      
      if (!client) {
        console.warn(`未知的客户端 ${clientId}`);
        return;
      }
      
      client.lastHeartbeat = Date.now();
      
      switch (message.type) {
        case 'register_tunnel':
          this.handleRegisterTunnel(clientId, message);
          break;
          
        case 'unregister_tunnel':
          this.handleUnregisterTunnel(clientId, message);
          break;
          
        case 'heartbeat':
          this.handleHeartbeat(clientId, message);
          break;

        case 'capabilities': {
          const client = this.clients.get(clientId);
          if (client) {
            client.capabilities = {
              stream: !!message.stream,
              multiValueHeaders: message.multiValueHeaders === true,
            };
            if (client.ws) client.ws.__multiValueHeaders = client.capabilities.multiValueHeaders;
            console.log(`客户端 ${clientId} 能力声明:`, client.capabilities);
          }
          break;
        }
          
        case 'http_response':
          this.handleHttpResponse(clientId, message);
          break;
          
        case 'sse_data':
          this.handleSSEData(clientId, message);
          break;
          
        case 'sse_end':
          this.handleSSEEnd(clientId, message);
          break;
          
        case 'sse_error':
          this.handleSSEError(clientId, message);
          break;
          
        case 'sse_close':
          this.handleSSEClose(clientId, message);
          break;
          
        case 'websocket_upgraded':
          this.handleWebSocketUpgraded(clientId, message);
          break;
          
        case 'websocket_data':
          this.handleWebSocketData(clientId, message);
          break;

        case 'stream_open':
        case 'stream_chunk':
        case 'stream_end':
        case 'stream_ack':
        case 'stream_cancel':
          if (this.proxyServer && this.proxyServer.handleStreamMessage) {
            this.proxyServer.handleStreamMessage(message, clientId);
          }
          break;
          
        case 'websocket_end':
          this.handleWebSocketEnd(clientId, message);
          break;
          
        case 'websocket_error':
          this.handleWebSocketError(clientId, message);
          break;
          
        case 'websocket_close':
          this.handleWebSocketClose(clientId, message);
          break;
          
        case 'tunnel_list':
          this.handleTunnelList(clientId, message);
          break;
          
        case 'command_response':
          this.handleCommandResponse(clientId, message);
          break;
          
        case 'stats':
          this.handleStats(clientId, message);
          break;
          
        case 'sse_connections':
          this.handleSSEConnections(clientId, message);
          break;
          
        case 'error':
          console.error(`客户端 ${clientId} 报告错误:`, message.error);
          break;
          
        default:
          console.warn(`未知的消息类型: ${message.type}`);
      }
    } catch (error) {
      console.error('处理消息失败:', error);
    }
  }
  
  handleRegisterTunnel(clientId, message) {
    const { tunnel } = message;
    const client = this.clients.get(clientId);
    
    if (!client) {
      return;
    }
    
    const { id, domain, localPort, localHost = '127.0.0.1', protocol = 'http', description = '' } = tunnel;
    
    if (!this.validateTunnelConfig(tunnel)) {
      this.sendMessage(client.ws, {
        type: 'registered',
        domain,
        success: false,
        error: 'Invalid tunnel configuration'
      });
      return;
    }
    
    if (this.isDomainTaken(domain, clientId)) {
      this.sendMessage(client.ws, {
        type: 'registered',
        domain,
        success: false,
        error: `Domain ${domain} is already taken by another client`
      });
      return;
    }
    
    client.tunnels.set(domain, {
      id,
      localPort,
      localHost,
      protocol,
      description,
      registeredAt: new Date().toISOString(),
      lastActivity: null,
      stats: {
        requests: 0,
        responses: 0,
        errors: 0,
        bytesSent: 0,
        bytesReceived: 0
      }
    });
    
    console.log(`客户端 ${clientId} 注册隧道: ${domain} -> ${protocol}://${localHost}:${localPort} (${description})`);
    
    this.sendMessage(client.ws, {
      type: 'registered',
      domain,
      success: true,
      message: `Tunnel ${domain} registered successfully`
    });
  }
  
  handleUnregisterTunnel(clientId, message) {
    const { domain } = message;
    const client = this.clients.get(clientId);
    
    if (client && client.tunnels.has(domain)) {
      client.tunnels.delete(domain);
      console.log(`客户端 ${clientId} 取消注册隧道: ${domain}`);
      
      this.sendMessage(client.ws, {
        type: 'unregistered',
        domain,
        success: true
      });
    }
  }
  
  handleHeartbeat(clientId, message) {
    const client = this.clients.get(clientId);
    if (client) {
      if (message.stats) {
        client.heartbeatStats = message.stats;
      }
      
      this.sendMessage(client.ws, {
        type: 'heartbeat_ack',
        timestamp: new Date().toISOString()
      });
    }
  }
  
  // websocket-server.js - handleHttpResponse 完整函数

  handleHttpResponse(clientId, message) {
    // 流式请求可能收到整包响应（小响应走经典路径），先交给流处理器
    if (this.proxyServer && this.proxyServer.tryHandleStreamResponse && this.proxyServer.tryHandleStreamResponse(message)) {
      return;
    }
    const { requestId, domain, status, headers, body, error, isSSE = false, sseRequestId = null, isWebSocket = false, wsRequestId = null } = message;
    
    // ========== 验证状态码 ==========
    let validStatus = status;
    if (typeof validStatus !== 'number' || validStatus < 100 || validStatus > 599) {
      console.warn(`[handleHttpResponse] 无效状态码: ${validStatus}, 使用 200`);
      validStatus = 200;
    }
    
    // ========== 对于 304/204，强制 body 为空 ==========
    let validBody = body;
    if (validStatus === 304 || validStatus === 204) {
      validBody = null;
    }
    
    // ========== SSE 响应处理 ==========
    if (isSSE && sseRequestId) {
      console.log(`收到SSE初始响应: ${sseRequestId} -> ${validStatus}`);
      // SSE 响应不需要后续处理，直接返回
      return;
    }
    
    // ========== WebSocket 响应处理 ==========
    if (isWebSocket && wsRequestId) {
      console.log(`收到 WebSocket 响应: ${wsRequestId} -> ${validStatus}`);

      // 新版握手流程通过 upgradeWaiters 等待结果（handleHttpUpgrade 里注册）
      if (this.proxyServer && this.proxyServer.resolveUpgrade) {
        this.proxyServer.resolveUpgrade(wsRequestId, validStatus, headers);
      }

      if (this.proxyServer && this.proxyServer.handleWebSocketUpgraded) {
        this.proxyServer.handleWebSocketUpgraded(wsRequestId, { status: validStatus, headers });
      }

      // 新的握手流程在 handleHttpUpgrade() 里 await 这个结果：
      // 必须 resolve，否则浏览器侧只能等到超时（旧的"先握手后校验"流程不需要）。
      const pendingUpgrade = this.pendingRequests.get(requestId);
      if (pendingUpgrade) {
        clearTimeout(pendingUpgrade.timeout);
        this.pendingRequests.delete(requestId);
        pendingUpgrade.resolve({ status: validStatus, headers: headers || {} });
      }

      if (validStatus === 101) {
        console.log(`WebSocket 升级成功: ${wsRequestId}`);
      } else {
        console.error(`WebSocket 升级失败: ${wsRequestId} -> ${validStatus}`);
      }

      return;
    }
    
    // ========== 升级请求兜底 ==========
    // 个别旧客户端回包不带 isWebSocket 字段：用 requestId 直接匹配等待中的升级，
    // 避免浏览器侧白等 10 秒超时。
    if (this.proxyServer && this.proxyServer.resolveUpgrade &&
        this.proxyServer.resolveUpgrade(requestId, validStatus, headers)) {
      return;
    }

    // ========== 普通 HTTP 响应处理 ==========
    const pendingRequest = this.pendingRequests.get(requestId);
    if (!pendingRequest) {
      console.warn(`找不到请求ID: ${requestId} (可能已超时)`);
      return;
    }
    
    // 验证客户端
    if (pendingRequest.clientId !== clientId) {
      console.error(`请求ID ${requestId} 的客户端不匹配`);
      return;
    }
    
    // 清除超时定时器
    clearTimeout(pendingRequest.timeout);
    
    const client = this.clients.get(clientId);
    const tunnel = client?.tunnels.get(domain);
    const requestTime = Date.now() - pendingRequest.startTime;
    
    // ========== 错误处理 ==========
    if (error) {
      console.error(`客户端 ${clientId} 报告请求错误: ${error} (耗时: ${requestTime}ms)`);
      pendingRequest.reject(new Error(error));
      
      if (tunnel) tunnel.stats.errors++;
      if (client) client.stats.totalErrors++;
      
      // 错误响应，清理 pending
      if (!pendingRequest.isSSE && !pendingRequest.isWebSocket) {
        this.pendingRequests.delete(requestId);
      }
      return;
    }
    
    // ========== 成功响应 ==========
    console.log(`收到客户端 ${clientId} 的响应: ${validStatus} (耗时: ${requestTime}ms)`);
    
    // 构建响应对象 - 使用验证后的状态码和 body
    const response = {
      status: validStatus,
      headers: headers || {},
      body: validBody
    };
    
    // 对于 304/204，确保 body 为空
    if (validStatus === 304 || validStatus === 204) {
      response.body = null;
      // 移除 Content-Length 和 Content-Type 头
      if (response.headers) {
        delete response.headers['content-length'];
        delete response.headers['content-type'];
      }
    }
    
    // 解析 Promise
    pendingRequest.resolve(response);
    
    // ========== 更新统计 ==========
    if (tunnel) {
      tunnel.stats.responses++;
      if (validBody) {
        const bodySize = Buffer.isBuffer(validBody) ? validBody.length : 
          (typeof validBody === 'string' ? Buffer.from(validBody, 'utf8').length : 0);
        tunnel.stats.bytesReceived += bodySize;
      }
    }
    if (client) {
      client.stats.totalResponses++;
    }
    
    // ========== 清理 pending 请求 ==========
    // 对于 SSE 和 WebSocket，保持连接活跃
    if (!pendingRequest.isSSE && !pendingRequest.isWebSocket) {
      this.pendingRequests.delete(requestId);
    } else if (pendingRequest.isSSE) {
      console.log(`SSE请求 ${requestId} 保持活动状态`);
    } else if (pendingRequest.isWebSocket) {
      console.log(`WebSocket请求 ${requestId} 保持活动状态`);
    }
  }
  
  handleSSEData(clientId, message) {
    const { sseRequestId, data, event, id, retry, timestamp } = message;
    
    console.log(`收到SSE数据: ${sseRequestId} (事件: ${event || 'message'})`);
    
    const sseStream = this.sseStreams.get(sseRequestId);
    if (!sseStream) {
      console.warn(`找不到SSE流: ${sseRequestId}`);
      return;
    }
    
    const client = this.clients.get(sseStream.clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      console.warn(`客户端 ${sseStream.clientId} 已断开，清理SSE流: ${sseRequestId}`);
      this.handleSSEConnectionClose(sseRequestId);
      return;
    }
    
    sseStream.lastDataTime = Date.now();
    
    if (this.proxyServer && this.proxyServer.handleSSEData) {
      let sseData = data;
      // 客户端把 data 作为字符串发送；只要带 event/id/retry 就必须组装成对象，
      // 否则旧旁路会把具名事件退化成默认 message 事件。
      if (typeof data === 'object' || event || id || retry) {
        sseData = {
          event,
          id,
          retry,
          data: data
        };
      }
      
      try {
        this.proxyServer.handleSSEData(sseRequestId, sseData);
      } catch (error) {
        console.error(`发送SSE数据到代理服务器失败: ${sseRequestId}`, error);
        
        if (error.code === 'ECONNRESET' || error.message.includes('aborted')) {
          this.handleSSEConnectionClose(sseRequestId);
        }
      }
    }
  }
  
  handleSSEEnd(clientId, message) {
    const { sseRequestId, timestamp } = message;
    console.log(`SSE连接正常结束: ${sseRequestId}`);
    this.handleSSEConnectionClose(sseRequestId);
  }
  
  handleSSEError(clientId, message) {
    const { sseRequestId, error, timestamp } = message;
    console.error(`SSE连接错误: ${sseRequestId}`, error);
    this.handleSSEConnectionClose(sseRequestId);
  }
  
  handleSSEClose(clientId, message) {
    const { sseRequestId, timestamp, reason } = message;
    console.log(`SSE连接关闭: ${sseRequestId} (原因: ${reason || 'unknown'})`);
    this.handleSSEConnectionClose(sseRequestId);
  }
  
  handleWebSocketUpgraded(clientId, message) {
    const { wsRequestId, status, headers } = message;
    console.log(`WebSocket升级成功: ${wsRequestId} -> ${status}`);
    
    const wsStream = this.wsStreams.get(wsRequestId);
    if (wsStream) {
      wsStream.lastDataTime = Date.now();
      wsStream.upgraded = true;
      
      if (this.proxyServer && this.proxyServer.handleWebSocketUpgraded) {
        this.proxyServer.handleWebSocketUpgraded(wsRequestId, { status, headers });
      }
    } else {
      console.warn(`找不到WebSocket流: ${wsRequestId}`);
    }
  }
  
  handleWebSocketData(clientId, message) {
    const { wsRequestId, data, timestamp } = message;
    
    console.log(`[WebSocketServer] 收到客户端 WebSocket 数据: ${wsRequestId}, 数据长度: ${data?.length || 0}`);
    
    if (this.proxyServer && this.proxyServer.handleWebSocketData) {
      try {
        this.proxyServer.handleWebSocketData(wsRequestId, data, message.isBinary);
        console.log(`[WebSocketServer] 数据已转发到代理服务器: ${wsRequestId}`);
      } catch (error) {
        console.error(`发送WebSocket数据到代理服务器失败: ${wsRequestId}`, error);
      }
    }
  }
  
  handleWebSocketEnd(clientId, message) {
    const { wsRequestId, timestamp, code, reason } = message;
    console.log(`WebSocket连接正常结束: ${wsRequestId}`);
    
    if (this.proxyServer && this.proxyServer.handleWebSocketClose) {
      this.proxyServer.handleWebSocketClose(wsRequestId, { code: code || 1000, reason: reason || 'Normal closure' });
    }
    
    this.handleWebSocketConnectionClose(wsRequestId);
  }
  
  handleWebSocketError(clientId, message) {
    const { wsRequestId, error, timestamp } = message;
    console.error(`WebSocket连接错误: ${wsRequestId}`, error);
    this.handleWebSocketConnectionClose(wsRequestId);
  }
  
  handleWebSocketClose(clientId, message) {
    const { wsRequestId, code, reason, timestamp } = message;
    
    let reasonStr = '';
    if (reason) {
      reasonStr = typeof reason === 'string' ? reason : JSON.stringify(reason);
    }
    
    console.log(`[WebSocketServer] 收到客户端关闭通知: ${wsRequestId}, code: ${code}, reason: ${reasonStr}`);
    
    if (this.proxyServer && this.proxyServer.handleWebSocketClose) {
      let closeCode = code;
      if (!closeCode || closeCode === 1005 || closeCode === 1006) {
        closeCode = 1000;
      }
      this.proxyServer.handleWebSocketClose(wsRequestId, { code: closeCode, reason: reasonStr });
    }
    
    this.handleWebSocketConnectionClose(wsRequestId);
  }
  
  handleTunnelList(clientId, message) {
    console.log(`客户端 ${clientId} 发送隧道列表:`, message.tunnels?.length || 0, '个隧道');
    
    const client = this.clients.get(clientId);
    if (client) {
      message.tunnels?.forEach(tunnel => {
        if (tunnel.enabled && !client.tunnels.has(tunnel.domain)) {
          this.handleRegisterTunnel(clientId, {
            tunnel: {
              id: tunnel.id,
              domain: tunnel.domain,
              localPort: tunnel.localPort || 80,
              localHost: tunnel.localHost || '127.0.0.1',
              protocol: tunnel.protocol || 'http',
              description: tunnel.description || ''
            }
          });
        }
      });
    }
  }
  
  handleCommandResponse(clientId, message) {
    console.log(`客户端 ${clientId} 命令响应:`, message.command, message.success ? '成功' : '失败');
  }
  
  handleStats(clientId, message) {
    const client = this.clients.get(clientId);
    if (client) {
      client.lastStatsUpdate = new Date().toISOString();
      client.clientStats = message.clientStats;
      client.tunnelStats = message.tunnelStats;
      
      console.log(`客户端 ${clientId} 统计更新:`, {
        connected: message.clientStats.connected,
        tunnelCount: message.clientStats.registeredTunnels?.length || 0,
        sseConnections: message.clientStats.sseConnections || 0
      });
    }
  }
  
  handleSSEConnections(clientId, message) {
    console.log(`客户端 ${clientId} SSE连接:`, message.connections?.length || 0, '个连接');
  }
  
  async sendHttpRequest(clientId, request) {
    return new Promise((resolve, reject) => {
      const client = this.clients.get(clientId);
      
      if (!client) {
        return reject(new Error('客户端未连接'));
      }
      
      if (client.ws.readyState !== WebSocket.OPEN) {
        return reject(new Error('客户端WebSocket连接未就绪'));
      }
      
      const host = request.headers.host;
      if (!host) {
        return reject(new Error('请求缺少Host头部'));
      }
      
      const tunnel = client.tunnels.get(host);
      if (!tunnel) {
        return reject(new Error(`客户端 ${clientId} 没有注册隧道 ${host}`));
      }
      
      tunnel.stats.requests++;
      tunnel.lastActivity = new Date().toISOString();
      client.stats.totalRequests++;
      
      const requestId = uuidv4();

      if (request.isWebSocket) {
        if (!request.headers['upgrade']) {
          request.headers['upgrade'] = 'websocket';
        }
        if (!request.headers['connection']) {
          request.headers['connection'] = 'upgrade';
        }
        if (!request.headers['sec-websocket-key']) {
          const crypto = require('crypto');
          request.headers['sec-websocket-key'] = crypto.randomBytes(16).toString('base64');
        }
      }
      
      if (request.isSSE && request.sseRequestId) {
        this.sseStreams.set(request.sseRequestId, {
          clientId,
          domain: host,
          lastDataTime: Date.now(),
          requestId: requestId
        });
        
        console.log(`创建SSE流: ${request.sseRequestId} (客户端: ${clientId}, 域名: ${host})`);
      }
      
      if (request.isWebSocket && request.wsRequestId) {
        this.wsStreams.set(request.wsRequestId, {
          clientId,
          domain: host,
          lastDataTime: Date.now(),
          requestId: requestId,
          upgraded: false
        });
        
        console.log(`创建WebSocket流: ${request.wsRequestId} (客户端: ${clientId}, 域名: ${host})`);
      }
      
      const isWebSocketUpgrade = request.headers && 
                               request.headers.upgrade === 'websocket';

      const requestData = {
        type: 'http_request',
        requestId,
        domain: host,
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: request.body,
        remoteAddress: request.remoteAddress,
        timestamp: new Date().toISOString(),
        isSSE: request.isSSE || false,
        sseRequestId: request.sseRequestId || null,
        isWebSocket: request.isWebSocket || isWebSocketUpgrade,
        wsRequestId: request.wsRequestId || (isWebSocketUpgrade ? requestId : null)
      };

      console.log(`[WebSocketServer] 发送请求到客户端:`, {
        requestId,
        domain: host,
        isWebSocket: requestData.isWebSocket,
        hasUpgradeHeader: request.headers?.upgrade
      });
      
      this.pendingRequests.set(requestId, {
        resolve,
        reject,
        clientId,
        domain: host,
        timeout: setTimeout(() => {
          const pendingRequest = this.pendingRequests.get(requestId);
          if (pendingRequest) {
            if (request.isSSE) {
              console.log(`SSE请求 ${requestId} 进入流式模式`);
            } else if (request.isWebSocket) {
              console.log(`WebSocket请求 ${requestId} 进入流式模式`);
            } else {
              pendingRequest.reject(new Error('请求超时 (30秒)'));
              this.pendingRequests.delete(requestId);
              tunnel.stats.errors++;
              client.stats.totalErrors++;
            }
          }
        }, 3000000),
        startTime: Date.now(),
        isSSE: request.isSSE || false,
        sseRequestId: request.sseRequestId,
        isWebSocket: request.isWebSocket || false,
        wsRequestId: request.wsRequestId
      });
      
      try {
        this.sendMessage(client.ws, requestData);
        console.log(`请求已发送到客户端 ${clientId}: ${request.method} ${request.url} (请求ID: ${requestId})`);
      } catch (error) {
        const pendingRequest = this.pendingRequests.get(requestId);
        if (pendingRequest) {
          clearTimeout(pendingRequest.timeout);
          this.pendingRequests.delete(requestId);
        }
        reject(error);
      }
    });
  }
  
  async handleDisconnect(clientId, ws) {
    const client = this.clients.get(clientId);
    if (client) {
      // 断开的如果不是当前登记的那条连接，直接忽略
      if (ws && client.ws !== ws) {
        console.log(`客户端 ${clientId} 的旧连接已断开，保留当前连接`);
        return;
      }
      console.log(`客户端 ${clientId} 断开连接`);
      // 先关闭 WebSocket 连接（如果还没关闭）
      for(let i= 0 ;i<5;i++)
      {
        await new Promise((resolve)=>setTimeout(resolve,1000))
        try {
          if (client.ws.readyState === WebSocket.OPEN || 
              client.ws.readyState === WebSocket.CONNECTING) {
            // client.ws.close();
            continue
          }
          break
        } catch (e) {
          // 忽略关闭错误
        }
      }
      try {
          if (client.ws.readyState === WebSocket.OPEN || 
              client.ws.readyState === WebSocket.CONNECTING) {
            // client.ws.close();
              return 
          }
        } catch (e) {
          // 忽略关闭错误
        }
      
      for (const [requestId, pendingRequest] of this.pendingRequests.entries()) {
        if (pendingRequest.clientId === clientId) {
          clearTimeout(pendingRequest.timeout);
          pendingRequest.reject(new Error('客户端断开连接'));
          this.pendingRequests.delete(requestId);
          
          const tunnel = client.tunnels.get(pendingRequest.domain);
          if (tunnel) {
            tunnel.stats.errors++;
          }
          client.stats.totalErrors++;
        }
      }
      
      for (const [sseRequestId, stream] of this.sseStreams.entries()) {
        if (stream.clientId === clientId) {
          console.log(`清理客户端 ${clientId} 的SSE流: ${sseRequestId}`);
          this.sseStreams.delete(sseRequestId);
        }
      }
      
      for (const [wsRequestId, stream] of this.wsStreams.entries()) {
        if (stream.clientId === clientId) {
          console.log(`清理客户端 ${clientId} 的WebSocket流: ${wsRequestId}`);
          this.wsStreams.delete(wsRequestId);
        }
      }
      
      // 删除前再比对一次：等待期间可能已被新连接替换
      const current = this.clients.get(clientId);
      if (ws && current && current.ws !== ws) {
        console.log(`客户端 ${clientId} 已被新连接接管，保留其隧道`);
        return;
      }

      // 本客户端的浏览器 WS 已经没有对端了，必须关掉，否则浏览器不会重连
      this.closeOrphanBrowserSockets(clientId, 'tunnel client disconnected');

      this.clients.delete(clientId);
    }
  }
  
  validateTunnelConfig(tunnel) {
    if (!tunnel.domain || !tunnel.localPort) {
      return false;
    }
    
    const domainRegex = /^[a-zA-Z0-9][a-zA-Z0-9-]*(\.[a-zA-Z0-9][a-zA-Z0-9-]*)+$/;
    if (!domainRegex.test(tunnel.domain)) {
      return false;
    }
    
    const port = parseInt(tunnel.localPort);
    if (isNaN(port) || port < 1 || port > 65535) {
      return false;
    }
    
    return true;
  }
  
  isDomainTaken(domain, excludeClientId) {
    for (const [clientId, client] of this.clients.entries()) {
      if (clientId !== excludeClientId && client.tunnels.has(domain)) {
        return true;
      }
    }
    return false;
  }
  
  startCleanupInterval() {
    // 请求清理已禁用
  }
  
  handleSSEConnectionClose(sseRequestId) {
    const sseStream = this.sseStreams.get(sseRequestId);
    if (sseStream) {
      const client = this.clients.get(sseStream.clientId);
      if (client && client.ws.readyState === WebSocket.OPEN) {
        try {
          this.sendMessage(client.ws, {
            type: 'sse_close',
            sseRequestId,
            timestamp: new Date().toISOString(),
            reason: 'connection_closed'
          });
        } catch (error) {
          console.error(`发送SSE关闭通知失败: ${error.message}`);
        }
      }
      
      this.sseStreams.delete(sseRequestId);
      
      for (const [requestId, pendingRequest] of this.pendingRequests.entries()) {
        if (pendingRequest.sseRequestId === sseRequestId) {
          clearTimeout(pendingRequest.timeout);
          this.pendingRequests.delete(requestId);
          break;
        }
      }
      
      console.log(`SSE流 ${sseRequestId} 已关闭`);
    }
  }
  
  handleWebSocketConnectionClose(wsRequestId) {
    const wsStream = this.wsStreams.get(wsRequestId);
    if (wsStream) {
      const client = this.clients.get(wsStream.clientId);
      if (client && client.ws.readyState === WebSocket.OPEN) {
        try {
          this.sendMessage(client.ws, {
            type: 'websocket_close',
            wsRequestId: wsRequestId,
            code: 1000,
            reason: 'connection_closed',
            timestamp: new Date().toISOString()
          });
        } catch (error) {
          console.error(`发送WebSocket关闭通知失败: ${error.message}`);
        }
      }
      
      this.wsStreams.delete(wsRequestId);
      
      for (const [requestId, pendingRequest] of this.pendingRequests.entries()) {
        if (pendingRequest.wsRequestId === wsRequestId) {
          clearTimeout(pendingRequest.timeout);
          this.pendingRequests.delete(requestId);
          break;
        }
      }
      
      console.log(`WebSocket流 ${wsRequestId} 已关闭`);
    }
  }
  
  startSSEStreamCleanup() {
    setInterval(() => {
      const now = Date.now();
      const timeout = 30 * 60 * 1000;
      
      for (const [wsRequestId, stream] of this.wsStreams.entries()) {
        if (now - stream.lastDataTime > timeout) {
          console.log(`WebSocket流 ${wsRequestId} 超时，清理`);
          this.handleWebSocketConnectionClose(wsRequestId);
        }
      }
    }, 300000);
  }
  
  getClient(clientId) {
    return this.clients.get(clientId);
  }
  
  getClientByDomain(domain) {
    for (const [clientId, client] of this.clients.entries()) {
      if (client.tunnels.has(domain)) {
        return { clientId, client };
      }
    }
    return null;
  }
  
  getAllClients() {
    return Array.from(this.clients.entries()).map(([clientId, data]) => ({
      clientId,
      connected: data.ws.readyState === WebSocket.OPEN,
      tunnels: Array.from(data.tunnels.entries()).map(([domain, tunnel]) => ({
        domain,
        ...tunnel
      })),
      stats: data.stats,
      metadata: data.metadata,
      lastHeartbeat: data.lastHeartbeat
    }));
  }
  
  getAllTunnels() {
    const tunnels = [];
    for (const [clientId, client] of this.clients.entries()) {
      for (const [domain, tunnel] of client.tunnels.entries()) {
        tunnels.push({
          clientId,
          domain,
          ...tunnel,
          clientConnected: client.ws.readyState === WebSocket.OPEN
        });
      }
    }
    return tunnels;
  }
  
  getSSEStreams() {
    return Array.from(this.sseStreams.entries()).map(([id, stream]) => ({
      id,
      ...stream,
      idleTime: Date.now() - stream.lastDataTime
    }));
  }
  
  getWebSocketStreams() {
    return Array.from(this.wsStreams.entries()).map(([id, stream]) => ({
      id,
      ...stream,
      idleTime: Date.now() - stream.lastDataTime
    }));
  }
  
  sendCommand(clientId, command, data = {}) {
    const client = this.clients.get(clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      throw new Error('客户端未连接');
    }
    
    const message = {
      type: 'command',
      command,
      data,
      timestamp: new Date().toISOString()
    };
    
    this.sendMessage(client.ws, message);
    return true;
  }
  
  broadcast(message, excludeClientId = null) {
    for (const [clientId, client] of this.clients.entries()) {
      if (clientId !== excludeClientId && client.ws.readyState === WebSocket.OPEN) {
        this.sendMessage(client.ws, message);
      }
    }
  }
}

module.exports = WebSocketServer;