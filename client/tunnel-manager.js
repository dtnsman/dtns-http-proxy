// tunnel-manager.js - 隧道管理器
const EventEmitter = require('events');
const HttpProxy = require('./http-proxy');
const { BinaryCodec } = require('./protocol');

class TunnelManager extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.tunnels = new Map();
    this.proxies = new Map();
    this.stats = new Map();
    // 本端已经不存在的 wsRequestId（客户端重启后，服务端仍在转发浏览器残留连接的数据）。
    // 每个 id 只回一次 websocket_close，避免刷屏。
    this.staleWsNotified = new Set();
    
    this.initializeTunnels();
  }
  
  initializeTunnels() {
    console.log('初始化隧道配置...');
    
    this.config.tunnels.forEach(tunnelConfig => {
      if (tunnelConfig.enabled) {
        this.addTunnel(tunnelConfig);
      }
    });
    
    console.log(`已加载 ${this.tunnels.size} 个隧道`);
  }
  
  addTunnel(tunnelConfig) {
    const { id, domain, local, description = '' } = tunnelConfig;
    
    if (!id || !domain || !local) {
      console.error(`隧道配置无效: ${JSON.stringify(tunnelConfig)}`);
      return false;
    }
    
    if (!this.validateDomain(domain)) {
      console.error(`域名格式无效: ${domain}`);
      return false;
    }
    
    const tunnel = {
      id,
      domain,
      local,
      description,
      enabled: true,
      createdAt: new Date().toISOString(),
      config: tunnelConfig
    };
    
    const proxy = new HttpProxy(local, tunnelConfig);
    
    this.tunnels.set(domain, tunnel);
    this.proxies.set(domain, proxy);
    this.stats.set(domain, {
      requests: 0,
      responses: 0,
      errors: 0,
      bytesSent: 0,
      bytesReceived: 0,
      lastRequest: null,
      lastResponse: null
    });
    
    console.log(`添加隧道: ${domain} -> ${local.protocol}://${local.host}:${local.port} (${description})`);
    
    this.emit('tunnelAdded', tunnel);
    return true;
  }
  
  removeTunnel(domain) {
    if (this.tunnels.has(domain)) {
      const tunnel = this.tunnels.get(domain);
      
      const proxy = this.proxies.get(domain);
      if (proxy) {
        proxy.destroy();
      }
      
      this.tunnels.delete(domain);
      this.proxies.delete(domain);
      this.stats.delete(domain);
      
      console.log(`移除隧道: ${domain}`);
      this.emit('tunnelRemoved', tunnel);
      return true;
    }
    return false;
  }
  
  enableTunnel(domain) {
    const tunnel = this.tunnels.get(domain);
    if (tunnel) {
      tunnel.enabled = true;
      console.log(`启用隧道: ${domain}`);
      this.emit('tunnelEnabled', tunnel);
      return true;
    }
    return false;
  }
  
  disableTunnel(domain) {
    const tunnel = this.tunnels.get(domain);
    if (tunnel) {
      tunnel.enabled = false;
      console.log(`禁用隧道: ${domain}`);
      this.emit('tunnelDisabled', tunnel);
      return true;
    }
    return false;
  }
  
  validateDomain(domain) {
    const domainRegex = /^[a-zA-Z0-9][a-zA-Z0-9-]*(\.[a-zA-Z0-9][a-zA-Z0-9-]*)+$/;
    return domainRegex.test(domain);
  }
  
  getTunnelByDomain(domain) {
    return this.tunnels.get(domain);
  }
  
  getTunnelById(id) {
    for (const [domain, tunnel] of this.tunnels.entries()) {
      if (tunnel.id === id) {
        return tunnel;
      }
    }
    return null;
  }
  
  getAllTunnels() {
    return Array.from(this.tunnels.values());
  }
  
  getEnabledTunnels() {
    return this.getAllTunnels().filter(tunnel => tunnel.enabled);
  }
  
  async forwardRequest(domain, requestData, wsClient = null) {
    const tunnel = this.tunnels.get(domain);
    const proxy = this.proxies.get(domain);
    const stats = this.stats.get(domain);
    
    if (!tunnel || !proxy || !tunnel.enabled) {
      throw new Error(`隧道 ${domain} 不存在或已禁用`);
    }
    
    stats.requests++;
    stats.lastRequest = new Date().toISOString();
    
    try {
      const response = await proxy.forwardRequest(requestData, wsClient);
      
      stats.responses++;
      stats.lastResponse = new Date().toISOString();
      
      // ========== 改进8：正确计算 Buffer 大小 ==========
      if (response.body) {
        let bodySize = 0;
        if (Buffer.isBuffer(response.body)) {
          bodySize = response.body.length;
        } else if (typeof response.body === 'string') {
          bodySize = Buffer.byteLength(response.body, 'utf8');
        } else if (typeof response.body === 'object') {
          bodySize = Buffer.byteLength(JSON.stringify(response.body), 'utf8');
        }
        stats.bytesReceived += bodySize;
      }
      
      if (requestData.body) {
        let requestBodySize = 0;
        if (Buffer.isBuffer(requestData.body)) {
          requestBodySize = requestData.body.length;
        } else if (typeof requestData.body === 'string') {
          requestBodySize = Buffer.byteLength(requestData.body, 'utf8');
        }
        stats.bytesSent += requestBodySize;
      }
      
      this.emit('requestCompleted', { domain, requestData, response });
      return response;
      
    } catch (error) {
      stats.errors++;
      this.emit('requestFailed', { domain, requestData, error });
      throw error;
    }
  
  }
  
  handleWebSocketData(wsRequestId, data, isBinary, wsClient = null) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        proxy.handleWebSocketData(wsRequestId, data, isBinary);
        return;
      }
    }

    // 服务端还在往这条 ws 转发数据，但本进程已经没有对应连接：
    // 典型场景是客户端重启过（进程内 wsConnections 丢失），服务端却保留了浏览器侧的 WS。
    // 这里只回一次 websocket_close，让服务端把浏览器连接关掉，浏览器会自动重连并重新发起握手。
    if (!this.staleWsNotified.has(wsRequestId)) {
      this.staleWsNotified.add(wsRequestId);
      console.warn(`未找到WebSocket连接: ${wsRequestId}，已请求服务端关闭该连接`);
      if (wsClient && wsClient.readyState === 1) {
        try {
          wsClient.send(BinaryCodec.encode({
            type: 'websocket_close',
            wsRequestId,
            code: 1011,
            reason: 'client lost connection',
            timestamp: new Date().toISOString(),
          }));
        } catch (e) {}
      }
    }
  }

  /** 重连/重启后清空残留 ws 记录，避免 Set 无限增长。 */
  clearStaleWebSockets() {
    this.staleWsNotified.clear();
  }

  /**
   * 流式消息路由：先按 streamId 找已存在的流；若是新的上行 open，再按域名找隧道。
   */
  handleStreamMessage(message, wsClient) {
    const streamId = message.streamId;
    if (!streamId) return;

    for (const [, proxy] of this.proxies.entries()) {
      if (proxy.streams && proxy.streams.has(streamId)) {
        proxy.handleStreamMessage(message, wsClient);
        return;
      }
    }

    if (message.type === 'stream_open' && (message.direction || 'up') === 'up') {
      const domain = (message.headers || {}).host;
      const proxy = this.proxies.get(domain);
      if (proxy) {
        proxy.handleStreamMessage(message, wsClient);
        return;
      }
      console.warn(`未找到流对应的隧道: ${domain}`);
    }
  }

  closeWebSocketConnection(wsRequestId) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        proxy.closeWebSocketConnection(wsRequestId);
        return;
      }
    }
  }

  cleanupAllWebSocketConnections() {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.size > 0) {
        for (const [wsRequestId] of proxy.wsConnections.entries()) {
          proxy.closeWebSocketConnection(wsRequestId);
        }
      }
    }
    console.log('所有 WebSocket 连接已清理');
  }

  handleWebSocketUpgraded(wsRequestId, info) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        if (proxy.handleWebSocketUpgraded) {
          proxy.handleWebSocketUpgraded(wsRequestId, info);
        }
        return;
      }
    }
  }

  handleWebSocketEnd(wsRequestId, data) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        if (proxy.handleWebSocketEnd) {
          proxy.handleWebSocketEnd(wsRequestId, data);
        }
        return;
      }
    }
  }

  handleWebSocketError(wsRequestId, error) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        if (proxy.handleWebSocketError) {
          proxy.handleWebSocketError(wsRequestId, error);
        }
        return;
      }
    }
  }

  handleWebSocketClose(wsRequestId, data) {
    for (const [domain, proxy] of this.proxies.entries()) {
      if (proxy.wsConnections && proxy.wsConnections.has(wsRequestId)) {
        if (proxy.handleWebSocketClose) {
          proxy.handleWebSocketClose(wsRequestId, data);
        }
        return;
      }
    }
  }
  
  getTunnelStats(domain) {
    return this.stats.get(domain) || null;
  }
  
  getAllStats() {
    const allStats = {};
    for (const [domain, stats] of this.stats.entries()) {
      allStats[domain] = { ...stats };
    }
    return allStats;
  }
  
  resetStats(domain) {
    if (domain) {
      const stats = this.stats.get(domain);
      if (stats) {
        this.stats.set(domain, {
          requests: 0,
          responses: 0,
          errors: 0,
          bytesSent: 0,
          bytesReceived: 0,
          lastRequest: null,
          lastResponse: null
        });
        return true;
      }
    } else {
      for (const [domain] of this.stats.entries()) {
        this.resetStats(domain);
      }
      return true;
    }
    return false;
  }
  
  addTunnelFromConfig(config) {
    return this.addTunnel(config);
  }
  
  loadTunnelsFromFile(filePath) {
    try {
      const fs = require('fs');
      const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      
      if (data.tunnels && Array.isArray(data.tunnels)) {
        let addedCount = 0;
        data.tunnels.forEach(tunnelConfig => {
          if (this.addTunnel(tunnelConfig)) {
            addedCount++;
          }
        });
        console.log(`从文件加载了 ${addedCount} 个隧道`);
        return addedCount;
      }
    } catch (error) {
      console.error('加载隧道配置文件失败:', error);
    }
    return 0;
  }
  
  exportTunnels() {
    return this.getAllTunnels().map(tunnel => ({
      id: tunnel.id,
      domain: tunnel.domain,
      local: tunnel.local,
      description: tunnel.description,
      enabled: tunnel.enabled
    }));
  }
  
  /**
   * 发送二进制消息
   */
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

  /**
   * 发送消息（自动选择格式）
   */
  sendMessage(ws, message) {
    const preferBinary = this.config.protocol?.preferred === 'binary';
    return this.sendBinaryMessage(ws, message);
  }
}

module.exports = TunnelManager;