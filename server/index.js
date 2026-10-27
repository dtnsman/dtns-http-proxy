// index.js - 服务端主文件
const WebSocketServer = require('./websocket-server');
const ProxyServer = require('./proxy-server');
const config = require('./config');

class TunnelServer {
  constructor() {
    console.log('启动内网穿透服务器...');
    
    this.wsServer = new WebSocketServer(
      config.websocket.port,
      config.websocket.path
    );
    
    this.proxyServer = new ProxyServer(this.wsServer, config);
    
    this.wsServer.proxyServer = this.proxyServer;
    
    this.proxyServer.start();
    
    this.startManagementInterface();
  }
  
  startManagementInterface() {
    const http = require('http');
    
    const managementServer = http.createServer((req, res) => {
      if (req.url === '/status' && req.method === 'GET') {
        const clients = this.wsServer.getAllClients();
        const tunnels = this.wsServer.getAllTunnels();
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: 'running',
          timestamp: new Date().toISOString(),
          clients: clients,
          tunnels: tunnels,
          stats: {
            totalClients: clients.length,
            connectedClients: clients.filter(c => c.connected).length,
            totalTunnels: tunnels.length,
            sseStreams: this.wsServer.sseStreams ? this.wsServer.sseStreams.size : 0
          }
        }));
      } else if (req.url === '/sse-streams' && req.method === 'GET') {
        const sseStreams = Array.from(this.wsServer.sseStreams?.entries() || []).map(([id, stream]) => ({
          id,
          ...stream,
          idleTime: Date.now() - stream.lastDataTime
        }));
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          sseStreams,
          total: sseStreams.length
        }));
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });
    
    // managementServer.listen(8081, () => {
    //   console.log('管理接口运行在 http://localhost:8081/status');
    //   console.log('SSE流监控: http://localhost:8081/sse-streams');
    // });
  }
}

const server = new TunnelServer();

process.on('SIGINT', () => {
  console.log('正在关闭服务器...');
  process.exit(0);
});