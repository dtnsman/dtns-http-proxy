// config.js - DTNS HTTP 隧道客户端配置示例
// 复制本文件为 config.js 并按实际环境修改后使用。
// 注意：仓库中自带的即为示例配置，直接运行前请至少修改 server.wsServerUrl
// 与 client.id，以及 tunnels 中的域名/端口。

module.exports = {
  // ===== 服务端连接 =====
  server: {
    // 服务端 WebSocket 地址（示例：wss://tunnel.example.com）
    wsServerUrl: 'wss://tunnel.example.com',
    // WebSocket 路径，需与服务端 websocket.path 一致
    wsPath: '/tunnel'
  },

  // ===== 客户端身份 =====
  client: {
    // 客户端 ID，可用环境变量 CLIENT_ID 覆盖
    id: process.env.CLIENT_ID || 'example-client',

    // 断线重连
    reconnect: {
      enabled: true,
      interval: 5000,
      maxAttempts: 100
    },

    // 心跳间隔（毫秒）
    heartbeat: {
      interval: 30000
    },

    // 性能参数
    performance: {
      maxConcurrentRequests: 100,
      requestTimeout: 30000,
      maxPayloadSize: 100 * 1024 * 1024
    }
  },

  // ===== 协议配置 =====
  protocol: {
    preferred: 'binary', // 'binary' | 'json' | 'auto'
    binaryEnabled: true,
    version: 1
  },

  // ===== 隧道列表 =====
  // 每条隧道把本地服务映射到一个公网域名。
  tunnels: [
    {
      id: 'example-web',
      domain: 'app.example.com',
      local: {
        host: '127.0.0.1',
        port: 3000,
        protocol: 'http'
      },
      enabled: true,
      description: '示例 Web 应用',
      // 流式传输（可选，按隧道覆盖；需服务端 streaming.enabled 打开）
      // streaming: {
      //   windowSize: 1048576,
      //   responseThreshold: 1048576,
      //   idleTimeoutMs: 60000
      // }
    }
    // 可继续添加更多隧道：
    // {
    //   id: 'example-api',
    //   domain: 'api.example.com',
    //   local: { host: '127.0.0.1', port: 8080, protocol: 'http' },
    //   enabled: false,
    //   description: '示例 API 服务'
    // }
  ]
};
