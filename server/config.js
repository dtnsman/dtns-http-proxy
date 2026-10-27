// config.js - DTNS HTTP 隧道服务端配置示例
// 复制本文件为 config.js 并按实际环境修改后使用。
// 注意：仓库中自带的即为示例配置，其中域名均为占位符。

module.exports = {
  // ===== HTTP 服务器 =====
  http: {
    port: 18386,
    host: '0.0.0.0',
    // 域名基础部分，用于拼接/校验隧道域名
    domainBase: '.example.com'
  },

  // ===== HTTPS 服务器（可选） =====
  https: {
    enabled: false,
    port: 443,
    keyPath: './ssl/private.key',
    certPath: './ssl/certificate.crt'
  },

  // ===== WebSocket 服务器 =====
  websocket: {
    port: 18385,
    path: '/tunnel'
  },

  // ===== 协议配置 =====
  protocol: {
    preferred: 'binary',
    binaryEnabled: true,
    version: 1
  },

  // ===== 客户端管理 =====
  clients: {
    // 心跳超时时间（毫秒）
    heartbeatTimeout: 60000,
    // 最大连接数
    maxConnections: 1000
  },

  // ===== 流式传输（双向） =====
  // 只有声明了 stream 能力的客户端才会走流式；旧客户端自动回退到全缓冲。
  streaming: {
    enabled: true,
    chunkSize: 262144,     // 单块大小 256 KB
    windowSize: 1048576,   // 每流信用窗口 1 MB
    idleTimeoutMs: 60000   // 空闲超时（不是总时长）
  },

  // ===== 资源限制 =====
  limits: {
    // 请求体上限（字节），超过直接返回 413
    maxBodySize: 33554432, // 32 MB

    // 按域名覆盖（键 = 隧道域名），适合需要传大文件的站点
    maxBodySizeByDomain: {
      // 'files.example.com': 1073741824 // 1 GB
    }
  }
};
