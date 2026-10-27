# DTNS HTTP Tunnel（开源版）

一个轻量的 **HTTP / WebSocket 内网穿透（反向隧道）** 实现，包含客户端与服务端两部分，协议版本 **2.0**。

- `client/` — 运行在内网机器上的客户端，把本地 HTTP 服务通过 WebSocket 隧道暴露出去
- `server/` — 运行在公网服务器上的服务端，按域名把公网请求转发到对应隧道

> 本仓库仅包含 `client` 与 `server` 两个目录的源码，配置均为**空白示例配置**，不含任何真实域名、密钥或隧道信息。

---

## 特性

- HTTP 与 WebSocket 双向代理，支持流式响应（SSE）
- 基于域名路由：`<子域名><domainBase>` → 指定隧道
- 多隧道管理，断线自动重连
- 大请求体支持，可按域名单独设置上限
- 客户端守护进程（daemon），进程异常退出后自动拉起

---

## 目录结构

```
.
├── client/
│   ├── index.js            # 客户端入口
│   ├── config.js           # 客户端配置（示例，需自行填写）
│   ├── protocol.js         # 协议实现
│   ├── http-proxy.js       # 本地 HTTP/WS 转发
│   ├── tunnel-manager.js   # 隧道与重连管理
│   ├── daemon.js           # 守护进程
│   └── package.json
├── server/
│   ├── index.js            # 服务端入口
│   ├── config.js           # 服务端配置（示例，需自行填写）
│   ├── protocol.js         # 协议实现
│   ├── proxy-server.js     # 公网 HTTP/WS 接入与转发
│   ├── websocket-server.js # 隧道 WebSocket 服务
│   └── package.json
├── LICENSE
└── README.md
```

---

## 快速开始

### 环境要求

- Node.js 18+（推荐 LTS）
- 公网服务器需可被客户端访问（WebSocket 端口）

### 1. 服务端

```bash
cd server
npm install
# 按需修改 config.js（端口、domainBase、鉴权等）
npm start
```

### 2. 客户端

```bash
cd client
npm install
# 按需修改 config.js：服务端地址、隧道列表、本地目标端口
npm start
```

### 3. 访问

配置完成后，访问 `http://<你的域名><domainBase>` 即可转发到本地服务。

---

## 配置说明

两个目录下的 `config.js` 均为**示例配置**，请复制一份实际配置后按注释填写，且**不要将真实配置提交到仓库**（`.gitignore` 已排除常见运行时文件）。

- **服务端**：监听端口、`domainBase`、请求体上限等
- **客户端**：`wsServerUrl`（服务端 WebSocket 地址）、隧道列表（域名、本地端口、目标协议）

---

## 安全提示

1. 请勿在仓库中提交真实域名、Token、证书私钥、隧道 ID 等敏感信息。
2. 生产环境建议在服务端前加一层反向代理并启用 HTTPS。
3. 请为隧道接入增加鉴权，避免隧道被滥用。

---

## 许可证

本项目基于 [MIT License](LICENSE) 开源。