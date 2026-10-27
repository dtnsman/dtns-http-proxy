# DTNS HTTP Tunnel

> 自托管的 HTTP / WebSocket 内网穿透：**一条 WebSocket 复用多条域名隧道**，自己的服务器、自己的域名、自己的流量。

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Node](https://img.shields.io/badge/Node.js-%3E%3D18-339933.svg)](https://nodejs.org)
[![Gitee](https://img.shields.io/badge/Gitee-dtnsman%2Fdtns--http--proxy-c71d23.svg)](https://gitee.com/dtnsman/dtns-http-proxy)
[![GitHub](https://img.shields.io/badge/GitHub-dtnsman%2Fdtns--http--proxy-181717.svg)](https://github.com/dtnsman/dtns-http-proxy)

- `client/` — 运行在内网机器上，把本地 HTTP 服务通过一条出站 WebSocket 暴露到公网
- `server/` — 运行在公网服务器上，按请求的 `Host` 把流量路由到对应隧道

> 本仓库仅包含 `client` 与 `server` 两个目录的源码。仓库内的 `config.js` 均为**空白示例配置**，不含任何真实域名、密钥或隧道信息。

---

## 一、它解决什么问题

市面上的穿透工具大致分两类：**SaaS 类**（ngrok、localtunnel）开箱即用但受制于配额、插页与第三方流量中转；**自托管类**（frp、bore、chisel）自由但配置偏重、偏底层端口转发。本项目瞄准的是中间的空白地带：**要自托管、要按域名做虚拟主机、要长连接流式不炸内存、且不想为此写一堆配置**。

五个具体痛点与对应答案：

| 痛点 | 常见方案的坑 | 本项目的做法 |
| --- | --- | --- |
| 临时给外部演示本地服务 | ngrok 免费版在 HTML 流量前插入确认页，客户看到的是"这是 ngrok 提供的站点" | 自建服务端，**没有插页、没有配额、没有品牌露出** |
| 域名经常变，链接发出去就失效 | 免费版多为每次重启随机子域名，固定域名要付费 | 域名由你自己在 `config.js` 里写死，**永久固定** |
| 一台机器上要暴露多个服务 | 多进程各起一条隧道，端口和配置成倍膨胀 | **单客户端多隧道**，一条 WebSocket 复用全部隧道，服务端按 `Host` 分流 |
| AI / 实时应用：SSE、WebSocket、大文件 | 整包缓冲导致首字节延迟高，大上传直接把内存打爆 | **双向信用窗口流式传输**，内存上界 = 窗口 × 并发流数，与文件大小无关 |
| 业务数据不想经过第三方 | 流量在第三方边缘节点终止，合规与隐私难交代 | 客户端直连**你自己的**公网服务器，链路上没有第三方 |

---

## 二、与同类项目对比

> 数据来自各项目官方文档与 GitHub 仓库（查询时间 2026-10）。对比仅陈述形态差异，不代表优劣。

| 维度 | **DTNS HTTP Tunnel** | [ngrok](https://ngrok.com) | [frp](https://github.com/fatedier/frp) | [cloudflared](https://github.com/cloudflare/cloudflared) | [localtunnel](https://github.com/localtunnel/localtunnel) | [bore](https://github.com/ekzhang/bore) / [chisel](https://github.com/jpillora/chisel) |
| --- | --- | --- | --- | --- | --- | --- |
| 部署形态 | 自托管（client + server） | SaaS | 自托管（frpc + frps） | SaaS（Cloudflare 边缘） | 公共共享服务 | 自托管 |
| 需要公网服务器 | ✅ 需要 | ❌ 不需要 | ✅ 需要 | ❌ 不需要 | ❌ 不需要 | ✅ 需要 |
| 需要第三方账号 | ❌ 不需要 | ✅ 需要 | ❌ 不需要 | ✅ 需要（且域名需托管在 CF） | ❌ 不需要 | ❌ 不需要 |
| 固定自有域名 | ✅ 直接写配置 | ⚠️ 免费版仅 1 个自动分配域名，自定义域名需付费 | ✅ 支持 | ✅ 支持（但须接入 CF） | ❌ 随机子域名 | ⚠️ 偏端口，非域名虚拟主机 |
| 免费额度 / 插页 | 无限制（自建） | ⚠️ 月度配额 + HTML 插页 | 无限制（自建） | 无限制，但受 CF 服务条款约束 | 公共服务器限速、不稳定 | 无限制（自建） |
| 流量是否经第三方 | ❌ 不经过 | ✅ 经过 | ❌ 不经过 | ✅ 经过 | ✅ 经过 | ❌ 不经过 |
| 配置方式 | 单个 `config.js` 对象 | 账号 + CLI / 配置文件 | TOML / INI，多 proxy 类型 | YAML + CLI | CLI | CLI 参数 |
| 技术栈 | Node.js | Go | Go | Go | JavaScript | Rust / Go |
| 许可证 | GPLv3 | 商业（闭源） | Apache-2.0 | Apache-2.0 | MIT | MIT |

### 怎么选

- **只想临时演示、不想碰服务器** → ngrok 或 cloudflared 更省事
- **要功能最全、生态最大、能接受 TOML 配置** → frp 是成熟选择
- **要按域名的 HTTP 虚拟主机 + 长连接流式 + 配置极简 + 完全自控** → 本项目
- **只需要端口转发 / SOCKS 代理** → bore、chisel 更直接

---

## 三、差异化能力

以下能力均可在源码中直接对应到实现，不是路线图。

### 3.1 单连接多隧道 + Host 路由

客户端与 `server` 之间只维持**一条** WebSocket 连接，所有隧道复用这条连接；服务端按请求的 `Host` 头匹配已注册隧道并转发。新增一个服务只需在 `tunnels` 数组里加一段配置，不需要多开进程、不需要额外端口。

### 3.2 双向信用窗口流式传输

上下行各自维护独立的信用窗口（默认 1 MB，块大小 256 KB），接收方通过 `stream_ack` 归还额度。效果是：

- 内存占用有确定上界，与请求体/响应体大小**无关**，大文件上传下载不会撑爆服务器
- 声明了 `stream` 能力的客户端才走流式，旧客户端自动回退到全缓冲，**向后兼容**

### 3.3 协议 v2：多值响应头与 2 字节状态码

针对两个真实踩过的坑：

- **多 `Set-Cookie` 被合并**：v1 协议把响应头表达为 `key: string`，多个 `Set-Cookie` 只能逗号拼接，浏览器会解析错误。v2 允许响应头为数组，原样还原。
- **状态码被截断**：v1 状态码占 1 字节，`401` / `404` / `500` 这类大于 255 的状态码会变成非法值，导致所有 4xx/5xx 接口失败。v2 改用 2 字节状态码；即便对端仍是旧服务端，客户端也会自动把该响应降级为 JSON 文本帧发送，保证状态码无损。

### 3.4 按域名覆盖的请求体上限

`limits.maxBodySize` 提供全局兜底（默认 32 MB），`limits.maxBodySizeByDomain` 允许为特定域名单独放宽（例如文件站给 1 GB），超限直接返回 `413`，避免单个超大上传打爆内存。

### 3.5 守护与自愈

- 客户端内置 `daemon.js`：进程异常退出后自动拉起，并用完整路径匹配避免误杀其他 Node 进程
- 断线自动重连，间隔按尝试次数递增并设上限
- 可选注册后健康校验：隧道注册完成后逐个做一次 HTTP 探测，连续失败则触发全量重新注册

---

## 四、功能特性

- HTTP 与 WebSocket 双向代理（含 `101` 握手与子协议透传）
- SSE / 流式响应支持
- 基于域名的虚拟主机路由：`<子域名><domainBase>` → 指定隧道
- 二进制协议 v2，兼容 v1 对端
- 多隧道管理、断线自动重连、客户端守护进程
- 按域名可配置的请求体上限
- 上游 Basic Auth 注入（隧道级，可选）
- 响应头改写：`Location`、`Set-Cookie Domain`、`Host`、`Origin`（按需开启）

---

## 五、不适用场景

诚实说明边界，避免误选：

- **需要 UDP 转发或 P2P 打洞** — 本项目只做 HTTP/WebSocket，纯 TCP/UDP 场景请用 frp、bore
- **需要开箱即用的 Web 控制台、计费、团队协作** — 本项目是内核级组件，没有 UI
- **超高吞吐 / 十万级并发** — Node.js 单线程模型不适合压榨极限性能，请评估后再上生产
- **完全不想维护公网服务器** — 直接用 cloudflared

---

## 六、目录结构

```
.
├── client/
│   ├── index.js            # 客户端入口：连接、注册、心跳、重连
│   ├── config.js           # 客户端配置（示例，需自行填写）
│   ├── protocol.js         # 二进制协议编解码（v1 / v2）
│   ├── http-proxy.js       # 本地 HTTP/WS 转发、头部改写、流式
│   ├── tunnel-manager.js   # 隧道注册、状态与重连管理
│   ├── daemon.js           # 守护进程
│   └── package.json
├── server/
│   ├── index.js            # 服务端入口
│   ├── config.js           # 服务端配置（示例，需自行填写）
│   ├── protocol.js         # 二进制协议编解码（v1 / v2）
│   ├── proxy-server.js     # 公网 HTTP/WS 接入、Host 路由、SSE、流式
│   ├── websocket-server.js # 隧道 WebSocket 服务、隧道注册表
│   └── package.json
├── LICENSE
└── README.md
```

---

## 七、快速开始

### 环境要求

- Node.js 18+（推荐 LTS）
- 公网服务器，且客户端能访问服务端的 WebSocket 端口

### 1. 服务端（公网机器）

```bash
cd server
npm install
# 按需修改 config.js：监听端口、domainBase、limits、streaming
npm start
```

### 2. 客户端（内网机器）

```bash
cd client
npm install
# 按需修改 config.js：wsServerUrl、client.id、tunnels
npm start
```

### 3. 访问

浏览器访问 `http://<隧道域名>`，流量即被转发到该隧道配置的本地端口。

### 4. 生产建议

在服务端前放一层 Nginx 终止 TLS，并同时反代 HTTP 入口与 WebSocket 升级请求：

```nginx
location /tunnel {
    proxy_pass http://127.0.0.1:18385;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```

---

## 八、工作原理

```
                     ┌──────────────────────────────┐
   公网用户  ───────►│  Nginx（TLS 终止 / 反向代理）  │
                     └───────────────┬──────────────┘
                                     │ HTTP / WebSocket
                     ┌───────────────▼──────────────┐
                     │  server  公网 VPS             │
                     │  ├─ :18386  HTTP  按 Host 路由 │
                     │  └─ :18385  WS    隧道注册表   │
                     └───────────────┬──────────────┘
                                     │ 单条 WebSocket
                                     │ 二进制协议 v1 / v2
                     ┌───────────────▼──────────────┐
                     │  client  内网机器              │
                     │  ├─ 隧道 A → 127.0.0.1:3000   │
                     │  └─ 隧道 B → 127.0.0.1:8080   │
                     └──────────────────────────────┘
```

客户端主动向外建立连接，因此**内网侧无需开放任何入站端口**。

---

## 九、配置说明

两个目录下的 `config.js` 均为**示例配置**，按注释填写即可；请勿把真实域名、Token、证书私钥提交到仓库（`.gitignore` 已排除常见运行时文件）。

| 配置文件 | 关键项 |
| --- | --- |
| `server/config.js` | `http.port` / `http.domainBase`、`websocket.port`、`clients.heartbeatTimeout`、`streaming.*`、`limits.maxBodySize*` |
| `client/config.js` | `server.wsServerUrl`、`client.id`、`client.reconnect.*`、`tunnels[]`（`domain` / `local.host` / `local.port`） |

---

## 十、安全提示

1. 服务端目前**未内置接入鉴权**，请务必前置 Nginx 并配合 IP 白名单、Basic Auth 或自定义头部校验。
2. 生产环境请启用 HTTPS，避免隧道内容在公网明文传输。
3. 不要提交真实域名、Token、证书私钥、隧道 ID 等敏感信息。
4. 仅暴露必要端口，避免把数据库管理、调试接口等敏感服务直接映射到公网。

---

## 十一、路线图

- [ ] 服务端接入 Token / 签名鉴权
- [ ] 内置 Let's Encrypt 证书自动签发
- [ ] 可选的只读状态接口（隧道数、连接数、流量统计）
- [ ] Docker Compose 一键部署

---

## 许可证

本项目基于 [GNU General Public License v3.0](LICENSE)（GPLv3）开源。

> 注意：GPLv3 为强 Copyleft 许可证，分发本项目及其衍生作品时须以相同许可证开放源代码。
