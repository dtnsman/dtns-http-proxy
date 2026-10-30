# DTNS HTTP Tunnel

> 一个**可嵌入的** HTTP / WebSocket 内网穿透协议与服务端实现：一条 WebSocket 复用多条域名隧道，二进制帧原样透传，自托管、无第三方中转。

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Node](https://img.shields.io/badge/Node.js-%3E%3D18-339933.svg)](https://nodejs.org)
[![Gitee](https://img.shields.io/badge/Gitee-dtnsman%2Fdtns--http--proxy-c71d23.svg)](https://gitee.com/dtnsman/dtns-http-proxy)
[![GitHub](https://img.shields.io/badge/GitHub-dtnsman%2Fdtns--http--proxy-181717.svg)](https://github.com/dtnsman/dtns-http-proxy)

- `client/` — 运行在内网机器上，把本地 HTTP 服务通过一条出站 WebSocket 暴露到公网
- `server/` — 运行在公网服务器上，按请求的 `Host` 把流量路由到对应隧道

> 本仓库仅包含 `client` 与 `server` 两个目录的源码。仓库内的 `config.js` 均为**空白示例配置**，不含任何真实域名、密钥或隧道信息。

---

## 一、它最硬的证明：四个已发布产品长在它上面

讲优势之前先说事实。同一个作者在同一天（2026-10-27）开源了 5 个项目，除本项目外的 4 个**都是基于本项目的协议与隧道能力构建的**，且都已实际运行：

| 产品 | 它是什么 | 复用本项目的哪一部分 |
| --- | --- | --- |
| [**ibbot-pty**](https://github.com/dtnsman/ibbot-pty) ｜ [在线体验](http://cases.html.dtns.top/pty2.html) | 浏览器里的多主机终端面板 + 可嵌入 SDK，一次暴露 HTTP **和** WebSocket | `server/src/tunnel/dtns-protocol.js` + `tunnel-client.js` + `local-proxy.js` |
| [**DTNS AI Browser**](https://github.com/dtnsman/dtns-ai-browser) | 把**你已登录的 Chrome** 变成 69 个端点的 HTTP API（Chrome MV3 扩展） | `src/protocol/dtns-protocol.js`，二进制帧让 body 原样透传 |
| [**dsh-bridge**](https://github.com/dtnsman/dsh-bridge) ｜ [在线体验](http://cases.html.dtns.top/dsh-proxy-hosts.html) | 把 DeepSeek Harness 控制面桥接成 REST + SSE，手机/脚本/Agent 均可调用 | `src/dtns-protocol.js` + `src/tunnel.js`，内置隧道客户端 |
| [**dtns-http-proxy-site**](https://github.com/dtnsman/dtns-http-proxy-site) ｜ [在线体验](http://dtns-proxy.html.dtns.top/) | 项目站 + **零安装**方案：打开一个网页标签页，隧道就通了 | `assets/dtns-protocol.js`，协议实现在浏览器里直接跑 |

想直接看效果：`ibbot-pty`、`dsh-bridge`、`dtns-http-proxy-site` 都有公开的在线体验页（纯 HTTP，浏览器打开即用，无需安装），链接见上表。

四个产品的 Gitee 镜像同名：`gitee.com/dtnsman/{ibbot-pty, dtns-ai-browser, dsh-bridge, dtns-http-proxy-site}`。

```
                     ┌──────────────────────────────────────────────┐
                     │   同一套 server（自托管公网 VPS）              │
                     │   Host 路由 → 隧道注册表 → 转发               │
                     └───▲───────────▲───────────▲───────────▲──────┘
                         │           │           │           │  每个客户端一条 WS
        ┌────────────────┘           │           │           └────────────────┐
        │                            │           │                            │
┌───────────────┐         ┌──────────┴─────┐ ┌───┴────────────┐    ┌──────────────────┐
│  ibbot-pty    │         │ dtns-ai-browser│ │  dsh-bridge    │    │ proxy-site       │
│  终端面板+SDK  │         │  Chrome 扩展   │ │  DSH 桥接       │    │ 浏览器标签页 Agent │
│  Node 进程    │         │  浏览器内运行   │ │  Node 进程      │    │ 纯浏览器，零安装   │
└───────────────┘         └────────────────┘ └────────────────┘    └──────────────────┘
```

这张图说明了本项目与竞品最本质的区别：**它不是"一个要守护进程的工具"，而是"一份可以被嵌进任何地方的协议实现"**。frp、ngrok、cloudflared 都必须先装一个二进制并常驻，而本项目的协议可以跑在 Node 服务里，也可以跑在 Chrome 扩展和普通网页标签页里。

---

## 二、它解决什么痛点

下面每一条都标注了**哪个已发布产品在为它背书**，不是设想。

| # | 真实痛点 | 市面常见方案的处境 | 本项目的做法 | 实战验证 |
| --- | --- | --- | --- | --- |
| 1 | **目标机器装不了客户端** —— 公司电脑无管理员权限，安全策略禁止常驻进程 | ngrok / frp / cloudflared 都要装二进制并常驻 | 协议实现是单文件、零依赖，**浏览器扩展、网页标签页、Node 进程都能当客户端** | `dtns-ai-browser`（Chrome 扩展）、`proxy-site`（标签页） |
| 2 | **实时能力被缓冲打断** —— 做 AI 流式输出、实时看板时 SSE 一断一个准 | 很多方案 HTTP 转发很顺，WS/SSE 要额外配置或直接不支持 | 同一条 WS 上复用二进制帧，**WebSocket 全双工 + SSE 分块不缓冲** | `dsh-bridge`（事件流）、`ibbot-pty`（终端输出） |
| 3 | **大文件与长连接打爆内存** —— 整包缓冲，传个 500MB 就 OOM | 多数隧道按整包转发，没有背压概念 | **双向信用窗口**，内存上界 = 窗口 × 并发流数，与文件大小无关 | `ibbot-pty`（单文件 500MB 上传） |
| 4 | **多服务、多端口、多套配置** —— 一个服务一条隧道一个端口 | 每暴露一个服务就再起一个隧道进程 | **一条连接多隧道 + Host 路由**，一套服务端按域名分流 | 4 个产品可共用一套服务端，各占一个域名前缀 |
| 5 | **链接发出去就失效** —— 免费方案多为随机子域名 | 固定域名通常要付费 | `clientId` 持久化 + 自有域名，**重启后域名不变** | `dsh-bridge`（手机上的旧链接不失效） |
| 6 | **业务数据不想经第三方** —— 流量在第三方边缘终止，合规难交代 | SaaS 类方案全部经过厂商节点 | 客户端直连**你自己的**公网服务器 | 4 个产品全部自托管 |

### 一个具体的复合案例

`dtns-ai-browser` 要同时满足这几件事，任何一条不成立整个方案就废掉：

1. 浏览器扩展必须**主动出站**连隧道（用户在公司防火墙后面，没有公网 IP）
2. 页面截图与 DOM 回传是**二进制**，不能 base64（否则体积 +33%，截图接口直接不可用）
3. 69 个端点里有长轮询与流式读取，**不能被整包缓冲**
4. 域名必须稳定，否则用户每次重启浏览器都要重发链接

这正是本项目协议设计的四个约束条件。换句话说：**这些产品不是"顺便用一下隧道"，而是它们的可行性完全建立在本项目的协议特性上。**

---

## 三、与同类项目对比

> 竞品特性基于各项目公开文档与 GitHub 仓库整理（查询时间 2026-10）。价格与额度会变动，选型前请以官方为准。此表只比形态差异，不评价工程质量。

| 维度 | **DTNS HTTP Tunnel** | [ngrok](https://ngrok.com) | [frp](https://github.com/fatedier/frp) | [cloudflared](https://github.com/cloudflare/cloudflared) | [Tailscale Funnel](https://tailscale.com/kb/1223/funnel) | [localtunnel](https://github.com/localtunnel/localtunnel) |
| --- | --- | --- | --- | --- | --- | --- |
| 形态 | **可嵌入协议** + 自托管服务端 | SaaS + 常驻 agent | 自托管，frpc 常驻 | SaaS + 常驻守护进程 | 组网 + 公网出口 | 免费公共中继 |
| 需要公网服务器 | ✅ 需要 | ❌ | ✅ 需要 | ❌ | ❌ | ❌ |
| 需要第三方账号 | ❌ | ✅ | ❌ | ✅ 且域名须托管在 CF | ✅ | ❌ |
| 固定自有域名 | ✅ 直接写配置 | ⚠️ 免费版仅 1 个自动分配域名 | ✅ | ✅（须接入 CF） | ⚠️ 固定为 `*.ts.net` | ❌ 随机 |
| 能否嵌进浏览器/扩展 | ✅ **可以**（协议零依赖） | ❌ 只能装 agent | ❌ 只能跑 frpc | ❌ 只能跑守护进程 | ❌ 只能装客户端 | ❌ |
| WebSocket 全双工 | ✅ | ✅ | ✅（需配置） | ✅ | ✅ | ⚠️ 有限 |
| SSE / 流式响应 | ✅ 分块不缓冲 | ✅ | ✅ | ✅ | ✅ | ⚠️ 不稳定 |
| 双向背压 | ✅ 信用窗口 | 未知 | ⚠️ 有限 | 未知 | — | ❌ |
| 免费额度 / 插页 | 无限制（自建） | ⚠️ 月度配额 + HTML 插页 | 无限制 | 无限制（受 CF 条款约束） | 无限制（限端口） | 公共中继限速 |
| 流量是否经第三方 | ❌ 不经过 | ✅ 经过 | ❌ 不经过 | ✅ 经过 | ✅ 经过 | ✅ 经过 |
| 配置方式 | 单个 `config.js` 对象 | 账号 + CLI | TOML / INI | YAML + CLI | CLI + 后台 | CLI |
| 技术栈 / 许可证 | Node.js / GPLv3 | Go / 闭源 | Go / Apache-2.0 | Go / Apache-2.0 | Go / BSD-3 | JS / MIT |

### 怎么选

- **只想临时演示、不想碰服务器** → ngrok 或 cloudflared 更省事
- **要功能最全、生态最大、能接受 TOML** → frp 是成熟选择
- **要在受限终端（无管理员权限、只能用浏览器）打通** → 只有本项目这一类"协议可嵌入"的方案可行
- **要按域名的 HTTP 虚拟主机 + 长连接流式 + 完全自控** → 本项目
- **只需要端口转发 / SOCKS** → bore、chisel 更直接

---

## 四、四个竞争点

### 1. 协议可嵌入，不是必须常驻的守护进程

这是所有差异的源头。`protocol.js` 是**单文件、只依赖 Node 内置 `crypto`** 的编解码实现，可以被直接复制进任何 JS 运行时——Chrome 扩展的 service worker、网页标签页、Node 服务、无框架的静态站点。

结果就是 `dtns-http-proxy-site` 能做到「打开一个网页，隧道就通了」，`dtns-ai-browser` 能把整个隧道塞进 Chrome 扩展。这是 ngrok / frp / cloudflared 在架构上做不到的：它们的隧道能力和一个 Go 二进制绑死。

### 2. 一条连接多隧道 + Host 路由

每个客户端只维持**一条** WebSocket，所有隧道复用这条连接；服务端按请求 `Host` 匹配注册表。加一个服务只需在 `tunnels` 数组里加一段配置。

在生态层面这条能力被放大了：多个产品可以共用同一个服务端实例，靠不同域名前缀区分命名空间（`browser2.*`、`pty.*`、`dsh.*`），彼此不冲突，也不需要为每个产品单独部署一套服务端。

### 3. 协议级透传：WebSocket 全双工 + SSE + 背压

二进制帧把请求与响应**原样搬运**，因此同一条 WS 上可以同时承载 WebSocket 升级、双向帧、关闭码，以及不被截断的 SSE 分块响应。上下行各维护独立信用窗口（默认 1 MB / 块 256 KB），接收方用 `stream_ack` 归还额度——**内存占用有确定上界，与传输量无关**。

`ibbot-pty` 的 500MB 文件上传、`dsh-bridge` 的 SSE 事件流、`dtns-ai-browser` 的截图回传，用的都是这一层。

### 4. 向后兼容的版本协商

协议有 v1 / v2 两代，服务端在 `welcome` 帧里声明版本，客户端据此切换行为。v2 修掉了两个真实踩过的坑：

- **多 `Set-Cookie` 被合并**：v1 只能表达 `key: string`，多个 `Set-Cookie` 逗号拼接后浏览器解析错误；v2 允许响应头为数组，原样还原。
- **状态码被截断**：v1 状态码占 1 字节，`401` / `404` / `500` 会变成非法值；v2 改用 2 字节。即便对端仍是旧服务端，客户端也会自动把该响应降级为 JSON 文本帧发送，保证状态码无损。

同一个服务端可以同时服务 v1 和 v2 客户端，升级不需要"停机 + 全体换客户端"。

---

## 五、功能特性

- HTTP 与 WebSocket 双向代理（含 `101` 握手、子协议透传、关闭码归一化）
- SSE / 流式响应，分块不缓冲
- 双向信用窗口背压
- 基于域名的虚拟主机路由：`<子域名><domainBase>` → 指定隧道
- 二进制协议 v2，兼容 v1 对端
- 多隧道管理、断线自动重连、客户端守护进程
- 按域名覆盖的请求体上限（`413` 快速失败）
- 上游 Basic Auth 注入（隧道级，可选）
- 响应头改写：`Location`、`Set-Cookie Domain`、`Host`、`Origin`（按需开启）

---

## 六、不适用场景

诚实说明边界，避免误选：

- **需要 UDP 转发或 P2P 打洞** — 本项目只做 HTTP/WebSocket，纯 TCP/UDP 用 frp、bore
- **需要开箱即用的 Web 控制台、计费、RBAC** — 本仓库是内核组件，没有 UI 与账号体系
- **超高吞吐 / 十万级并发** — Node.js 单线程不适合压榨极限性能，请压测后再上生产
- **完全不想维护公网服务器** — 直接用 cloudflared
- **需要商业 SLA 与审计合规** — 本项目定位是轻量、可控、可自托管，不承诺 SLA

---

## 七、目录结构

```
.
├── client/
│   ├── index.js            # 客户端入口：连接、注册、心跳、重连
│   ├── config.js           # 客户端配置（示例，需自行填写）
│   ├── protocol.js         # 二进制协议编解码（v1 / v2，可被单独复用）
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

## 八、快速开始

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
    # SSE / 流式响应必须关闭缓冲
    proxy_buffering off;
}
```

---

## 九、工作原理

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

## 十、配置说明

两个目录下的 `config.js` 均为**示例配置**，按注释填写即可；请勿把真实域名、Token、证书私钥提交到仓库（`.gitignore` 已排除常见运行时文件）。

| 配置文件 | 关键项 |
| --- | --- |
| `server/config.js` | `http.port` / `http.domainBase`、`websocket.port`、`clients.heartbeatTimeout`、`streaming.*`、`limits.maxBodySize*` |
| `client/config.js` | `server.wsServerUrl`、`client.id`、`client.reconnect.*`、`tunnels[]`（`domain` / `local.host` / `local.port`） |

---

## 十一、安全提示

1. 服务端目前**未内置接入鉴权**，请务必前置 Nginx 并配合 IP 白名单、Basic Auth 或自定义头部校验。
2. 生产环境请启用 HTTPS，避免隧道内容在公网明文传输。
3. 不要提交真实域名、Token、证书私钥、隧道 ID 等敏感信息。
4. 仅暴露必要端口，避免把数据库管理、调试接口等敏感服务直接映射到公网。

---

## 十二、路线图

- [ ] 服务端接入 Token / 签名鉴权
- [ ] 内置 Let's Encrypt 证书自动签发
- [ ] 可选的只读状态接口（隧道数、连接数、流量统计）
- [ ] Docker Compose 一键部署
- [ ] 发布独立的 `dtns-protocol` npm 包，便于第三方直接嵌入

---

## 相关项目

| 项目 | 说明 | 在线体验 |
| --- | --- | --- |
| [ibbot-pty](https://github.com/dtnsman/ibbot-pty) | 多主机浏览器终端面板 + 可嵌入 SDK | [pty2.html](http://cases.html.dtns.top/pty2.html) |
| [dtns-ai-browser](https://github.com/dtnsman/dtns-ai-browser) | 把已登录的 Chrome 变成 HTTP API | — |
| [dsh-bridge](https://github.com/dtnsman/dsh-bridge) | DeepSeek Harness 控制面 → REST + SSE | [dsh-proxy-hosts.html](http://cases.html.dtns.top/dsh-proxy-hosts.html) |
| [dtns-http-proxy-site](https://github.com/dtnsman/dtns-http-proxy-site) | 项目站 + 浏览器标签页零安装 Agent | [dtns-proxy.html.dtns.top](http://dtns-proxy.html.dtns.top/) |

---

## 许可证

本项目基于 [GNU General Public License v3.0](LICENSE)（GPLv3）开源。

> 注意：GPLv3 为强 Copyleft 许可证，分发本项目及其衍生作品时须以相同许可证开放源代码。
