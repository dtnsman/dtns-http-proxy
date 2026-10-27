// protocol.js - 二进制协议定义（与服务端相同，纯二进制）
const crypto = require('crypto');

const PROTOCOL = {
  MAGIC: Buffer.from('DTNS', 'utf8'),
  VERSION: 0x01,
  // v2 起 http_response 的状态码改为 2 字节（UInt16BE），修复 3xx/4xx/5xx 被截断
  VERSION_2: 0x02,
  TYPE: {
    REGISTER_TUNNEL: 0x0001,
    UNREGISTER_TUNNEL: 0x0002,
    HEARTBEAT: 0x0003,
    HTTP_RESPONSE: 0x0004,
    TUNNEL_LIST: 0x0005,
    COMMAND_RESPONSE: 0x0006,
    STATS: 0x0007,
    SSE_DATA: 0x0008,
    SSE_END: 0x0009,
    SSE_ERROR: 0x000A,
    SSE_CLOSE: 0x000B,
    WEBSOCKET_UPGRADED: 0x000C,
    WEBSOCKET_DATA: 0x000D,
    WEBSOCKET_END: 0x000E,
    WEBSOCKET_ERROR: 0x000F,
    WEBSOCKET_CLOSE: 0x0010,
    SSE_CONNECTIONS: 0x0011,
    ERROR: 0x0012,

    // ===== 流式传输（双向）=====
    // 一条 HTTP 请求/响应可以拆成 open + 若干 chunk + end，靠 ack 做信用窗口。
    STREAM_OPEN: 0x0020,
    STREAM_CHUNK: 0x0021,
    STREAM_END: 0x0022,
    STREAM_ACK: 0x0023,
    STREAM_CANCEL: 0x0024,
    CAPABILITIES: 0x0025,
    
    WELCOME: 0x0101,
    REGISTERED: 0x0102,
    UNREGISTERED: 0x0103,
    HEARTBEAT_ACK: 0x0104,
    HTTP_REQUEST: 0x0105,
    TUNNEL_STATUS: 0x0106,
    COMMAND: 0x0107,
    WEBSOCKET_DATA_FROM_SERVER: 0x0108,
    WEBSOCKET_CLOSE_FROM_SERVER: 0x0109,
    SSE_DATA_FROM_SERVER: 0x010A,
    SSE_END_FROM_SERVER: 0x010B,
    SSE_ERROR_FROM_SERVER: 0x010C,
    SSE_CLOSE_FROM_SERVER: 0x010D,
  },
  
  FLAG: {
    NONE: 0x0000,
    HAS_BODY: 0x0001,
    HAS_HEADERS: 0x0002,
    IS_SSE: 0x0004,
    IS_WEBSOCKET: 0x0008,
    GZIPPED: 0x0010,
    // WebSocket 数据帧的原始类型：置位表示 binary frame，清零表示 text frame。
    // 旧实现靠 base64 + UTF-8 猜测，这个标志位让类型在两端无损传递。
    IS_BINARY: 0x0020,
  }
};

class BinaryCodec {
  static encode(msg) {
    const { type, ...fields } = msg;
    const typeCode = this.getTypeCode(type);
    const payload = this.encodePayload(typeCode, fields);
    
    const headerSize = 4 + 1 + 2 + 2 + 4;
    const totalSize = headerSize + payload.length;
    
    const buffer = Buffer.alloc(totalSize);
    let offset = 0;
    
    PROTOCOL.MAGIC.copy(buffer, offset);
    offset += 4;
    buffer.writeUInt8(PROTOCOL.VERSION, offset);
    offset += 1;
    buffer.writeUInt16BE(typeCode, offset);
    offset += 2;
    const flags = this.extractFlags(fields);
    buffer.writeUInt16BE(flags, offset);
    offset += 2;
    buffer.writeUInt32BE(payload.length, offset);
    offset += 4;
    payload.copy(buffer, offset);
    
    return buffer;
  }
  
  static decode(buffer) {
    if (buffer.length < 13) {
      throw new Error('消息太短');
    }
    
    let offset = 0;
    const magic = buffer.slice(offset, offset + 4);
    if (!magic.equals(PROTOCOL.MAGIC)) {
      throw new Error(`无效的魔数: ${magic.toString()}`);
    }
    offset += 4;
    const version = buffer.readUInt8(offset);
    offset += 1;
    const typeCode = buffer.readUInt16BE(offset);
    offset += 2;
    const flags = buffer.readUInt16BE(offset);
    offset += 2;
    const length = buffer.readUInt32BE(offset);
    offset += 4;
    const payload = buffer.slice(offset, offset + length);
    
    return this.decodePayload(typeCode, payload, flags, version);
  }
  
  static getTypeCode(type) {
    const typeMap = {
      'register_tunnel': PROTOCOL.TYPE.REGISTER_TUNNEL,
      'unregister_tunnel': PROTOCOL.TYPE.UNREGISTER_TUNNEL,
      'heartbeat': PROTOCOL.TYPE.HEARTBEAT,
      'http_response': PROTOCOL.TYPE.HTTP_RESPONSE,
      'tunnel_list': PROTOCOL.TYPE.TUNNEL_LIST,
      'command_response': PROTOCOL.TYPE.COMMAND_RESPONSE,
      'stats': PROTOCOL.TYPE.STATS,
      'sse_data': PROTOCOL.TYPE.SSE_DATA,
      'sse_end': PROTOCOL.TYPE.SSE_END,
      'sse_error': PROTOCOL.TYPE.SSE_ERROR,
      'sse_close': PROTOCOL.TYPE.SSE_CLOSE,
      'websocket_upgraded': PROTOCOL.TYPE.WEBSOCKET_UPGRADED,
      'websocket_data': PROTOCOL.TYPE.WEBSOCKET_DATA,
      'websocket_end': PROTOCOL.TYPE.WEBSOCKET_END,
      'websocket_error': PROTOCOL.TYPE.WEBSOCKET_ERROR,
      'websocket_close': PROTOCOL.TYPE.WEBSOCKET_CLOSE,
      'sse_connections': PROTOCOL.TYPE.SSE_CONNECTIONS,
      'error': PROTOCOL.TYPE.ERROR,
      'stream_open': PROTOCOL.TYPE.STREAM_OPEN,
      'stream_chunk': PROTOCOL.TYPE.STREAM_CHUNK,
      'stream_end': PROTOCOL.TYPE.STREAM_END,
      'stream_ack': PROTOCOL.TYPE.STREAM_ACK,
      'stream_cancel': PROTOCOL.TYPE.STREAM_CANCEL,
      'capabilities': PROTOCOL.TYPE.CAPABILITIES,
      'welcome': PROTOCOL.TYPE.WELCOME,
      'registered': PROTOCOL.TYPE.REGISTERED,
      'unregistered': PROTOCOL.TYPE.UNREGISTERED,
      'heartbeat_ack': PROTOCOL.TYPE.HEARTBEAT_ACK,
      'http_request': PROTOCOL.TYPE.HTTP_REQUEST,
      'tunnel_status': PROTOCOL.TYPE.TUNNEL_STATUS,
      'command': PROTOCOL.TYPE.COMMAND,
    };
    return typeMap[type] || PROTOCOL.TYPE.ERROR;
  }
  
  static getTypeName(typeCode) {
    const nameMap = {};
    for (const [name, code] of Object.entries(PROTOCOL.TYPE)) {
      nameMap[code] = name.toLowerCase();
    }
    return nameMap[typeCode] || 'unknown';
  }
  
  static encodePayload(typeCode, fields) {
    switch (typeCode) {
      case PROTOCOL.TYPE.REGISTER_TUNNEL:
        return this.encodeRegisterTunnel(fields);
      case PROTOCOL.TYPE.UNREGISTER_TUNNEL:
        return this.encodeUnregisterTunnel(fields);
      case PROTOCOL.TYPE.HEARTBEAT:
        return this.encodeHeartbeat(fields);
      case PROTOCOL.TYPE.HTTP_RESPONSE:
        return this.encodeHttpResponse(fields);
      case PROTOCOL.TYPE.HTTP_REQUEST:
        return this.encodeHttpRequest(fields);
      case PROTOCOL.TYPE.WEBSOCKET_DATA:
      case PROTOCOL.TYPE.WEBSOCKET_DATA_FROM_SERVER:
        return this.encodeWebSocketData(fields);
      case PROTOCOL.TYPE.WEBSOCKET_CLOSE:
      case PROTOCOL.TYPE.WEBSOCKET_CLOSE_FROM_SERVER:
        return this.encodeWebSocketClose(fields);
      case PROTOCOL.TYPE.SSE_DATA:
      case PROTOCOL.TYPE.SSE_DATA_FROM_SERVER:
        return this.encodeSSEData(fields);
      case PROTOCOL.TYPE.COMMAND:
        return this.encodeCommand(fields);
      case PROTOCOL.TYPE.WELCOME:
        return this.encodeWelcome(fields);
      case PROTOCOL.TYPE.REGISTERED:
        return this.encodeRegistered(fields);
      case PROTOCOL.TYPE.STATS:
        return this.encodeStats(fields);
      case PROTOCOL.TYPE.TUNNEL_LIST:
        return this.encodeTunnelList(fields);
      case PROTOCOL.TYPE.COMMAND_RESPONSE:
        return this.encodeCommandResponse(fields);
      case PROTOCOL.TYPE.ERROR:
        return this.encodeError(fields);
      case PROTOCOL.TYPE.STREAM_OPEN:
        return this.encodeStreamOpen(fields);
      case PROTOCOL.TYPE.STREAM_CHUNK:
        return this.encodeStreamChunk(fields);
      case PROTOCOL.TYPE.STREAM_END:
        return this.encodeStreamEnd(fields);
      case PROTOCOL.TYPE.STREAM_ACK:
        return this.encodeStreamAck(fields);
      case PROTOCOL.TYPE.STREAM_CANCEL:
        return this.encodeStreamCancel(fields);
      case PROTOCOL.TYPE.CAPABILITIES:
        return this.encodeCapabilities(fields);
      default:
        // 没有专用编码器的消息（如 heartbeat_ack）走通用 JSON：
        // 必须把 type 一起带上，否则对端解出来 type=undefined，日志里全是"未知消息类型"。
        return this.encodeGeneric({ type: this.getTypeName(typeCode), ...fields });
    }
  }
  
  static decodePayload(typeCode, payload, flags, version) {
    switch (typeCode) {
      case PROTOCOL.TYPE.REGISTER_TUNNEL:
        return this.decodeRegisterTunnel(payload);
      case PROTOCOL.TYPE.UNREGISTER_TUNNEL:
        return this.decodeUnregisterTunnel(payload);
      case PROTOCOL.TYPE.HEARTBEAT:
        return this.decodeHeartbeat(payload);
      case PROTOCOL.TYPE.HTTP_RESPONSE:
        return this.decodeHttpResponse(payload, flags, version);
      case PROTOCOL.TYPE.HTTP_REQUEST:
        return this.decodeHttpRequest(payload, flags);
      case PROTOCOL.TYPE.WEBSOCKET_DATA:
      case PROTOCOL.TYPE.WEBSOCKET_DATA_FROM_SERVER:
        return this.decodeWebSocketData(payload, flags);
      case PROTOCOL.TYPE.WEBSOCKET_CLOSE:
      case PROTOCOL.TYPE.WEBSOCKET_CLOSE_FROM_SERVER:
        return this.decodeWebSocketClose(payload);
      case PROTOCOL.TYPE.SSE_DATA:
      case PROTOCOL.TYPE.SSE_DATA_FROM_SERVER:
        return this.decodeSSEData(payload);
      case PROTOCOL.TYPE.COMMAND:
        return this.decodeCommand(payload);
      case PROTOCOL.TYPE.WELCOME:
        return this.decodeWelcome(payload);
      case PROTOCOL.TYPE.REGISTERED:
        return this.decodeRegistered(payload);
      case PROTOCOL.TYPE.STATS:
        return this.decodeStats(payload);
      case PROTOCOL.TYPE.TUNNEL_LIST:
        return this.decodeTunnelList(payload);
      case PROTOCOL.TYPE.COMMAND_RESPONSE:
        return this.decodeCommandResponse(payload);
      case PROTOCOL.TYPE.ERROR:
        return this.decodeError(payload);
      case PROTOCOL.TYPE.STREAM_OPEN:
        return this.decodeStreamOpen(payload);
      case PROTOCOL.TYPE.STREAM_CHUNK:
        return this.decodeStreamChunk(payload);
      case PROTOCOL.TYPE.STREAM_END:
        return this.decodeStreamEnd(payload);
      case PROTOCOL.TYPE.STREAM_ACK:
        return this.decodeStreamAck(payload);
      case PROTOCOL.TYPE.STREAM_CANCEL:
        return this.decodeStreamCancel(payload);
      case PROTOCOL.TYPE.CAPABILITIES:
        return this.decodeCapabilities(payload);
      default:
        return this.decodeGeneric(payload);
    }
  }
  
  // ==================== 工具函数 ====================
  
  static encodeString(str) {
    if (!str) return Buffer.alloc(2);
    const buf = Buffer.from(str, 'utf8');
    const result = Buffer.alloc(2 + buf.length);
    result.writeUInt16BE(buf.length, 0);
    buf.copy(result, 2);
    return result;
  }
  
  static decodeString(buffer, offset) {
    const len = buffer.readUInt16BE(offset);
    const str = buffer.slice(offset + 2, offset + 2 + len).toString('utf8');
    return { value: str, nextOffset: offset + 2 + len };
  }
  
  static encodeHeaders(headers, options) {
    const multi = !!(options && options.multiValue);
    if (!headers) return Buffer.alloc(2);
    const entries = Object.entries(headers);
    const parts = [];
    let totalLen = 2;
    for (const [key, value] of entries) {
      if (value === undefined || value === null) continue;
      const keyBuf = this.encodeString(key);
      const valStr = Array.isArray(value)
        ? value.map(String).join(multi ? '\n' : ',')
        : String(value);
      const valBuf = this.encodeString(valStr);
      parts.push(keyBuf, valBuf);
      totalLen += keyBuf.length + valBuf.length;
    }
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    result.writeUInt16BE(entries.filter(([k, v]) => v !== undefined && v !== null).length, offset);
    offset += 2;
    for (const [key, value] of entries) {
      if (value === undefined || value === null) continue;
      const keyBuf = this.encodeString(key);
      const valStr = Array.isArray(value)
        ? value.map(String).join(multi ? '\n' : ',')
        : String(value);
      const valBuf = this.encodeString(valStr);
      keyBuf.copy(result, offset);
      offset += keyBuf.length;
      valBuf.copy(result, offset);
      offset += valBuf.length;
    }
    return result;
  }
  
  static decodeHeaders(buffer, offset) {
    const count = buffer.readUInt16BE(offset);
    offset += 2;
    const headers = {};
    for (let i = 0; i < count; i++) {
      const { value: key, nextOffset: off1 } = this.decodeString(buffer, offset);
      const { value: val, nextOffset: off2 } = this.decodeString(buffer, off1);
      headers[key] = val.indexOf('\n') >= 0 ? val.split('\n') : val;
      offset = off2;
    }
    return { value: headers, nextOffset: offset };
  }
  
  static encodeBuffer(buf) {
    if (!buf || buf.length === 0) return Buffer.alloc(4);
    const result = Buffer.alloc(4 + buf.length);
    result.writeUInt32BE(buf.length, 0);
    buf.copy(result, 4);
    return result;
  }
  
  static decodeBuffer(buffer, offset) {
    const len = buffer.readUInt32BE(offset);
    const buf = buffer.slice(offset + 4, offset + 4 + len);
    return { value: buf, nextOffset: offset + 4 + len };
  }
  
  static extractFlags(fields) {
    let flags = 0;
    if (fields.body) flags |= PROTOCOL.FLAG.HAS_BODY;
    if (fields.headers) flags |= PROTOCOL.FLAG.HAS_HEADERS;
    if (fields.isSSE) flags |= PROTOCOL.FLAG.IS_SSE;
    if (fields.isWebSocket) flags |= PROTOCOL.FLAG.IS_WEBSOCKET;
    if (fields.binary) flags |= PROTOCOL.FLAG.IS_BINARY;
    return flags;
  }
  
  // ==================== HTTP Request - 纯二进制 ====================
  static encodeRegisterTunnel(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(fields.tunnel?.id || ''));
    parts.push(this.encodeString(fields.tunnel?.domain || ''));
    parts.push(Buffer.from([fields.tunnel?.localPort || 80]));
    parts.push(this.encodeString(fields.tunnel?.localHost || '127.0.0.1'));
    parts.push(this.encodeString(fields.tunnel?.protocol || 'http'));
    parts.push(this.encodeString(fields.tunnel?.description || ''));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeRegisterTunnel(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: id, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: domain, nextOffset: o3 } = this.decodeString(payload, offset);
    offset = o3;
    const localPort = payload.readUInt8(offset);
    offset += 1;
    const { value: localHost, nextOffset: o4 } = this.decodeString(payload, offset);
    offset = o4;
    const { value: protocol, nextOffset: o5 } = this.decodeString(payload, offset);
    offset = o5;
    const { value: description, nextOffset: o6 } = this.decodeString(payload, offset);
    
    return {
      type: 'register_tunnel',
      clientId,
      tunnel: { id, domain, localPort, localHost, protocol, description }
    };
  }
  
  static encodeUnregisterTunnel(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(fields.domain || ''));
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeUnregisterTunnel(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: domain, nextOffset: o2 } = this.decodeString(payload, offset);
    return { type: 'unregister_tunnel', clientId, domain };
  }
  
  static encodeHeartbeat(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const stats = fields.stats || {};
    const statsBuf = Buffer.alloc(20);
    statsBuf.writeUInt32BE(stats.tunnelCount || 0, 0);
    statsBuf.writeUInt32BE(stats.registeredCount || 0, 4);
    statsBuf.writeUInt32BE(stats.sseConnections || 0, 8);
    statsBuf.writeUInt32BE(stats.totalRequests || 0, 12);
    statsBuf.writeUInt32BE(stats.totalErrors || 0, 16);
    parts.push(statsBuf);
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeHeartbeat(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: timestamp, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const tunnelCount = payload.readUInt32BE(offset);
    const registeredCount = payload.readUInt32BE(offset + 4);
    const sseConnections = payload.readUInt32BE(offset + 8);
    const totalRequests = payload.readUInt32BE(offset + 12);
    const totalErrors = payload.readUInt32BE(offset + 16);
    
    return {
      type: 'heartbeat',
      clientId,
      timestamp,
      stats: { tunnelCount, registeredCount, sseConnections, totalRequests, totalErrors }
    };
  }
  static encodeHttpRequest(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.requestId || ''));
    parts.push(this.encodeString(fields.domain || ''));
    parts.push(this.encodeString(fields.method || 'GET'));
    parts.push(this.encodeString(fields.url || '/'));
    parts.push(this.encodeHeaders(fields.headers || {}, { multiValue: fields.multiValue === true }));
    
    const bodyBuffer = Buffer.isBuffer(fields.body) ? fields.body : Buffer.alloc(0);
    parts.push(this.encodeBuffer(bodyBuffer));
    
    parts.push(this.encodeString(fields.remoteAddress || ''));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const flags = Buffer.alloc(1);
    let f = 0;
    if (fields.isSSE) f |= 0x01;
    if (fields.isWebSocket) f |= 0x02;
    flags.writeUInt8(f, 0);
    parts.push(flags);
    
    parts.push(this.encodeString(fields.sseRequestId || ''));
    parts.push(this.encodeString(fields.wsRequestId || ''));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeHttpRequest(payload, flags) {
    let offset = 0;
    const { value: requestId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: domain, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: method, nextOffset: o3 } = this.decodeString(payload, offset);
    offset = o3;
    const { value: url, nextOffset: o4 } = this.decodeString(payload, offset);
    offset = o4;
    const { value: headers, nextOffset: o5 } = this.decodeHeaders(payload, offset);
    offset = o5;
    
    const { value: bodyBuf, nextOffset: o6 } = this.decodeBuffer(payload, offset);
    offset = o6;
    
    const { value: remoteAddress, nextOffset: o7 } = this.decodeString(payload, offset);
    offset = o7;
    const { value: timestamp, nextOffset: o8 } = this.decodeString(payload, offset);
    offset = o8;
    const f = payload.readUInt8(offset);
    offset += 1;
    const { value: sseRequestId, nextOffset: o9 } = this.decodeString(payload, offset);
    offset = o9;
    const { value: wsRequestId, nextOffset: o10 } = this.decodeString(payload, offset);
    
    return {
      type: 'http_request',
      requestId,
      domain,
      method,
      url,
      headers,
      body: bodyBuf,
      remoteAddress,
      timestamp,
      isSSE: !!(f & 0x01),
      isWebSocket: !!(f & 0x02),
      sseRequestId: sseRequestId || null,
      wsRequestId: wsRequestId || null
    };
  }
  
  // ==================== HTTP Response - 纯二进制 ====================
  
  static encodeHttpResponse(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.requestId || ''));
    parts.push(this.encodeString(fields.domain || ''));
    parts.push(Buffer.from([fields.status || 200]));
    parts.push(this.encodeHeaders(fields.headers || {}, { multiValue: fields.multiValue === true }));
    
    const bodyBuffer = Buffer.isBuffer(fields.body) ? fields.body : Buffer.alloc(0);
    parts.push(this.encodeBuffer(bodyBuffer));
    
    const flagsBuf = Buffer.alloc(1);
    let f = 0;
    if (fields.isSSE) f |= 0x01;
    if (fields.isWebSocket) f |= 0x02;
    if (fields.error) f |= 0x04;
    flagsBuf.writeUInt8(f, 0);
    parts.push(flagsBuf);
    
    parts.push(this.encodeString(fields.sseRequestId || ''));
    parts.push(this.encodeString(fields.wsRequestId || ''));
    parts.push(this.encodeString(fields.error || ''));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  // protocol.js - decodeHttpResponse
  static decodeHttpResponse(payload, flags, version) {
    let offset = 0;
    const { value: requestId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: domain, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    
    // ========== 读取状态码并验证 ==========
    let rawStatus;
    if (version >= PROTOCOL.VERSION_2) {
      rawStatus = payload.readUInt16BE(offset);
      offset += 2;
    } else {
      rawStatus = payload.readUInt8(offset);
      offset += 1;
    }
    
    let status = rawStatus;
    if (typeof status !== 'number' || status < 100 || status > 599) {
      console.warn(`[decodeHttpResponse] 无效状态码: ${status}, 使用 200`);
      status = 200;
    }
    
    const { value: headers, nextOffset: o3 } = this.decodeHeaders(payload, offset);
    offset = o3;
    
    const { value: bodyBuf, nextOffset: o4 } = this.decodeBuffer(payload, offset);
    offset = o4;
    
    const f = payload.readUInt8(offset);
    offset += 1;
    const { value: sseRequestId, nextOffset: o5 } = this.decodeString(payload, offset);
    offset = o5;
    const { value: wsRequestId, nextOffset: o6 } = this.decodeString(payload, offset);
    offset = o6;
    const { value: error, nextOffset: o7 } = this.decodeString(payload, offset);
    
    return {
      type: 'http_response',
      requestId,
      domain,
      status: status,  // 已验证的状态码
      headers,
      body: bodyBuf,
      isSSE: !!(f & 0x01),
      isWebSocket: !!(f & 0x02),
      error: (f & 0x04) ? error : null,
      sseRequestId: sseRequestId || null,
      wsRequestId: wsRequestId || null
    };
  }
  
  // ==================== WebSocket Data - 纯二进制 ====================
  
  static encodeWebSocketData(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.wsRequestId || ''));
    
    let dataBuffer;
    if (fields.data) {
      if (Buffer.isBuffer(fields.data)) {
        dataBuffer = fields.data;
      } else {
        dataBuffer = Buffer.from(fields.data, 'base64');
      }
    } else {
      dataBuffer = Buffer.alloc(0);
    }
    parts.push(this.encodeBuffer(dataBuffer));
    
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeWebSocketData(payload, flags = 0) {
    let offset = 0;
    const { value: wsRequestId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: dataBuf, nextOffset: o2 } = this.decodeBuffer(payload, offset);
    offset = o2;
    const { value: timestamp, nextOffset: o3 } = this.decodeString(payload, offset);
    
    // 直接返回原始字节（不再转 base64），由上层按 binary 标志决定发 text 还是 binary frame
    return {
      type: 'websocket_data',
      wsRequestId,
      data: dataBuf,
      isBinary: (flags & PROTOCOL.FLAG.IS_BINARY) !== 0,
      timestamp
    };
  }
  
  static encodeWebSocketClose(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.wsRequestId || ''));
    parts.push(Buffer.from([fields.code || 1000]));
    parts.push(this.encodeString(fields.reason || ''));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeWebSocketClose(payload) {
    let offset = 0;
    const { value: wsRequestId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const code = payload.readUInt8(offset);
    offset += 1;
    const { value: reason, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: timestamp, nextOffset: o3 } = this.decodeString(payload, offset);
    
    return {
      type: 'websocket_close',
      wsRequestId,
      code,
      reason,
      timestamp
    };
  }
  
  static encodeSSEData(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.sseRequestId || ''));
    
    const data = typeof fields.data === 'string' 
      ? Buffer.from(fields.data, 'utf8')
      : Buffer.from(JSON.stringify(fields.data || ''), 'utf8');
    parts.push(this.encodeBuffer(data));
    
    parts.push(this.encodeString(fields.event || ''));
    parts.push(this.encodeString(fields.id || ''));
    parts.push(Buffer.from([fields.retry || 0]));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeSSEData(payload) {
    let offset = 0;
    const { value: sseRequestId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: dataBuf, nextOffset: o2 } = this.decodeBuffer(payload, offset);
    offset = o2;
    const { value: event, nextOffset: o3 } = this.decodeString(payload, offset);
    offset = o3;
    const { value: id, nextOffset: o4 } = this.decodeString(payload, offset);
    offset = o4;
    const retry = payload.readUInt8(offset);
    offset += 1;
    const { value: timestamp, nextOffset: o5 } = this.decodeString(payload, offset);
    
    const dataStr = dataBuf.toString('utf8');
    return {
      type: 'sse_data',
      sseRequestId,
      data: dataStr,
      event: event || null,
      id: id || null,
      retry: retry || null,
      timestamp
    };
  }
  
  static encodeWelcome(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    parts.push(this.encodeString(fields.message || ''));
    parts.push(Buffer.from([fields.protocolVersion || PROTOCOL.VERSION]));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeWelcome(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: timestamp, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: message, nextOffset: o3 } = this.decodeString(payload, offset);
    offset = o3;
    let protocolVersion = PROTOCOL.VERSION;
    if (offset < payload.length) {
      protocolVersion = payload.readUInt8(offset);
    }
    
    return { type: 'welcome', clientId, timestamp, message, protocolVersion };
  }
  
  static encodeRegistered(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.domain || ''));
    parts.push(Buffer.from([fields.success ? 1 : 0]));
    parts.push(this.encodeString(fields.error || ''));
    parts.push(this.encodeString(fields.message || ''));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeRegistered(payload) {
    let offset = 0;
    const { value: domain, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const success = payload.readUInt8(offset) === 1;
    offset += 1;
    const { value: error, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: message, nextOffset: o3 } = this.decodeString(payload, offset);
    
    return { type: 'registered', domain, success: !!success, error: error || null, message };
  }
  
  static encodeCommand(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.command || ''));
    
    const dataStr = JSON.stringify(fields.data || {});
    parts.push(this.encodeBuffer(Buffer.from(dataStr, 'utf8')));
    
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeCommand(payload) {
    let offset = 0;
    const { value: command, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: dataBuf, nextOffset: o2 } = this.decodeBuffer(payload, offset);
    offset = o2;
    const { value: timestamp, nextOffset: o3 } = this.decodeString(payload, offset);
    
    let data = {};
    try {
      data = JSON.parse(dataBuf.toString('utf8'));
    } catch (e) {}
    
    return { type: 'command', command, data, timestamp };
  }
  
  static encodeStats(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(JSON.stringify(fields.clientStats || {})));
    parts.push(this.encodeString(JSON.stringify(fields.tunnelStats || {})));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeStats(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: clientStatsStr, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: tunnelStatsStr, nextOffset: o3 } = this.decodeString(payload, offset);
    
    let clientStats = {}, tunnelStats = {};
    try { clientStats = JSON.parse(clientStatsStr); } catch (e) {}
    try { tunnelStats = JSON.parse(tunnelStatsStr); } catch (e) {}
    
    return { type: 'stats', clientId, clientStats, tunnelStats };
  }
  
  static encodeTunnelList(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    parts.push(this.encodeString(JSON.stringify(fields.tunnels || [])));
    parts.push(this.encodeString(JSON.stringify(fields.registered || [])));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeTunnelList(payload) {
    let offset = 0;
    const { value: clientId, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: tunnelsStr, nextOffset: o2 } = this.decodeString(payload, offset);
    offset = o2;
    const { value: registeredStr, nextOffset: o3 } = this.decodeString(payload, offset);
    
    let tunnels = [], registered = [];
    try { tunnels = JSON.parse(tunnelsStr); } catch (e) {}
    try { registered = JSON.parse(registeredStr); } catch (e) {}
    
    return { type: 'tunnel_list', clientId, tunnels, registered };
  }
  
  static encodeCommandResponse(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.command || ''));
    parts.push(Buffer.from([fields.success ? 1 : 0]));
    parts.push(this.encodeString(fields.message || ''));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeCommandResponse(payload) {
    let offset = 0;
    const { value: command, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const success = payload.readUInt8(offset) === 1;
    offset += 1;
    const { value: message, nextOffset: o2 } = this.decodeString(payload, offset);
    
    return { type: 'command_response', command, success: !!success, message };
  }
  
  static encodeError(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.error || ''));
    parts.push(this.encodeString(fields.timestamp || new Date().toISOString()));
    
    const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
    const result = Buffer.alloc(totalLen);
    let offset = 0;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }
    return result;
  }
  
  static decodeError(payload) {
    let offset = 0;
    const { value: error, nextOffset: o1 } = this.decodeString(payload, offset);
    offset = o1;
    const { value: timestamp, nextOffset: o2 } = this.decodeString(payload, offset);
    
    return { type: 'error', error, timestamp };
  }

  // ==================== 流式传输（双向） ====================
  // 一条传输被拆成 open + chunk* + end，ack 用于信用窗口（credit）。
  // direction: 'up'（浏览器 -> 本地应用）| 'down'（本地应用 -> 浏览器）

  static encodeStreamOpen(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.streamId || ''));
    parts.push(this.encodeString(fields.requestId || ''));
    parts.push(this.encodeString(fields.direction || 'up'));
    parts.push(this.encodeString(fields.method || ''));
    parts.push(this.encodeString(fields.url || ''));
    const statusBuf = Buffer.alloc(2);
    statusBuf.writeUInt16BE(fields.status || 0, 0);
    parts.push(statusBuf);
    parts.push(this.encodeHeaders(fields.headers || {}, { multiValue: fields.multiValue === true }));
    return Buffer.concat(parts);
  }

  static decodeStreamOpen(payload) {
    let offset = 0;
    const a = this.decodeString(payload, offset); offset = a.nextOffset;
    const b = this.decodeString(payload, offset); offset = b.nextOffset;
    const c = this.decodeString(payload, offset); offset = c.nextOffset;
    const d = this.decodeString(payload, offset); offset = d.nextOffset;
    const e = this.decodeString(payload, offset); offset = e.nextOffset;
    const status = payload.readUInt16BE(offset); offset += 2;
    const h = this.decodeHeaders(payload, offset);

    return {
      type: 'stream_open',
      streamId: a.value,
      requestId: b.value,
      direction: c.value || 'up',
      method: d.value,
      url: e.value,
      status,
      headers: h.value,
    };
  }

  static encodeStreamChunk(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.streamId || ''));
    const buf = Buffer.isBuffer(fields.data) ? fields.data : Buffer.from(fields.data || []);
    parts.push(this.encodeBuffer(buf));
    return Buffer.concat(parts);
  }

  static decodeStreamChunk(payload) {
    let offset = 0;
    const a = this.decodeString(payload, offset); offset = a.nextOffset;
    const b = this.decodeBuffer(payload, offset);
    return { type: 'stream_chunk', streamId: a.value, data: b.value };
  }

  static encodeStreamEnd(fields) {
    return this.encodeString(fields.streamId || '');
  }

  static decodeStreamEnd(payload) {
    const a = this.decodeString(payload, 0);
    return { type: 'stream_end', streamId: a.value };
  }

  static encodeStreamAck(fields) {
    const idBuf = this.encodeString(fields.streamId || '');
    const creditBuf = Buffer.alloc(4);
    creditBuf.writeUInt32BE(fields.credit || 0, 0);
    return Buffer.concat([idBuf, creditBuf]);
  }

  static decodeStreamAck(payload) {
    const a = this.decodeString(payload, 0);
    const credit = payload.readUInt32BE(a.nextOffset);
    return { type: 'stream_ack', streamId: a.value, credit };
  }

  static encodeStreamCancel(fields) {
    return Buffer.concat([
      this.encodeString(fields.streamId || ''),
      this.encodeString(fields.reason || ''),
    ]);
  }

  static decodeStreamCancel(payload) {
    const a = this.decodeString(payload, 0);
    const b = this.decodeString(payload, a.nextOffset);
    return { type: 'stream_cancel', streamId: a.value, reason: b.value };
  }

  // ==================== 能力声明 ====================
  // 客户端连上后立刻声明自己支持哪些能力，服务端据此决定是否走流式，
  // 避免把 stream_open 发给不认识的旧客户端导致请求悬挂。

  static encodeCapabilities(fields) {
    const parts = [];
    parts.push(this.encodeString(fields.clientId || ''));
    const flag = Buffer.alloc(1);
    let f = 0;
    if (fields.stream) f |= 0x01;
    // bit1: 支持多值头（数组用换行编码）
    if (fields.multiValueHeaders) f |= 0x02;
    flag.writeUInt8(f, 0);
    parts.push(flag);
    return Buffer.concat(parts);
  }

  static decodeCapabilities(payload) {
    const a = this.decodeString(payload, 0);
    const f = payload.readUInt8(a.nextOffset);
    return {
      type: 'capabilities',
      clientId: a.value,
      stream: (f & 0x01) !== 0,
      multiValueHeaders: (f & 0x02) !== 0,
    };
  }
  
  static encodeGeneric(fields) {
    const json = JSON.stringify(fields);
    return this.encodeBuffer(Buffer.from(json, 'utf8'));
  }
  
  static decodeGeneric(payload) {
    const { value: buf } = this.decodeBuffer(payload, 0);
    try {
      return JSON.parse(buf.toString('utf8'));
    } catch (e) {
      return {};
    }
  }
}

module.exports = { PROTOCOL, BinaryCodec };