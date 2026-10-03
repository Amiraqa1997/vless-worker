import { connect } from "cloudflare:sockets";

const UUID = "2ed36262-3716-46dd-878d-442661ab1dbd";
const PATH = "/vless";

export default {
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade");

    console.log("REQUEST:", request.method, request.url);
    console.log("UPGRADE:", upgrade);

    // Normal browser request
    if (upgrade !== "websocket") {
      return new Response("VLESS Worker", { status: 200 });
    }

    const url = new URL(request.url);

    if (url.pathname !== PATH) {
      console.log("BAD PATH:", url.pathname);
      return new Response("Not Found", { status: 404 });
    }

    console.log("WEBSOCKET REQUEST RECEIVED");

    const webSocketPair = new WebSocketPair();
    const client = webSocketPair[0];
    const server = webSocketPair[1];

    // Explicitly receive binary WebSocket frames as ArrayBuffer.
    server.binaryType = "arraybuffer";

    server.accept({
      allowHalfOpen: true,
    });

    console.log("WEBSOCKET ACCEPTED");

    handleVless(server).catch((error) => {
      console.error("HANDLE ERROR:", error?.stack || error);

      try {
        server.close(1011, "Internal error");
      } catch {}
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleVless(ws) {
  let socket = null;
  let writer = null;
  let initialized = false;

  console.log("WAITING FOR VLESS MESSAGE");

  const firstMessage = await new Promise((resolve, reject) => {
    const onMessage = (event) => {
      console.log("MESSAGE RECEIVED");

      if (typeof event.data === "string") {
        console.log("MESSAGE TYPE: STRING");
        reject(new Error("VLESS message was text"));
        return;
      }

      let data;

      if (event.data instanceof ArrayBuffer) {
        data = new Uint8Array(event.data);
      } else if (event.data instanceof Blob) {
        event.data.arrayBuffer().then((buffer) => {
          resolve(new Uint8Array(buffer));
        }).catch(reject);

        return;
      } else {
        reject(new Error("Unknown WebSocket message type"));
        return;
      }

      console.log("MESSAGE SIZE:", data.length);

      resolve(data);
    };

    ws.addEventListener("message", onMessage, { once: true });

    ws.addEventListener(
      "close",
      () => reject(new Error("WebSocket closed before VLESS message")),
      { once: true }
    );

    ws.addEventListener(
      "error",
      () => reject(new Error("WebSocket error before VLESS message")),
      { once: true }
    );
  });

  console.log(
    "FIRST BYTES:",
    Array.from(firstMessage.slice(0, 12))
      .map((x) => x.toString(16).padStart(2, "0"))
      .join(" ")
  );

  const requestInfo = parseVlessRequest(firstMessage);

  console.log(
    "VLESS TARGET:",
    requestInfo.hostname + ":" + requestInfo.port
  );

  console.log("VLESS INITIAL PAYLOAD:", requestInfo.payload.length);

  // Connect to requested TCP destination.
  console.log("CONNECTING TCP...");

  try {
    socket = connect({
      hostname: requestInfo.hostname,
      port: requestInfo.port,
    });

    await socket.opened;

    console.log("TCP CONNECTED");
  } catch (error) {
    console.error("TCP CONNECTION FAILED:", error?.stack || error);

    try {
      ws.close(1011, "TCP connection failed");
    } catch {}

    return;
  }

  initialized = true;

  // VLESS response header.
  ws.send(new Uint8Array([0, 0]));

  console.log("VLESS RESPONSE SENT");

  // Keep one writer for the WebSocket -> TCP direction.
  writer = socket.writable.getWriter();

  try {
    // Send the data that came after the VLESS header.
    if (requestInfo.payload.length > 0) {
      await writer.write(requestInfo.payload);

      console.log(
        "INITIAL PAYLOAD SENT:",
        requestInfo.payload.length
      );
    }

    // TCP -> WebSocket
    const tcpToWs = relayTcpToWebSocket(socket, ws);

    // WebSocket -> TCP
    const wsToTcp = relayWebSocketToTcp(ws, writer);

    await Promise.all([tcpToWs, wsToTcp]);
  } catch (error) {
    console.error("RELAY ERROR:", error?.stack || error);
  } finally {
    if (initialized) {
      console.log("CONNECTION CLOSED");
    }

    try {
      writer.releaseLock();
    } catch {}

    try {
      await socket.close();
    } catch {}

    try {
      ws.close();
    } catch {}
  }
}

function parseVlessRequest(data) {
  console.log("PARSING VLESS REQUEST");

  if (data.length < 24) {
    throw new Error("VLESS header too short");
  }

  // Version
  if (data[0] !== 0x00) {
    throw new Error(
      "Unsupported VLESS version: " + data[0]
    );
  }

  // UUID
  const uuidBytes = hexToBytes(UUID.replaceAll("-", ""));

  for (let i = 0; i < 16; i++) {
    if (data[1 + i] !== uuidBytes[i]) {
      throw new Error("Invalid UUID");
    }
  }

  console.log("UUID OK");

  let offset = 17;

  // Addons length
  const addonsLength = data[offset++];

  console.log("ADDONS LENGTH:", addonsLength);

  if (data.length < offset + addonsLength + 4) {
    throw new Error("Invalid VLESS addon section");
  }

  // Skip addons
  offset += addonsLength;

  // Command
  const command = data[offset++];

  console.log("COMMAND:", command);

  // 1 = TCP
  if (command !== 1) {
    throw new Error(
      "Unsupported VLESS command: " + command
    );
  }

  // Port
  if (data.length < offset + 2) {
    throw new Error("Missing port");
  }

  const port =
    (data[offset] << 8) |
    data[offset + 1];

  offset += 2;

  console.log("PORT:", port);

  // Address type
  if (data.length < offset + 1) {
    throw new Error("Missing address type");
  }

  const addressType = data[offset++];

  console.log("ADDRESS TYPE:", addressType);

  let hostname;

  // IPv4
  if (addressType === 1) {
    if (data.length < offset + 4) {
      throw new Error("Invalid IPv4 address");
    }

    hostname = Array.from(
      data.slice(offset, offset + 4)
    ).join(".");

    offset += 4;
  }

  // Domain
  else if (addressType === 2) {
    if (data.length < offset + 1) {
      throw new Error("Missing domain length");
    }

    const length = data[offset++];

    if (data.length < offset + length) {
      throw new Error("Invalid domain");
    }

    hostname = new TextDecoder().decode(
      data.slice(offset, offset + length)
    );

    offset += length;
  }

  // IPv6
  else if (addressType === 3) {
    if (data.length < offset + 16) {
      throw new Error("Invalid IPv6 address");
    }

    const parts = [];

    for (let i = 0; i < 16; i += 2) {
      parts.push(
        (
          (data[offset + i] << 8) |
          data[offset + i + 1]
        ).toString(16)
      );
    }

    hostname = parts.join(":");

    offset += 16;
  }

  else {
    throw new Error(
      "Unsupported address type: " + addressType
    );
  }

  console.log("HOSTNAME:", hostname);

  return {
    hostname,
    port,
    payload: data.slice(offset),
  };
}

async function relayTcpToWebSocket(socket, ws) {
  console.log("TCP -> WEBSOCKET RELAY STARTED");

  const reader = socket.readable.getReader();

  try {
    while (true) {
      const { value, done } = await reader.read();

      if (done) {
        console.log("TCP STREAM ENDED");
        break;
      }

      if (value && value.length > 0) {
        console.log(
          "TCP -> WS:",
          value.length,
          "bytes"
        );

        if (ws.readyState === WebSocket.OPEN) {
          ws.send(value);
        } else {
          break;
        }
      }
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {}
  }
}

async function relayWebSocketToTcp(ws, writer) {
  console.log("WEBSOCKET -> TCP RELAY STARTED");

  return new Promise((resolve, reject) => {
    const onMessage = async (event) => {
      try {
        let data;

        if (event.data instanceof ArrayBuffer) {
          data = new Uint8Array(event.data);
        } else if (event.data instanceof Blob) {
          data = new Uint8Array(
            await event.data.arrayBuffer()
          );
        } else {
          console.log("IGNORED TEXT MESSAGE");
          return;
        }

        console.log(
          "WS -> TCP:",
          data.length,
          "bytes"
        );

        if (data.length > 0) {
          await writer.write(data);
        }
      } catch (error) {
        console.error(
          "WS -> TCP ERROR:",
          error?.stack || error
        );

        cleanup();
        reject(error);
      }
    };

    const onClose = () => {
      console.log("WEBSOCKET CLOSED");

      cleanup();
      resolve();
    };

    const onError = () => {
      console.error("WEBSOCKET ERROR");

      cleanup();
      reject(new Error("WebSocket error"));
    };

    function cleanup() {
      ws.removeEventListener("message", onMessage);
      ws.removeEventListener("close", onClose);
      ws.removeEventListener("error", onError);
    }

    ws.addEventListener("message", onMessage);
    ws.addEventListener("close", onClose);
    ws.addEventListener("error", onError);
  });
}

function hexToBytes(hex) {
  const result = new Uint8Array(hex.length / 2);

  for (let i = 0; i < result.length; i++) {
    result[i] = parseInt(
      hex.substr(i * 2, 2),
      16
    );
  }

  return result;
}
