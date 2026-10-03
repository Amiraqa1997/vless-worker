import { connect } from "cloudflare:sockets";

const UUID = "2ed36262-3716-46dd-878d-442661ab1dbd";
const PATH = "/vless";

export default {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("VLESS Worker", { status: 200 });
    }

    const url = new URL(request.url);

    if (url.pathname !== PATH) {
      return new Response("Not Found", { status: 404 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept({ allowHalfOpen: true });

    handleWebSocket(server).catch((error) => {
      console.error("VLESS error:", error);

      try {
        server.close(1011, "VLESS error");
      } catch {}
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleWebSocket(ws) {
  const firstMessage = await new Promise((resolve, reject) => {
    ws.addEventListener(
      "message",
      (event) => resolve(event.data),
      { once: true }
    );

    ws.addEventListener(
      "close",
      () => reject(new Error("WebSocket closed")),
      { once: true }
    );

    ws.addEventListener(
      "error",
      () => reject(new Error("WebSocket error")),
      { once: true }
    );
  });

  let data;

  if (firstMessage instanceof ArrayBuffer) {
    data = new Uint8Array(firstMessage);
  } else if (firstMessage instanceof Blob) {
    data = new Uint8Array(await firstMessage.arrayBuffer());
  } else {
    throw new Error("Binary VLESS message required");
  }

  if (data.length < 24) {
    throw new Error("Invalid VLESS header");
  }

  if (data[0] !== 0) {
    throw new Error("Unsupported VLESS version");
  }

  const uuidBytes = hexToBytes(UUID.replaceAll("-", ""));

  for (let i = 0; i < 16; i++) {
    if (data[1 + i] !== uuidBytes[i]) {
      throw new Error("Invalid UUID");
    }
  }

  const addonsLength = data[17];
  let offset = 18 + addonsLength;

  if (data.length <= offset) {
    throw new Error("Invalid request");
  }

  const command = data[offset++];

  if (command !== 1) {
    throw new Error("Only TCP is supported");
  }

  if (data.length < offset + 2) {
    throw new Error("Invalid port");
  }

  const port = (data[offset] << 8) | data[offset + 1];
  offset += 2;

  const addressType = data[offset++];

  let hostname;

  if (addressType === 1) {
    if (data.length < offset + 4) {
      throw new Error("Invalid IPv4");
    }

    hostname = Array.from(
      data.slice(offset, offset + 4)
    ).join(".");

    offset += 4;

  } else if (addressType === 2) {
    if (data.length <= offset) {
      throw new Error("Invalid domain length");
    }

    const length = data[offset++];

    if (data.length < offset + length) {
      throw new Error("Invalid domain");
    }

    hostname = new TextDecoder().decode(
      data.slice(offset, offset + length)
    );

    offset += length;

  } else if (addressType === 3) {
    if (data.length < offset + 16) {
      throw new Error("Invalid IPv6");
    }

    const parts = [];

    for (let i = 0; i < 16; i += 2) {
      parts.push(
        ((data[offset + i] << 8) |
          data[offset + i + 1]).toString(16)
      );
    }

    hostname = parts.join(":");
    offset += 16;

  } else {
    throw new Error("Unsupported address type");
  }

  console.log(`Connecting to ${hostname}:${port}`);

  let socket;

  try {
    socket = connect({
      hostname,
      port,
    });

    await socket.opened;

    console.log(`Connected to ${hostname}:${port}`);
  } catch (error) {
    console.error(
      `TCP connection failed ${hostname}:${port}`,
      error
    );

    throw error;
  }

  // VLESS response
  ws.send(new Uint8Array([0, 0]));

  // Send any payload that arrived with the VLESS header.
  const initialPayload = data.slice(offset);

  if (initialPayload.length > 0) {
    const writer = socket.writable.getWriter();

    try {
      await writer.write(initialPayload);
    } finally {
      writer.releaseLock();
    }
  }

  await Promise.all([
    relaySocketToWebSocket(socket, ws),
    relayWebSocketToSocket(socket, ws),
  ]);
}

async function relaySocketToWebSocket(socket, ws) {
  try {
    const reader = socket.readable.getReader();

    try {
      while (true) {
        const { value, done } = await reader.read();

        if (done) break;

        if (ws.readyState === WebSocket.OPEN) {
          ws.send(value);
        } else {
          break;
        }
      }
    } finally {
      reader.releaseLock();
    }
  } catch (error) {
    console.error("Socket → WebSocket error:", error);

    try {
      ws.close();
    } catch {}
  }
}

async function relayWebSocketToSocket(socket, ws) {
  ws.addEventListener("message", async (event) => {
    try {
      let data;

      if (event.data instanceof ArrayBuffer) {
        data = new Uint8Array(event.data);
      } else if (event.data instanceof Blob) {
        data = new Uint8Array(
          await event.data.arrayBuffer()
        );
      } else {
        return;
      }

      const writer = socket.writable.getWriter();

      try {
        await writer.write(data);
      } finally {
        writer.releaseLock();
      }

    } catch (error) {
      console.error("WebSocket → Socket error:", error);

      try {
        ws.close();
      } catch {}
    }
  });

  ws.addEventListener("close", () => {
    try {
      socket.close();
    } catch {}
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
