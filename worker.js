import { connect } from "cloudflare:sockets";

const UUID = "2ed36262-3716-46dd-878d-442661ab1dbd";
const PATH = "/vless";

export default {
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade");

    // Normal browser request
    if (upgrade !== "websocket") {
      return new Response("VLESS Worker OK", {
        status: 200,
      });
    }

    const url = new URL(request.url);

    if (url.pathname !== PATH) {
      return new Response("Not Found", {
        status: 404,
      });
    }

    const pair = new WebSocketPair();

    const client = pair[0];
    const server = pair[1];

    // Support both ArrayBuffer and Blob.
    server.binaryType = "arraybuffer";

    server.accept({
      allowHalfOpen: true,
    });

    handleConnection(request, server).catch((error) => {
      console.error(
        "CONNECTION ERROR:",
        error?.stack || error
      );

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

async function handleConnection(request, ws) {
  console.log("VLESS CONNECTION START");

  /*
   * Some VLESS clients can put the first VLESS packet
   * inside Sec-WebSocket-Protocol as early data.
   */
  const earlyDataHeader =
    request.headers.get("sec-websocket-protocol") || "";

  const readable = makeWebSocketStream(
    ws,
    earlyDataHeader
  );

  const reader = readable.getReader();

  /*
   * Read enough data to contain the complete VLESS header.
   * We don't assume that the whole header arrives in one
   * WebSocket frame.
   */

  let buffer = new Uint8Array(0);

  let requestInfo = null;

  while (!requestInfo) {
    const { value, done } = await reader.read();

    if (done) {
      throw new Error(
        "WebSocket closed before VLESS request"
      );
    }

    const chunk = await toUint8Array(value);

    if (!chunk || chunk.length === 0) {
      continue;
    }

    console.log(
      "VLESS DATA RECEIVED:",
      chunk.length,
      "bytes"
    );

    buffer = concatUint8Arrays(buffer, chunk);

    if (buffer.length > 65536) {
      throw new Error(
        "VLESS header is too large"
      );
    }

    try {
      requestInfo = parseVlessHeader(buffer);
    } catch (error) {
      /*
       * If the data is simply incomplete, keep reading.
       * Otherwise propagate the real protocol error.
       */
      if (
        error?.message === "INCOMPLETE_VLESS_HEADER"
      ) {
        continue;
      }

      throw error;
    }
  }

  console.log(
    "VLESS TARGET:",
    requestInfo.hostname +
      ":" +
      requestInfo.port
  );

  console.log(
    "VLESS PAYLOAD:",
    requestInfo.payload.length,
    "bytes"
  );

  /*
   * Connect to the requested TCP destination.
   */
  console.log("CONNECTING TCP...");

  const socket = connect({
    hostname: requestInfo.hostname,
    port: requestInfo.port,
  });

  await socket.opened;

  console.log("TCP CONNECTED");

  const writer =
    socket.writable.getWriter();

  /*
   * VLESS response header.
   */
  ws.send(
    new Uint8Array([
      0x00,
      0x00,
    ])
  );

  console.log(
    "VLESS RESPONSE SENT"
  );

  /*
   * Send any data that was already included
   * after the VLESS header.
   */
  if (requestInfo.payload.length > 0) {
    await writer.write(
      requestInfo.payload
    );

    console.log(
      "INITIAL DATA SENT:",
      requestInfo.payload.length
    );
  }

  /*
   * Continue receiving data from WebSocket
   * and send it to TCP.
   */
  const wsToTcp = relayWebSocketToTcp(
    reader,
    writer
  );

  /*
   * Receive TCP data and send it to WebSocket.
   */
  const tcpToWs = relayTcpToWebSocket(
    socket,
    ws
  );

  await Promise.race([
    wsToTcp,
    tcpToWs,
  ]);

  try {
    writer.releaseLock();
  } catch {}

  try {
    await socket.close();
  } catch {}

  try {
    ws.close();
  } catch {}

  console.log(
    "VLESS CONNECTION CLOSED"
  );
}


/* =========================================================
   WebSocket -> ReadableStream
   ========================================================= */

function makeWebSocketStream(
  ws,
  earlyDataHeader
) {
  let cancelled = false;

  return new ReadableStream({
    start(controller) {

      ws.addEventListener(
        "message",
        (event) => {

          if (cancelled) {
            return;
          }

          controller.enqueue(
            event.data
          );
        }
      );

      ws.addEventListener(
        "close",
        () => {

          if (cancelled) {
            return;
          }

          cancelled = true;

          try {
            controller.close();
          } catch {}
        }
      );

      ws.addEventListener(
        "error",
        (error) => {

          if (cancelled) {
            return;
          }

          cancelled = true;

          try {
            controller.error(error);
          } catch {}
        }
      );

      /*
       * WebSocket Early Data
       */
      if (earlyDataHeader) {

        try {

          const decoded =
            base64UrlDecode(
              earlyDataHeader
            );

          if (decoded.length > 0) {
            controller.enqueue(decoded);
          }

        } catch (error) {

          console.error(
            "EARLY DATA ERROR:",
            error
          );
        }
      }
    },

    cancel() {
      cancelled = true;

      try {
        ws.close();
      } catch {}
    },
  });
}


/* =========================================================
   WebSocket -> TCP
   ========================================================= */

async function relayWebSocketToTcp(
  reader,
  writer
) {
  try {

    while (true) {

      const {
        value,
        done,
      } = await reader.read();

      if (done) {
        break;
      }

      const data =
        await toUint8Array(value);

      if (
        data &&
        data.length > 0
      ) {

        await writer.write(data);

        console.log(
          "WS -> TCP:",
          data.length,
          "bytes"
        );
      }
    }

  } catch (error) {

    console.error(
      "WS -> TCP ERROR:",
      error?.stack || error
    );

    throw error;
  }
}


/* =========================================================
   TCP -> WebSocket
   ========================================================= */

async function relayTcpToWebSocket(
  socket,
  ws
) {
  const reader =
    socket.readable.getReader();

  try {

    while (true) {

      const {
        value,
        done,
      } = await reader.read();

      if (done) {
        break;
      }

      if (
        value &&
        value.length > 0
      ) {

        if (
          ws.readyState !==
          WebSocket.OPEN
        ) {
          break;
        }

        ws.send(value);

        console.log(
          "TCP -> WS:",
          value.length,
          "bytes"
        );
      }
    }

  } finally {

    try {
      reader.releaseLock();
    } catch {}
  }
}


/* =========================================================
   VLESS HEADER
   ========================================================= */

function parseVlessHeader(
  data
) {
  /*
   * Minimum possible VLESS TCP header:
   *
   * 1  version
   * 16 UUID
   * 1  addons length
   * 1  command
   * 2  port
   * 1  address type
   * 4  IPv4
   *
   * = 26 bytes
   */

  if (data.length < 24) {
    throw new Error(
      "INCOMPLETE_VLESS_HEADER"
    );
  }

  /*
   * Version
   */

  if (data[0] !== 0x00) {
    throw new Error(
      "INVALID_VLESS_VERSION"
    );
  }

  /*
   * UUID
   */

  const uuidBytes =
    uuidToBytes(UUID);

  for (let i = 0; i < 16; i++) {

    if (
      data[1 + i] !==
      uuidBytes[i]
    ) {
      throw new Error(
        "INVALID_UUID"
      );
    }
  }

  console.log(
    "UUID VERIFIED"
  );

  let offset = 17;

  /*
   * Addons length
   */

  if (data.length < offset + 1) {
    throw new Error(
      "INCOMPLETE_VLESS_HEADER"
    );
  }

  const addonsLength =
    data[offset];

  offset += 1;

  /*
   * Skip addons.
   */

  if (
    data.length <
    offset + addonsLength + 1
  ) {
    throw new Error(
      "INCOMPLETE_VLESS_HEADER"
    );
  }

  offset += addonsLength;

  /*
   * Command
   *
   * 1 = TCP
   * 2 = UDP
   */

  const command =
    data[offset];

  offset += 1;

  if (command !== 1) {

    throw new Error(
      "ONLY_TCP_SUPPORTED"
    );
  }

  /*
   * Port
   */

  if (data.length < offset + 2) {
    throw new Error(
      "INCOMPLETE_VLESS_HEADER"
    );
  }

  const port =
    (data[offset] << 8) |
    data[offset + 1];

  offset += 2;

  /*
   * Address type
   *
   * 1 = IPv4
   * 2 = Domain
   * 3 = IPv6
   */

  if (data.length < offset + 1) {
    throw new Error(
      "INCOMPLETE_VLESS_HEADER"
    );
  }

  const addressType =
    data[offset];

  offset += 1;

  let hostname;

  /*
   * IPv4
   */

  if (addressType === 1) {

    if (data.length < offset + 4) {
      throw new Error(
        "INCOMPLETE_VLESS_HEADER"
      );
    }

    hostname =
      Array.from(
        data.slice(
          offset,
          offset + 4
        )
      ).join(".");

    offset += 4;
  }

  /*
   * Domain
   */

  else if (addressType === 2) {

    if (data.length < offset + 1) {
      throw new Error(
        "INCOMPLETE_VLESS_HEADER"
      );
    }

    const length =
      data[offset];

    offset += 1;

    if (
      data.length <
      offset + length
    ) {
      throw new Error(
        "INCOMPLETE_VLESS_HEADER"
      );
    }

    hostname =
      new TextDecoder().decode(
        data.slice(
          offset,
          offset + length
        )
      );

    offset += length;
  }

  /*
   * IPv6
   */

  else if (addressType === 3) {

    if (data.length < offset + 16) {
      throw new Error(
        "INCOMPLETE_VLESS_HEADER"
      );
    }

    const parts = [];

    for (
      let i = 0;
      i < 16;
      i += 2
    ) {

      const part =
        (data[offset + i] << 8) |
        data[offset + i + 1];

      parts.push(
        part.toString(16)
      );
    }

    hostname =
      parts.join(":");

    offset += 16;
  }

  else {

    throw new Error(
      "INVALID_ADDRESS_TYPE"
    );
  }

  if (!hostname) {
    throw new Error(
      "EMPTY_HOSTNAME"
    );
  }

  console.log(
    "HOST:",
    hostname
  );

  console.log(
    "PORT:",
    port
  );

  return {
    hostname,
    port,
    payload: data.slice(offset),
  };
}


/* =========================================================
   Helpers
   ========================================================= */

async function toUint8Array(
  value
) {
  if (!value) {
    return new Uint8Array(0);
  }

  if (
    value instanceof Uint8Array
  ) {
    return value;
  }

  if (
    value instanceof ArrayBuffer
  ) {
    return new Uint8Array(value);
  }

  if (
    value instanceof Blob
  ) {
    const buffer =
      await value.arrayBuffer();

    return new Uint8Array(buffer);
  }

  throw new Error(
    "UNKNOWN_WEBSOCKET_DATA_TYPE"
  );
}


function concatUint8Arrays(
  a,
  b
) {
  const result =
    new Uint8Array(
      a.length + b.length
    );

  result.set(a, 0);
  result.set(b, a.length);

  return result;
}


function uuidToBytes(
  uuid
) {
  const hex =
    uuid.replaceAll("-", "");

  const result =
    new Uint8Array(16);

  for (
    let i = 0;
    i < 16;
    i++
  ) {

    result[i] =
      parseInt(
        hex.substr(i * 2, 2),
        16
      );
  }

  return result;
}


function base64UrlDecode(
  input
) {
  let value =
    input
      .replaceAll("-", "+")
      .replaceAll("_", "/");

  while (
    value.length % 4 !== 0
  ) {
    value += "=";
  }

  const binary =
    atob(value);

  const result =
    new Uint8Array(
      binary.length
    );

  for (
    let i = 0;
    i < binary.length;
    i++
  ) {
    result[i] =
      binary.charCodeAt(i);
  }

  return result;
}
