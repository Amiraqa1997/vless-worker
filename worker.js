import { connect } from "cloudflare:sockets";

const PATH = "/vless";

export default {
  async fetch(request, env) {
    const upgrade = request.headers.get("Upgrade");

    if (upgrade !== "websocket") {
      return new Response("VLESS Worker OK");
    }

    const url = new URL(request.url);

    if (url.pathname !== PATH) {
      return new Response("Not Found", { status: 404 });
    }

    const uuid = env.UUID;
    const proxyIP = env.PROXYIP;

    if (!uuid) {
      return new Response("UUID is missing", { status: 500 });
    }

    if (!proxyIP) {
      return new Response("PROXYIP is missing", { status: 500 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();

    handleVless(
      request,
      server,
      uuid,
      proxyIP
    ).catch((err) => {
      console.error(
        "VLESS ERROR:",
        err?.stack || err
      );

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


async function handleVless(
  request,
  ws,
  uuid,
  proxyIP
) {
  const earlyData =
    request.headers.get(
      "Sec-WebSocket-Protocol"
    ) || "";

  const stream =
    createWebSocketStream(
      ws,
      earlyData
    );

  const reader =
    stream.getReader();

  let buffer = new Uint8Array(0);

  let parsed = null;

  while (!parsed) {
    const {
      value,
      done,
    } = await reader.read();

    if (done) {
      throw new Error(
        "WebSocket closed"
      );
    }

    const data =
      await toBytes(value);

    buffer =
      concat(buffer, data);

    try {
      parsed =
        parseVless(
          buffer,
          uuid
        );
    } catch (err) {

      if (
        err.message ===
        "INCOMPLETE"
      ) {
        continue;
      }

      throw err;
    }
  }

  console.log(
    "TARGET:",
    parsed.host,
    parsed.port
  );

  /*
   * First try the real destination.
   */
  let socket;

  try {
    socket = connect({
      hostname: parsed.host,
      port: parsed.port,
    });

    await socket.opened;

    console.log(
      "DIRECT TCP CONNECTED"
    );

  } catch (err) {

    console.log(
      "DIRECT CONNECTION FAILED"
    );

    socket = null;
  }

  /*
   * If direct connection failed,
   * use ProxyIP.
   */
  if (!socket) {

    console.log(
      "USING PROXYIP:",
      proxyIP
    );

    socket = connect({
      hostname: proxyIP,
      port: parsed.port,
    });

    await socket.opened;

    console.log(
      "PROXYIP TCP CONNECTED"
    );
  }

  const writer =
    socket.writable.getWriter();

  /*
   * VLESS response.
   */
  ws.send(
    new Uint8Array([
      0x00,
      0x00,
    ])
  );

  /*
   * Send the first payload.
   */
  if (
    parsed.payload.length > 0
  ) {
    await writer.write(
      parsed.payload
    );
  }

  /*
   * Continue WS -> TCP.
   */
  const wsToTcp =
    relayWsToTcp(
      reader,
      writer
    );

  /*
   * TCP -> WS.
   */
  const tcpToWs =
    relayTcpToWs(
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
}


/* =========================
   WebSocket Stream
========================= */

function createWebSocketStream(
  ws,
  earlyData
) {
  return new ReadableStream({

    start(controller) {

      ws.addEventListener(
        "message",
        (event) => {

          controller.enqueue(
            event.data
          );
        }
      );

      ws.addEventListener(
        "close",
        () => {
          try {
            controller.close();
          } catch {}
        }
      );

      ws.addEventListener(
        "error",
        (err) => {
          try {
            controller.error(err);
          } catch {}
        }
      );

      if (earlyData) {

        try {

          const data =
            decodeBase64(
              earlyData
            );

          if (data.length) {
            controller.enqueue(
              data
            );
          }

        } catch {}
      }
    },

    cancel() {
      try {
        ws.close();
      } catch {}
    },
  });
}


/* =========================
   WS -> TCP
========================= */

async function relayWsToTcp(
  reader,
  writer
) {
  while (true) {

    const {
      value,
      done,
    } = await reader.read();

    if (done) {
      break;
    }

    const data =
      await toBytes(value);

    if (data.length) {
      await writer.write(data);
    }
  }
}


/* =========================
   TCP -> WS
========================= */

async function relayTcpToWs(
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
        value.length &&
        ws.readyState ===
        WebSocket.OPEN
      ) {
        ws.send(value);
      }
    }

  } finally {

    try {
      reader.releaseLock();
    } catch {}
  }
}


/* =========================
   VLESS Parser
========================= */

function parseVless(
  data,
  uuid
) {
  if (data.length < 24) {
    throw new Error("INCOMPLETE");
  }

  if (data[0] !== 0x00) {
    throw new Error(
      "INVALID VERSION"
    );
  }

  const uuidBytes =
    uuidToBytes(uuid);

  for (let i = 0; i < 16; i++) {

    if (
      data[i + 1] !==
      uuidBytes[i]
    ) {
      throw new Error(
        "INVALID UUID"
      );
    }
  }

  let offset = 17;

  const addonLength =
    data[offset++];

  if (
    data.length <
    offset +
      addonLength +
      1
  ) {
    throw new Error("INCOMPLETE");
  }

  offset += addonLength;

  const command =
    data[offset++];

  if (command !== 1) {
    throw new Error(
      "ONLY TCP SUPPORTED"
    );
  }

  if (
    data.length <
    offset + 3
  ) {
    throw new Error("INCOMPLETE");
  }

  const port =
    (data[offset] << 8) |
    data[offset + 1];

  offset += 2;

  const addressType =
    data[offset++];

  let host;

  /*
   * IPv4
   */
  if (addressType === 1) {

    if (
      data.length <
      offset + 4
    ) {
      throw new Error(
        "INCOMPLETE"
      );
    }

    host =
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

    if (
      data.length <
      offset + 1
    ) {
      throw new Error(
        "INCOMPLETE"
      );
    }

    const length =
      data[offset++];

    if (
      data.length <
      offset + length
    ) {
      throw new Error(
        "INCOMPLETE"
      );
    }

    host =
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

    if (
      data.length <
      offset + 16
    ) {
      throw new Error(
        "INCOMPLETE"
      );
    }

    const parts = [];

    for (
      let i = 0;
      i < 16;
      i += 2
    ) {

      parts.push(
        (
          (data[offset + i] << 8) |
          data[offset + i + 1]
        ).toString(16)
      );
    }

    host =
      parts.join(":");

    offset += 16;
  }

  else {
    throw new Error(
      "INVALID ADDRESS TYPE"
    );
  }

  return {
    host,
    port,
    payload:
      data.slice(offset),
  };
}


/* =========================
   Helpers
========================= */

async function toBytes(
  data
) {
  if (
    data instanceof Uint8Array
  ) {
    return data;
  }

  if (
    data instanceof ArrayBuffer
  ) {
    return new Uint8Array(data);
  }

  if (
    data instanceof Blob
  ) {
    return new Uint8Array(
      await data.arrayBuffer()
    );
  }

  return new Uint8Array(0);
}


function concat(a, b) {
  const result =
    new Uint8Array(
      a.length + b.length
    );

  result.set(a);
  result.set(b, a.length);

  return result;
}


function uuidToBytes(uuid) {
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
        hex.slice(
          i * 2,
          i * 2 + 2
        ),
        16
      );
  }

  return result;
}


function decodeBase64(input) {
  let value =
    input
      .replaceAll("-", "+")
      .replaceAll("_", "/");

  while (
    value.length % 4
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
