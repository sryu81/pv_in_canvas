"""
ca2web_bridge.py  —  Bidirectional CA/PVA <-> WebSocket bridge.

WebSocket message protocol
--------------------------
Client -> bridge:
  {"type": "subscribe",   "pv": "NAME", "protocol": "ca"|"pva"}
  {"type": "unsubscribe", "pv": "NAME"}
  {"type": "put",         "pv": "NAME", "value": <val>}
  {"type": "get",         "pv": "NAME", "protocol": "ca"|"pva"}

Bridge -> client:
  {"type": "monitor",      "pv": "NAME", "value": <val>, "protocol": "ca"|"pva"}
  {"type": "get_response", "pv": "NAME", "value": <val>}
  {"type": "put_ack",      "pv": "NAME", "status": "ok"|"error", "message": "..."}
  {"type": "error",        "pv": "NAME", "message": "..."}
"""

import asyncio
import json
import logging
import websockets
from caproto.asyncio.client import Context as CAContext

try:
    from p4p.client.asyncio import Context as PVAContext
    _pva_available = True
except ImportError:
    _pva_available = False
    logging.warning("p4p not installed — PVA support disabled. pip install p4p")

logging.basicConfig(level=logging.INFO)

HOST = "localhost"
PORT = 8765

# Plain string -> CA. Dict with protocol key -> as specified.
MONITORED_PVS = [
    "ROOM:TEMP",
    "ROOM:HUMIDITY",
    "VALVE:STATE",
    # {"pv": "SOME:PVA:PV", "protocol": "pva"},
]

_ca_ctx: CAContext | None = None
_pva_ctx = None  # PVAContext | None

# pv_name -> asyncio.Task
_monitor_tasks: dict[str, asyncio.Task] = {}

# pv_name -> set of websockets subscribed to that PV
_pv_subscribers: dict[str, set] = {}

# pv_name -> "ca" | "pva"
_pv_protocols: dict[str, str] = {}


def _parse_pv_spec(spec) -> tuple[str, str]:
    if isinstance(spec, str):
        return spec, "ca"
    return spec["pv"], spec.get("protocol", "ca")


def _extract_ca_value(response):
    data = response.data
    return data[0].item() if len(data) == 1 else data.tolist()


def _extract_pva_value(value):
    try:
        v = value.value
        return v.tolist() if hasattr(v, "tolist") else v
    except AttributeError:
        return str(value)


async def _broadcast(pv_name: str, payload: dict):
    targets = _pv_subscribers.get(pv_name, set()).copy()
    if targets:
        msg = json.dumps(payload)
        await asyncio.gather(*(c.send(msg) for c in targets), return_exceptions=True)


# ── CA ───────────────────────────────────────────────────────────────────────

async def _ca_monitor(pv_name: str):
    global _ca_ctx
    logging.info(f"[CA] monitor start: {pv_name}")
    try:
        (pv,) = await _ca_ctx.get_pvs(pv_name)
        async for response in pv.subscribe():
            await _broadcast(pv_name, {
                "type": "monitor",
                "pv": pv_name,
                "value": _extract_ca_value(response),
                "protocol": "ca",
            })
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"[CA] monitor error {pv_name}: {e}")


async def _ca_put(pv_name: str, value):
    (pv,) = await _ca_ctx.get_pvs(pv_name)
    await pv.write([value] if not isinstance(value, (list, tuple)) else value, notify=True)


async def _ca_get(pv_name: str):
    (pv,) = await _ca_ctx.get_pvs(pv_name)
    return _extract_ca_value(await pv.read())


# ── PVA ──────────────────────────────────────────────────────────────────────

async def _pva_monitor(pv_name: str):
    global _pva_ctx
    if not _pva_available:
        logging.error(f"[PVA] p4p unavailable, cannot monitor {pv_name}")
        return
    loop = asyncio.get_running_loop()
    q: asyncio.Queue = asyncio.Queue()

    def _on_value(val):
        loop.call_soon_threadsafe(q.put_nowait, ("update", val))

    def _on_error(exc):
        loop.call_soon_threadsafe(q.put_nowait, ("error", exc))

    logging.info(f"[PVA] monitor start: {pv_name}")
    sub = _pva_ctx.monitor(pv_name, _on_value, notify_disconnect=True)
    try:
        while True:
            kind, val = await q.get()
            if kind == "error":
                logging.warning(f"[PVA] disconnect {pv_name}: {val}")
                await asyncio.sleep(5)
                break
            await _broadcast(pv_name, {
                "type": "monitor",
                "pv": pv_name,
                "value": _extract_pva_value(val),
                "protocol": "pva",
            })
    except asyncio.CancelledError:
        pass
    finally:
        sub.close()


async def _pva_put(pv_name: str, value):
    if not _pva_available:
        raise RuntimeError("p4p not installed")
    await _pva_ctx.put(pv_name, value)


async def _pva_get(pv_name: str):
    if not _pva_available:
        raise RuntimeError("p4p not installed")
    return _extract_pva_value(await _pva_ctx.get(pv_name))


# ── Subscription management ───────────────────────────────────────────────────

def _ensure_monitor(pv_name: str, protocol: str):
    _pv_protocols[pv_name] = protocol
    task = _monitor_tasks.get(pv_name)
    if task is None or task.done():
        coro = _ca_monitor(pv_name) if protocol == "ca" else _pva_monitor(pv_name)
        _monitor_tasks[pv_name] = asyncio.ensure_future(coro)


def _subscribe(ws, pv_name: str, protocol: str):
    _pv_subscribers.setdefault(pv_name, set()).add(ws)
    _ensure_monitor(pv_name, protocol)


def _unsubscribe(ws, pv_name: str):
    _pv_subscribers.get(pv_name, set()).discard(ws)


def _unsubscribe_all(ws):
    for subs in _pv_subscribers.values():
        subs.discard(ws)


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def handler(websocket):
    # Auto-subscribe to all configured PVs on connect (backward compat).
    for pv_name, protocol in list(_pv_protocols.items()):
        _pv_subscribers.setdefault(pv_name, set()).add(websocket)

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "message": "invalid JSON"}))
                continue

            mtype = msg.get("type")
            pv_name = msg.get("pv", "")

            if mtype == "subscribe":
                protocol = msg.get("protocol", _pv_protocols.get(pv_name, "ca"))
                _subscribe(websocket, pv_name, protocol)

            elif mtype == "unsubscribe":
                _unsubscribe(websocket, pv_name)

            elif mtype == "put":
                protocol = _pv_protocols.get(pv_name, "ca")
                try:
                    if protocol == "pva":
                        await _pva_put(pv_name, msg["value"])
                    else:
                        await _ca_put(pv_name, msg["value"])
                    await websocket.send(json.dumps({"type": "put_ack", "pv": pv_name, "status": "ok"}))
                except Exception as e:
                    await websocket.send(json.dumps({"type": "put_ack", "pv": pv_name, "status": "error", "message": str(e)}))

            elif mtype == "get":
                protocol = msg.get("protocol", _pv_protocols.get(pv_name, "ca"))
                try:
                    value = await (_pva_get(pv_name) if protocol == "pva" else _ca_get(pv_name))
                    await websocket.send(json.dumps({"type": "get_response", "pv": pv_name, "value": value}))
                except Exception as e:
                    await websocket.send(json.dumps({"type": "error", "pv": pv_name, "message": str(e)}))

            else:
                await websocket.send(json.dumps({"type": "error", "message": f"unknown type: {mtype}"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _unsubscribe_all(websocket)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    global _ca_ctx, _pva_ctx

    _ca_ctx = CAContext()
    if _pva_available:
        _pva_ctx = PVAContext("pva")

    for spec in MONITORED_PVS:
        pv_name, protocol = _parse_pv_spec(spec)
        _pv_subscribers.setdefault(pv_name, set())
        _ensure_monitor(pv_name, protocol)

    server = await websockets.serve(handler, HOST, PORT)
    logging.info(f"Bridge running on ws://{HOST}:{PORT}")
    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bridge shut down.")
