import asyncio
import json
import os
import random
import string
import websockets

PORT = int(os.environ.get("PORT", 8765))
MAX_PLAYERS = 6
ROOMS = {}


def generate_room_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
        if code not in ROOMS:
            return code


def room_roster(room):
    """List of {id, name} for connected players."""
    out = []
    for pid, meta in sorted(room.get("meta", {}).items()):
        if pid in room["players"]:
            out.append({"id": pid, "name": meta.get("name") or f"PILOT {pid}"})
    return out


async def broadcast(room, payload, exclude=None):
    dead = []
    raw = json.dumps(payload)
    for client in list(room["clients"]):
        if client is exclude:
            continue
        try:
            await client.send(raw)
        except Exception:
            dead.append(client)
    for c in dead:
        if c in room["clients"]:
            room["clients"].remove(c)


async def send_lobby_state(room):
    await broadcast(room, {
        "action": "lobby_state",
        "room_code": room.get("code"),
        "roster": room_roster(room),
        "match_started": bool(room.get("match_started")),
        "allow_late_join": bool(room.get("allow_late_join")),
        "host_id": 1 if 1 in room["players"] else (min(room["players"].keys()) if room["players"] else None),
    })


async def handle_client(websocket):
    current_room = None
    player_id = None

    try:
        async for message in websocket:
            msg = json.loads(message)
            action = msg.get("action")

            if action == "create_room":
                is_public = bool(msg.get("public", True))
                password = msg.get("password")
                allow_late = bool(msg.get("allow_late_join", False))
                room_code = generate_room_code()
                name = str(msg.get("name") or "HOST")[:24]

                ROOMS[room_code] = {
                    "code": room_code,
                    "password": password,
                    "is_public": is_public,
                    "clients": [websocket],
                    "players": {1: websocket},
                    "meta": {1: {"name": name}},
                    "match_started": False,
                    "allow_late_join": allow_late,
                }
                current_room = room_code
                player_id = 1
                await websocket.send(json.dumps({
                    "status": "ok",
                    "room_code": room_code,
                    "player_id": 1,
                    "match_started": False,
                    "allow_late_join": allow_late,
                    "host_id": 1,
                }))
                await send_lobby_state(ROOMS[room_code])

            elif action == "list_public":
                available = [
                    code for code, info in ROOMS.items()
                    if info["is_public"]
                    and len(info["clients"]) < MAX_PLAYERS
                    and (not info.get("match_started") or info.get("allow_late_join"))
                ]
                await websocket.send(json.dumps({"status": "ok", "rooms": available}))

            elif action == "join_room":
                room_code = str(msg.get("room_code", "")).upper()
                password = msg.get("password")
                name = str(msg.get("name") or "PILOT")[:24]

                if room_code not in ROOMS:
                    await websocket.send(json.dumps({"status": "error", "msg": "Room not found."}))
                    continue

                room = ROOMS[room_code]
                if len(room["clients"]) >= MAX_PLAYERS:
                    await websocket.send(json.dumps({"status": "error", "msg": "Room is full (Max 6)."}))
                    continue

                if room["password"] is not None and room["password"] != password:
                    await websocket.send(json.dumps({"status": "error", "msg": "Invalid password."}))
                    continue

                if room.get("match_started") and not room.get("allow_late_join"):
                    await websocket.send(json.dumps({
                        "status": "error",
                        "msg": "Match already started — late join is off."
                    }))
                    continue

                assigned_id = next(i for i in range(1, MAX_PLAYERS + 1) if i not in room["players"])
                room["clients"].append(websocket)
                room["players"][assigned_id] = websocket
                room.setdefault("meta", {})[assigned_id] = {"name": name}
                current_room = room_code
                player_id = assigned_id

                await websocket.send(json.dumps({
                    "status": "ok",
                    "room_code": room_code,
                    "player_id": assigned_id,
                    "match_started": bool(room.get("match_started")),
                    "allow_late_join": bool(room.get("allow_late_join")),
                    "host_id": 1 if 1 in room["players"] else min(room["players"].keys()),
                }))
                await broadcast(room, {"action": "player_joined", "player_id": assigned_id, "name": name}, exclude=websocket)
                await send_lobby_state(room)

                # Late join into live match
                if room.get("match_started") and room.get("allow_late_join"):
                    await websocket.send(json.dumps({"action": "match_start", "late": True}))

            elif action == "set_name":
                if current_room and current_room in ROOMS and player_id is not None:
                    room = ROOMS[current_room]
                    name = str(msg.get("name") or f"PILOT {player_id}")[:24]
                    room.setdefault("meta", {}).setdefault(player_id, {})["name"] = name
                    await send_lobby_state(room)

            elif action == "set_allow_late_join":
                if current_room and current_room in ROOMS:
                    room = ROOMS[current_room]
                    host = min(room["players"].keys()) if room["players"] else None
                    if player_id == host:
                        room["allow_late_join"] = bool(msg.get("allow_late_join"))
                        await send_lobby_state(room)

            elif action == "start_match":
                if current_room and current_room in ROOMS:
                    room = ROOMS[current_room]
                    host = min(room["players"].keys()) if room["players"] else None
                    if player_id != host:
                        continue
                    if room.get("match_started"):
                        continue
                    room["match_started"] = True
                    await broadcast(room, {
                        "action": "match_start",
                        "allow_late_join": bool(room.get("allow_late_join")),
                    })
                    await send_lobby_state(room)

            elif action == "kick_player":
                if current_room and current_room in ROOMS:
                    room = ROOMS[current_room]
                    host = min(room["players"].keys()) if room["players"] else None
                    if player_id != host:
                        continue
                    if room.get("match_started"):
                        await websocket.send(json.dumps({"status": "error", "msg": "Cannot kick after start."}))
                        continue
                    kid = int(msg.get("player_id") or 0)
                    if kid == host or kid not in room["players"]:
                        continue
                    target = room["players"].get(kid)
                    if target:
                        try:
                            await target.send(json.dumps({
                                "status": "error",
                                "action": "kicked",
                                "msg": "Kicked by host."
                            }))
                        except Exception:
                            pass
                        try:
                            await target.close()
                        except Exception:
                            pass
                        # cleanup happens on connection closed

            elif action == "game_packet":
                if current_room and current_room in ROOMS:
                    room = ROOMS[current_room]
                    # Only relay combat once match has started
                    if not room.get("match_started"):
                        continue
                    for client in room["clients"]:
                        if client != websocket:
                            try:
                                await client.send(json.dumps(msg))
                            except Exception:
                                pass

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if current_room and current_room in ROOMS:
            room = ROOMS[current_room]
            if websocket in room["clients"]:
                room["clients"].remove(websocket)
            if player_id in room["players"]:
                del room["players"][player_id]
            if player_id in room.get("meta", {}):
                del room["meta"][player_id]
            await broadcast(room, {"action": "player_left", "player_id": player_id})
            if room["clients"]:
                await send_lobby_state(room)
            else:
                del ROOMS[current_room]


async def main():
    print(f"6-Player Server running on port {PORT}...")
    async with websockets.serve(handle_client, "0.0.0.0", PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
