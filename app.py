from flask import Flask, request, jsonify
import asyncio
import aiohttp
import os
from pymongo import MongoClient

# Assurez-vous que vos modules 'byte' et 'visit_count_pb2' sont bien présents.
from byte import Encrypt_ID, encrypt_api
from visit_count_pb2 import Info
from config import REGION_CONFIG, DB_NAME, MONGO_URI, MAX_USAGE  # make sure these exist

app = Flask(__name__)

# --- MongoDB client (fixed) ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]  # <-- use DB_NAME from config

def fetch_tokens(region):
    """Récupère les tokens d'une région spécifique depuis MongoDB."""
    config = REGION_CONFIG.get(region)
    if not config:
        app.logger.error(f"No REGION_CONFIG for {region}")
        return []
    coll_name = config.get("tokens")
    if not coll_name:
        app.logger.error(f"No token collection configured for region {region}")
        return []
    try:
        cursor = db[coll_name].find({}, {"_id": 0, "token": 1})
        tokens = [t.get("token") for t in cursor if t.get("token")]
        app.logger.info(f"Fetched {len(tokens)} tokens from {coll_name}")
        return tokens
    except Exception as e:
        app.logger.error(f"Mongo error reading {coll_name}: {e}")
        return []

def parse_protobuf_response(response_data):
    """Analyse la réponse protobuf et retourne les informations du joueur."""
    try:
        info = Info()
        info.ParseFromString(response_data)
        return {
            "uid": getattr(info.AccountInfo, "UID", 0) or 0,
            "nickname": getattr(info.AccountInfo, "PlayerNickname", "") or "",
            "likes": getattr(info.AccountInfo, "Likes", 0) or 0,
            "region": getattr(info.AccountInfo, "PlayerRegion", "") or "",
            "level": getattr(info.AccountInfo, "Levels", 0) or 0
        }
    except Exception as e:
        app.logger.error(f"❌ Erreur de parsing Protobuf: {e}")
        return None

async def get_header(token):
    """Retourne les en-têtes de la requête."""
    return {
        "Expect": "100-continue",
        "Authorization": f"Bearer {token}",
        "X-Unity-Version": "2018.4.11f1",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB51",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-N975F Build/PI)",
        "Connection": "close",
        "Accept-Encoding": "gzip, deflate, br"
    }

async def send_friend_request(session, uid, url, token):
    """Envoie une requête d'ajout d'ami en utilisant aiohttp."""
    try:
        encrypted_id = Encrypt_ID(uid)
        payload = f"08a7c4839f1e10{encrypted_id}1801"
        encrypted_payload = encrypt_api(payload)

        headers = await get_header(token)
        data = bytes.fromhex(encrypted_payload)

        async with session.post(url, headers=headers, data=data, timeout=10) as response:
            return response.status == 200

    except Exception as e:
        app.logger.error(f"❌ Exception avec le token...{(token or '')[-5:]}: {e}")
        return False

# --- Visits (async) ---
async def visit(session, url, token, data):
    headers = await get_header(token)
    try:
        async with session.post(url, headers=headers, data=data, ssl=False) as resp:
            if resp.status == 200:
                return True, await resp.read()
    except Exception as e:
        app.logger.error(f"Exception pendant la visite: {e}")
    return False, None

async def send_until_1000_success(tokens, uid, server_name, target_success=500):
    url = REGION_CONFIG[server_name]["url_visit"]
    connector = aiohttp.TCPConnector(limit=0)
    total_success = 0
    total_sent = 0
    player_info = None
    first_success_response = None

    async with aiohttp.ClientSession(connector=connector) as session:
        encrypted = encrypt_api("08" + Encrypt_ID(str(uid)) + "1801")
        data = bytes.fromhex(encrypted)

        # rotate tokens in round-robin
        token_count = len(tokens)
        if token_count == 0:
            return 0, 0, None

        while total_success < target_success:
            batch_size = min(target_success - total_success, 100)  # smaller batch to avoid huge concurrency
            tasks = [
                asyncio.create_task(visit(session, url, tokens[(total_sent + i) % token_count], data))
                for i in range(batch_size)
            ]
            results = await asyncio.gather(*tasks)

            # capture player info from the first successful response
            if first_success_response is None:
                for success, response in results:
                    if success and response is not None:
                        first_success_response = response
                        player_info = parse_protobuf_response(response)
                        break

            batch_success = sum(1 for r, _ in results if r)
            total_success += batch_success
            total_sent += len(results)

            # avoid infinite tight loop (small sleep)
            await asyncio.sleep(0.01)

    return total_success, total_sent, player_info

# --- Flask routes ---
@app.route("/send_requests", methods=["GET"])
async def send_requests():
    uid = request.args.get("uid")
    region = request.args.get("region")

    if not uid or not region:
        return jsonify({"error": "Missing 'uid' or 'region' in query"}), 400

    region = region.upper()
    if region not in REGION_CONFIG:
        return jsonify({"error": f"Region '{region}' not supported."}), 400
    config = REGION_CONFIG.get(region)
    if not config:
        return jsonify({"error": f"Invalid region '{region}'"}), 400

    tokens = fetch_tokens(region)
    if not tokens:
        return jsonify({"error": f"No tokens available for region '{region}'"}), 500

    url_visit = config["url_visit"]
    player_info = None

    # safe guard for MAX_USAGE
    max_try = min(MAX_USAGE, len(tokens))
    try:
        async with aiohttp.ClientSession() as session:
            encrypted_info_data = bytes.fromhex(encrypt_api("08" + Encrypt_ID(str(uid)) + "1801"))

            for i in range(max_try):
                try:
                    headers_info = await get_header(tokens[i])
                    async with session.post(url_visit, headers=headers_info, data=encrypted_info_data, timeout=10) as response:
                        if response.status == 200:
                            player_info = parse_protobuf_response(await response.read())
                            break
                except Exception as inner_e:
                    app.logger.warning(f"Erreur avec le token index {i}: {inner_e}")

    except Exception as e:
        app.logger.error(f"Erreur générale lors de la récupération des infos du joueur: {e}")

    if not player_info:
        return jsonify({"error": "Could not retrieve player information."}), 500

    # send friend requests (safe slice)
    tasks = []
    slice_count = min(110, len(tokens))
    async with aiohttp.ClientSession() as session:
        for token in tokens[:slice_count]:
            tasks.append(send_friend_request(session, uid, config["url_spam"], token))

        results = await asyncio.gather(*tasks)

    success_count = sum(1 for r in results if r)
    failed_count = len(results) - success_count

    return jsonify({
        "fail": failed_count,
        "success": success_count,
        **player_info
    })

@app.route("/visits", methods=["GET"])
def send_visits():
    uid = request.args.get("uid")
    region = request.args.get("region")

    if not uid or not region:
        return jsonify({"error": "Missing 'uid' or 'region' in query"}), 400

    region = region.upper()
    if region in {"BR", "US", "SAC", "NA", "NX"}:
        region = "BR"
    elif region == "IND":
        region = "IND"
    else:
        region = "BD"

    tokens = fetch_tokens(region)
    if not tokens:
        return jsonify({"error": "No valid tokens found"}), 500

    total_success, total_sent, player_info = asyncio.run(send_until_1000_success(
        tokens, uid, region
    ))

    if player_info:
        return jsonify({
            "success": total_success,
            "fail": total_sent - total_success,
            **player_info
        })
    return jsonify({"error": "Could not decode player information"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
