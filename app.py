from flask import Flask, request, jsonify
import asyncio
import aiohttp
import os
from pymongo import MongoClient

# Assurez-vous que vos modules 'byte' et 'visit_count_pb2' sont bien présents.
from byte import Encrypt_ID, encrypt_api
from visit_count_pb2 import Info
from config import REGION_CONFIG,DB_NAME,MONGO_URI,MAX_USAGE

app = Flask(__name__)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def fetch_tokens(region):
    """Récupère les tokens d'une région spécifique depuis MongoDB."""
    config = REGION_CONFIG.get(region)
    if not config:
        return []
    tokens = [t["token"] for t in db[config["tokens"]].find({}, {"_id": 0, "token": 1})]
    return tokens

def parse_protobuf_response(response_data):
    """Analyse la réponse protobuf et retourne les informations du joueur."""
    try:
        info = Info()
        info.ParseFromString(response_data)
        return {
            "uid": info.AccountInfo.UID or 0,
            "nickname": info.AccountInfo.PlayerNickname or "",
            "likes": info.AccountInfo.Likes or 0,
            "region": info.AccountInfo.PlayerRegion or "",
            "level": info.AccountInfo.Levels or 0
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
        app.logger.error(f"❌ Exception avec le token...{token[-5:]}: {e}")
        return False

# --- Routes Flask avec asyncio ---
async def visit(session, url, token, uid, data):
    headers = {
        "ReleaseVersion": "OB51",
        "X-GA": "v1 1",
        "Authorization": f"Bearer {token}",
        "X-Unity-Version": "2018.4.11f1",
        "X-GA": "v1 1",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-N975F Build/PI)",
        "Connection": "close",
        "Accept-Encoding": "gzip, deflate, br"
    }
    try:
        async with session.post(url, headers=headers, data=data, ssl=False) as resp:
            if resp.status == 200:
                response_data = await resp.read()
                return True, response_data
            else:
                return False, None
    except Exception as e:
        app.logger.error(f"❌ Visit error: {e}")
        return False, None
async def send_until_1000_success(tokens, uid, server_name, target_success=500):
    url = REGION_CONFIG[server_name]["url_visit"]
    connector = aiohttp.TCPConnector(limit=100)  # limite à 100 connexions max
    total_success = 0
    total_sent = 0
    first_success_response = None
    player_info = None
    sem = asyncio.Semaphore(100)

    async def limited_visit(session, url, token, uid, data):
        async with sem:
            return await visit(session, url, token, uid, data)

    async with aiohttp.ClientSession(connector=connector) as session:
        encrypted = encrypt_api("08" + Encrypt_ID(str(uid)) + "1801")
        data = bytes.fromhex(encrypted)

        while total_success < target_success:
            batch_size = min(target_success - total_success, 200)  # 200 max par batch
            tasks = [
                asyncio.create_task(limited_visit(session, url, tokens[(total_sent + i) % len(tokens)], uid, data))
                for i in range(batch_size)
            ]
            results = await asyncio.gather(*tasks)

            if first_success_response is None:
                for success, response in results:
                    if success and response is not None:
                        first_success_response = response
                        player_info = parse_protobuf_response(response)
                        break

            batch_success = sum(1 for r, _ in results if r)
            total_success += batch_success
            total_sent += batch_size

            await asyncio.sleep(0.5)  # pause pour soulager le serveur

    return total_success, total_sent, player_info

 
@app.route("/", methods=["GET"])
def home():
    return "Spam and Visits api  running good!"
    
@app.route("/visits", methods=["GET"])
def send_visits():
    uid = request.args.get("uid")
    region = request.args.get("region")

    if not uid or not region:
        return jsonify({"error": "Missing 'uid' or 'region' in query"}), 400

    region = region.upper()
    if region == "IND":
        region = "IND"
    elif region in {"BR", "US", "SAC", "NA", "NX"}:
        region = "BR"
    else:
        region = "SG"

    tokens = fetch_tokens(region)
    if not tokens:
        return jsonify({"error": "No valid tokens found"}), 500

    try:
        # Utiliser asyncio pour exécuter les visites
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        total_success, total_sent, player_info = loop.run_until_complete(
            send_until_1000_success(tokens, uid, region)
        )
        loop.close()
    except RuntimeError:
        total_success, total_sent, player_info = asyncio.get_event_loop().run_until_complete(
            send_until_1000_success(tokens, uid, region)
        )

    if player_info:
        return jsonify({
            "success": total_success,
            "fail": total_sent - total_success,
            **player_info
        })

    return jsonify({"error": "Could not decode player information"}), 500


from pprint import pprint
@app.route("/send_requests", methods=["GET"])
async def send_requests():
    """Endpoint pour envoyer des requêtes d'amis, optimisé avec asyncio."""
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
    
    # Récupération des infos du joueur de manière asynchrone
    
    url_visit = REGION_CONFIG[region]["url_visit"]
    player_info = None

    try:
        async with aiohttp.ClientSession() as session:
            encrypted_info_data = bytes.fromhex(encrypt_api("08" + Encrypt_ID(str(uid)) + "1801"))

            # Essayer avec les 10 premiers tokens
            for i in range(1,MAX_USAGE):
                try:
                    headers_info = await get_header(tokens[i])
                    async with session.post(url_visit, headers=headers_info, data=encrypted_info_data, timeout=10) as response:
                        if response.status == 200:
                            player_info = parse_protobuf_response(await response.read())
                            break  # On sort de la boucle si ça marche

                except Exception as inner_e:
                    app.logger.warning(f"Erreur avec le token {i}: {inner_e}")
                    
    except Exception as e:
        app.logger.error(f"Erreur générale lors de la récupération des infos du joueur: {e}")

    if not player_info:
        return jsonify({"error": "Could not retrieve player information."}), 500


    # Envoi des requêtes d'amis en parallèle avec asyncio.gather
    tasks = []
    async with aiohttp.ClientSession() as session:
        for token in tokens[:110]:
            tasks.append(send_friend_request(session, uid, config["url_spam"], token))
        
        results = await asyncio.gather(*tasks)
        
    success_count = sum(results)
    failed_count = len(results) - success_count
    
    return jsonify({
        "fail": failed_count,
        "success": success_count,
        **player_info
    })

if __name__ == "__main__":
    import os
    port = 5006
    app.run(host="0.0.0.0", port=port, debug=False)



