import os
from dotenv import load_dotenv

load_dotenv()
MAX_USAGE=25
DB_NAME = "spam_xpert"
MONGO_URI = os.getenv("MONGO_URI") 
BASE_URLS = {
    "IND": "https://client.ind.freefiremobile.com",
    "BR":  "https://client.us.freefiremobile.com",
    "ME":  "https://clientbp.ggblueshark.com",
    "BD":  "https://clientbp.ggblueshark.com",
    "PK":  "https://clientbp.ggblueshark.com",
}

REGION_CONFIG = {
    region: {
        "tokens":    f"{region.lower()}_tokens",
        "url_spam":  f"{base}/RequestAddingFriend",
        "url_visit": f"{base}/GetPlayerPersonalShow",
    }
    for region, base in BASE_URLS.items()
}
