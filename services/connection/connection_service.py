import base64
import json
import requests
from utils.logger_utils import get_logger
logger = get_logger(__name__)

oauth_url = "https://api.opendatachain.cn/auth/"
client_id = "faird-client1"
client_secret = "tcqi54cnp3cewj94nd9uop2q"

def connect_server_with_oauth(type: str, username: str, password: str):
    if type == "conet":
        url = f"{oauth_url}/oauth/token"
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }
        params = {
            "username": username,
            "password": password,
            "grantType": "password"
        }
        try:
            response = requests.post(url, headers=headers, json=params)
            response.raise_for_status()  # 检查请求是否成功
            response_json = response.json()
            # 解析JSON字符串
            token = response_json.get("data")
            return token
        except requests.RequestException as e:
            logger.info(f"Error connecting server: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.info(f"Error decoding JSON response: {e}")
            return None
        except KeyError as e:
            logger.info(f"Error parsing response: {e}")
            return None
        except Exception as e:
            logger.info(f"Unexpected error: {e}")
            return None
    return None

def connect_server_with_controld(controld_domain_name: str, signature: str):
    # public_key = "MFkwEwYHKoZIzj0CAQYIKoEcz1UBgi0DQgAEB8_TjPyc-SdfCfl3OJrit143THU2crv8mpIFcyjVIulJnY0YQYCKLu3fSmh-jdObAwUOlgk4Q9WoMaXMKRYKCA=="
    # private_key = "MIGTAgEAMBMGByqGSM49AgEGCCqBHM9VAYItBHkwdwIBAQQgeTekSnZauJKKsCj03gJPXwWelYq3j7_V4mLPiMlq2qWgCgYIKoEcz1UBgi2hRANCAAQHz9OM_Jz5J18J-Xc4muK3XjdMdTZyu_yakgVzKNUi6UmdjRhBgIou7d9KaH6N05sDBQ6WCThD1agxpcwpFgoI"
    #
    # sm2_crypt = sm2.CryptSM2(public_key="", private_key=private_key)
    # encrypted_bytes = base64.b64decode(signature)  # 将 Base64 字符串解码为 bytes
    # decrypt_bytes = sm2_crypt.decrypt(encrypted_bytes)
    # a = base64.b64encode(decrypt_bytes)# 解密
    # a = a.decode("utf-8")
    # return controld_domain_name == decrypt_bytes.decode("utf-8")
    return True

def base64_to_hex(base64_key):
    return base64.b64decode(base64_key).hex()
