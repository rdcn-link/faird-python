import base64
import json
import requests
from utils.logger_utils import get_logger
from cryptography.hazmat.primitives import serialization, hashes

from utils.rsa_util import RSAUtil

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
    ciphertext = base64.b64decode(signature)
    pri_pem = b"""-----BEGIN PRIVATE KEY-----
        MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCFyeVAa15yH4m5
        SRTeXm2gHrJUwZ9TZ5Vwvyw5X1ImhC+N3or2acwVhdXzh40QI+SLxUU+3H1fVfy7
        4V1J8jpYWV7hlLqkSSi7FMGjZDu9nIwUVOd7SOJQdQHt98AGimdgQNiOyk0VcrQL
        RiXvBRyj944zvAVM0KW8TSdHcBLjzwI7IkK/mRfGCDZtOtxGw8ptTv8j+zWJdCmj
        9GSyBzjq6Lf9XE5necrmWY6IEWIz4d3xE+pmclaFHCelO0N/yKSehiOh+tjqHeZk
        jN/eBAMDfdezSmhkkD783HYp1jYlJwsRfxl64yQjkaMm6ThmrihRwRwJza8aRGF4
        6jepg0oPAgMBAAECggEAKHbLWSQAMTkbsQgLAUSDGIogOqVDGHk8SBjx+bo8QPz6
        2whDdErEWIAdmSXWwtpwPdvg2SXb33FE2thLHFngTqddHJIcryKXaJnbBR2EOhF2
        hpG1X2LIXDg/aKdlkzTaFlEWjn1eOg3yqXRCddvQcUEm57vGyJhUgDK/ngomFHMw
        TS9vNUXX63bb/b1kNs8JShW6D8nEamxXGOSg48kRI2z/UR8jTFngK91PMbuszRBy
        TZEAPuqyQMCxEecFNWHBixton0/6CHGW+8Fz0CRB9BBkqoKUYOvkYm5bxq76UPts
        U18vY9CybP/cuajp9tNUgg41/Yf+sLZC4+et2744cQKBgQC8QhBbqKY4UveNg8Lw
        m01TMuYaAou9pW8pU0X3g0ehkgddzgEdafuRffthcfxD941rEYXwSbeZyc42yG2i
        YBgowbOxyb7NdRyDRUbm0mZvDzl13Cw61J0Uj4UCNYTn9FQ7jcrHIKgVkKgRV+vj
        fB4cjSWRLhw2Dsn3JmN28J/tsQKBgQC17jl5+Onhnykg3HvQrJ2NcoNyasBLwBRZ
        y6uosoELJUTkwgDTAyxs06Rw3XNi3urAex8dYOCqzKnNkgDX7K5Vm/zu150DvBiO
        4Y79CPQ+oBMP8Hr0zLgbUYt5FMiGulKisC82qygZxNUncc+BuQJrVgSMpng5GUJo
        LSEbM33jvwKBgQCT2NpZ8bgg8d+izwDwzzbKCWY2tRsj5GHJtbF0MjC2TiXk4J11
        iT9dwvACVm+EvUzd8lQbIvnDGH8P/RZE+GzgzUcfcE6dssSiv2xwaHqz6+P3kONX
        jJSUiiYuVvO66uKgJrpC6n22+fVUMZV7AYXwbFU9kFXtim3I4Ogqh81B8QKBgFxs
        j8qFR9wVTYGiQUx7xPas7FZeIR7aj/wmWiiztpcA8gT9AgoxHIqU4n9sIUhjUwNC
        CUaVhs1+d+01dTQ8yzw2qMJ1yxCwqBE+SbQGrn56N/TrCKwArK9EOATlKBI56e04
        cGPng9HDiz4ktXSDltUMt/b0QzNOv8vI0X7OI/BlAoGANFRJSt+rzQnSst9536Ks
        3mHgklw6Te3kEJsGhZnUDjy8aNuWAj9Qd0Jp/rf1EAr3X9JJuAi6SO1uCULww/VB
        8g0E6Mq4GLx7sp6MhGuAwg8ZjeYkLypd4hj2MmYLWcvDKCNucz5ePzB1Xx20fcXq
        SORlppTr4W5KaARF/DkhsXI=
        -----END PRIVATE KEY-----"""
    pri = serialization.load_pem_private_key(pri_pem, password=None)
    dec = RSAUtil.decrypt(pri, ciphertext)
    dec_decode = dec.decode()
    return controld_domain_name == dec_decode

def base64_to_hex(base64_key):
    return base64.b64decode(base64_key).hex()
