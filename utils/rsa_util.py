from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
import base64

class RSAUtil:
    @staticmethod
    def generate_key_pair():
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()
        return public_key, private_key

    @staticmethod
    def save_private_key(private_key, path):
        pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        with open(path, "wb") as f:
            f.write(pem)

    @staticmethod
    def save_public_key(public_key, path):
        pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        with open(path, "wb") as f:
            f.write(pem)

    @staticmethod
    def load_private_key(path):
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    @staticmethod
    def load_public_key(path):
        with open(path, "rb") as f:
            return serialization.load_pem_public_key(f.read())

    @staticmethod
    def encrypt(public_key, data: bytes) -> bytes:
        return public_key.encrypt(
            data,
            padding.PKCS1v15()
        )

    @staticmethod
    def decrypt(private_key, ciphertext: bytes) -> bytes:
        return private_key.decrypt(
            ciphertext,
            padding.PKCS1v15()
        )

    @staticmethod
    def sign(private_key, data: bytes) -> bytes:
        return private_key.sign(
            data,
            padding.PKCS1v15(),
            hashes.SHA256()
        )

    @staticmethod
    def verify(public_key, data: bytes, signature: bytes) -> bool:
        try:
            public_key.verify(
                signature,
                data,
                padding.PKCS1v15(),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

def test_java_rsa():
    # Java RSA 密钥对
    # 粘贴Java输出的密文Base64
    cipher_b64 = "aYii16AHpVH0YNhXtu6Q/r9I3bUmccH7hEVuaqmzUWvzvwqPuYeH8VtBz4XsDSmCii2GTi+4ZOfYEe/QfIaNccGMjwUM5we6H4HfkXYTaKnBllgRnh9/RtzgGB2oEHXMHkX3Sep+r0HFgqp7xC3r+a1hQuGrewt8/97WVVKVfFuVarWncDmrUe4GKCgJz8zcINEpBi4NKu2/qLGs3hwh9iymfj1QZAheXXQP+xw3BYVkNT6rq3HYA0Ux0QIslsWv13+ud4fFEbFftVODIoPp72JB7qiq4Kq8xZcifCiVxC69tPLcYv4p99WOLZ9KPe7ysUEMqTEMA4tfa8LsBqvlqA=="
    ciphertext = base64.b64decode(cipher_b64)

    # 粘贴Java RSA 密钥对
    pub_pem = b"""-----BEGIN PUBLIC KEY-----
    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhcnlQGtech+JuUkU3l5t
    oB6yVMGfU2eVcL8sOV9SJoQvjd6K9mnMFYXV84eNECPki8VFPtx9X1X8u+FdSfI6
    WFle4ZS6pEkouxTBo2Q7vZyMFFTne0jiUHUB7ffABopnYEDYjspNFXK0C0Yl7wUc
    o/eOM7wFTNClvE0nR3AS488COyJCv5kXxgg2bTrcRsPKbU7/I/s1iXQpo/Rksgc4
    6ui3/VxOZ3nK5lmOiBFiM+Hd8RPqZnJWhRwnpTtDf8iknoYjofrY6h3mZIzf3gQD
    A33Xs0poZJA+/Nx2KdY2JScLEX8ZeuMkI5GjJuk4Zq4oUcEcCc2vGkRheOo3qYNK
    DwIDAQAB
    -----END PUBLIC KEY-----"""

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

    # 解密
    dec = RSAUtil.decrypt(pri, ciphertext)
    print("解密明文:", dec.decode())

def test_generate_key_pair():
    # 生成密钥对
    pub, pri = RSAUtil.generate_key_pair()
    pri_pem = pri.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    pub_pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    print("公钥PEM:\n", pub_pem.decode())
    print("私钥PEM:\n", pri_pem.decode())
    # 加密解密
    msg = b"hello from python"
    enc = RSAUtil.encrypt(pub, msg)
    print("密文Base64:", base64.b64encode(enc).decode())
    dec = RSAUtil.decrypt(pri, enc)
    print("解密明文:", dec.decode())
    # 签名验签
    sig = RSAUtil.sign(pri, msg)
    print("签名Base64:", base64.b64encode(sig).decode())
    print("验签:", RSAUtil.verify(pub, msg, sig))
    
    
    
if __name__ == "__main__":
    test_generate_key_pair()
    test_java_rsa()
    
    
    
    