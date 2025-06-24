from gmssl import sm2, func
import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import os

class SM2Utils:
    def __init__(self, private_key=None, public_key=None):
        """
        初始化 SM2 工具类
        :param private_key: 私钥（16 进制字符串）
        :param public_key: 公钥（16 进制字符串）
        """
        self.private_key = private_key
        self.public_key = public_key
        self.sm2_crypt = sm2.CryptSM2(public_key=public_key, private_key=private_key)

    @staticmethod
    def generate_key_pair():
        """
        生成 SM2 密钥对
        :return: 公钥和私钥（16 进制字符串）
        """
        private_key_hex = func.random_hex(64)  # 生成 64 位随机私钥
        sm2_crypt = sm2.CryptSM2(private_key=private_key_hex, public_key="")  # 临时初始化
        public_key_hex = sm2_crypt._kg(int(private_key_hex, 16), sm2_crypt.ecc_table['g'])  # 通过私钥生成公钥

        # 转换为 Base64 格式
        private_key_base64 = base64.b64encode(bytes.fromhex(private_key_hex)).decode("utf-8")
        public_key_base64 = base64.b64encode(bytes.fromhex(public_key_hex)).decode("utf-8")

        return public_key_base64, private_key_base64

    @staticmethod
    def generate_base64_key_pair():
        # 生成椭圆曲线密钥对
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key = private_key.public_key()

        # 提取私钥
        private_numbers = private_key.private_numbers()
        private_value = private_numbers.private_value

        # 提取公钥的 X 和 Y 坐标
        public_numbers = public_key.public_numbers()
        x = public_numbers.x
        y = public_numbers.y

        # 转换为 Base64 格式
        private_key_base64 = base64.b64encode(private_value.to_bytes(32, byteorder="big")).decode("utf-8")
        public_key_base64 = base64.b64encode(x.to_bytes(32, byteorder="big") + y.to_bytes(32, byteorder="big")).decode(
            "utf-8")

        return public_key_base64, private_key_base64

    def encrypt(self, data):
        """
        使用公钥加密数据
        :param data: 待加密数据（字节类型）
        :return: 加密后的字节数据
        """
        return self.sm2_crypt.encrypt(data)

    def decrypt(self, encrypted_data):
        """
        使用私钥解密数据
        :param encrypted_data: 加密后的字节数据
        :return: 解密后的字节数据
        """
        return self.sm2_crypt.decrypt(encrypted_data)

# 示例用法
if __name__ == "__main__":
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    # 对方的公钥（示例中使用自己的公钥）
    peer_public_key = public_key

    # 生成共享密钥
    shared_key = private_key.exchange(ec.ECDH(), peer_public_key)

    derived_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b'handshake data',
    ).derive(shared_key)

    # # 待加密数据
    # data = b'{"key": "value"}'
    #
    # # 使用 AES 加密数据
    # iv = os.urandom(16)  # 生成随机 IV
    # cipher = Cipher(algorithms.AES(derived_key), modes.CFB(iv))
    # encryptor = cipher.encryptor()
    # encrypted_data = encryptor.update(data) + encryptor.finalize()
    #
    # # 解密数据
    # decryptor = cipher.decryptor()
    # decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
    #
    # print("原始数据:", data)
    # print("加密数据:", encrypted_data)
    # print("解密数据:", decrypted_data)
    #
    # # 生成密钥对
    # public_key_base64_0, private_key_base64_0 = SM2Utils.generate_base64_key_pair()
    # print("Public Key:", public_key_base64_0)
    # print("Private Key:", private_key_base64_0)

    public_key_base64 = "MFkwEwYHKoZIzj0CAQYIKoEcz1UBgi0DQgAEtzATutfBFy08TH0_M28LfDuL_6t8_L8qGzIroYcWInQpBasGTmcwuhfPR74N3iEjXyrNomRaRK4v74RAkVzjaw=="
    private_key_base64 = "MIGTAgEAMBMGByqGSM49AgEGCCqBHM9VAYItBHkwdwIBAQQgBRVb_SZbnA5yizZRmlqNZzr4lZY7E3oiFQWhdaP6zU2gCgYIKoEcz1UBgi2hRANCAAS3MBO618EXLTxMfT8zbwt8O4v_q3z8vyobMiuhhxYidCkFqwZOZzC6F89Hvg3eISNfKs2iZFpEri_vhECRXONr"

    public_key_hex = base64.b64decode(public_key_base64).hex()
    private_key_hex = base64.b64decode(private_key_base64).hex()

    # 初始化 SM2 工具类
    sm2_utils = SM2Utils(private_key=private_key, public_key=public_key)

    # 待加密的 JSON 数据
    json_data = '{"key": "value"}'.encode("utf-8")

    # 加密
    encrypted_data = sm2_utils.encrypt(json_data)
    print("Encrypted (Base64):", base64.b64encode(encrypted_data).decode("utf-8"))

    # 解密
    decrypted_data = sm2_utils.decrypt(encrypted_data)
    print("Decrypted:", decrypted_data)
    #print("Decrypted:", decrypted_data.decode("utf-8"))

    # 验证加密解密结果
    assert json_data == decrypted_data, "SM2 加密解密测试失败"