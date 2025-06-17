from gmssl import sm2, func
import base64

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
    # 生成密钥对
    public_key_base64_0, private_key_base64_0 = SM2Utils.generate_key_pair()
    print("Public Key:", public_key_base64_0)
    print("Private Key:", private_key_base64_0)

    #public_key_base64 = "MFkwEwYHKoZIzj0CAQYIKoEcz1UBgi0DQgAEaGraE8rgvcgxq7RKZe-IQaRuVsiKHtWM9a7A4ehshcS3aAjIJbbWR93mvFzImhpIV4kE6sknQpHoBm9qi3nufw=="
    #private_key_base64 = "MIGTAgEAMBMGByqGSM49AgEGCCqBHM9VAYItBHkwdwIBAQQgpybOFF8JSWIZ6g9ShllNZHnBc4nrshcDTDodjphTEG2gCgYIKoEcz1UBgi2hRANCAARoatoTyuC9yDGrtEpl74hBpG5WyIoe1Yz1rsDh6GyFxLdoCMglttZH3ea8XMiaGkhXiQTqySdCkegGb2qLee5_"

    public_key = bytes.hex(base64.b64decode(public_key_base64_0)) if public_key_base64_0 else None
    private_key = bytes.hex(base64.b64decode(private_key_base64_0)) if private_key_base64_0 else None

    # 初始化 SM2 工具类
    sm2_utils = SM2Utils(private_key=private_key, public_key=public_key)

    # 待加密的 JSON 数据
    json_data = '{"key": "value"}'.encode("utf-8")

    # 加密
    encrypted_data = sm2_utils.encrypt(json_data)
    print("Encrypted (Base64):", base64.b64encode(encrypted_data).decode("utf-8"))

    # 解密
    decrypted_data = sm2_utils.decrypt(encrypted_data)
    print("Decrypted:", decrypted_data.decode("utf-8"))

    # 验证加密解密结果
    assert json_data == decrypted_data, "SM2 加密解密测试失败"