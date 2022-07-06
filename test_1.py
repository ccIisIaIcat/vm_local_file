import hashlib



while True :
    info = input('输入需要转换为sha256哈希值的文本：')
    data_sha = hashlib.sha256(info.encode('utf-8')).hexdigest()   
    print(info,"的sha256哈希值为：")
    print(data_sha)
