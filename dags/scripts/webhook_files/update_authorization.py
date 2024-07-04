from aux.get_token import refresh_token_function
import aux.config as main_cfg
import requests
import rsa
import string
import random


# Geração de chave pública e privada, bem como a mensagem para ser assinada
public_key, private_key = rsa.newkeys(2048)
encoded_key = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(232))

#Registra Chave Privada em um arquivo .PEM 
with open("webhook_service/cert/private.pem", "wb") as file:
    file.write(private_key.save_pkcs1("PEM"))

with open("webhook_service/cert/public.pem", "wb") as file:
    file.write(public_key.save_pkcs1("PEM"))

#Registra chave assinada num arquivo txt
with open("webhook_service/cert/encoded_key", "wb") as file:
    file.write(encoded_key.encode("utf8"))

#Cria assinatura da chave "aleatória" com a chave privada e registra em um arquivo binário
signature = rsa.sign(encoded_key.encode("utf8"), private_key, "SHA-256")
with open("webhook_service/cert/signature", "wb") as file:
    file.write(signature)


with open("webhook_service/cert/public.pem", "rb") as file:
    public_key_bytes = file.read()

public_key_PEM = public_key_bytes.decode("utf8")

update_json = {
  "authorizationHeaderValue": f"{public_key_PEM}",
  "concurrentDeliveries": 5,
  "maxBatchSize": 5,
  "status": "Active"
}

update_json["authorizationHeaderValue"] = update_json["authorizationHeaderValue"].replace("\n", main_cfg.AUTH_TOKEN_ALIAS)

header = {'Authorization': f'Bearer {refresh_token_function(main_cfg.REFRESH_TOKEN)}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'Content-Type': 'application/vnd.deere.axiom.v3+json'}

response = requests.patch("https://partnerapi.deere.com/platform/eventSubscriptionDelivery", headers=header, json=update_json)

print(update_json)
print(response)

