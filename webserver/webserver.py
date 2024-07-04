from flask import Flask, request
import pandas as pd
import rsa
from datetime import datetime

#===================================================================================================#
def processar_request(request, tabela):
    now = datetime.now()
    marcador = now.strftime("%d_%m_%Y-%H_%M_%S.%f")

    post_content = request.json

    post_content_df = pd.json_normalize(post_content, errors="ignore")
    post_content_df.to_csv(f"dags/scripts/webhook_files/eventos/{tabela}/evento_{marcador}.csv", index = False)

def verificar_autenticacao(request_header) -> str:
    if "Authorization" not in request_header:
        return "Abort"

    #Obtém assinatura
    with open("webserver/cert/signature", "rb") as file:
        signature = file.read()
    
    #Obtém chave que foi assinada
    with open("webserver/cert/encoded_key", "rb") as file:
        encoded_key = file.read()
    
    #Alias que se passa pelo código de autorização no cabeçalho do request recebido
    AUTH_TOKEN_ALIAS = "!@#"

    #Obtém chave pública do header "Authorization"
    public_key_str = request_header["Authorization"].replace(AUTH_TOKEN_ALIAS, '\n')

    public_key_bytes = public_key_str.encode("utf8")

    public_key = rsa.PublicKey.load_pkcs1(public_key_bytes)
    
    #Cria a verificação do RSA da chave pública com a chave e o objeto da assinatura
    signature_verification = rsa.verify(encoded_key, signature, public_key)

    #Verifica se a assinatura utiliza o Hash utilizado
    if signature_verification != "SHA-256":
        return "Abort"

    return "Success"

#===================================================================================================#

app = Flask(__name__)

#END POINT: https://dw.iguacumaquinas.com.br:6001

#===================================================================================================#

@app.route("/webhookcallback/fieldOperationApplication", methods = ["POST"])
def fop_app():
    if verificar_autenticacao(request.headers) == "Success":
        processar_request(request, "fop_app")
        return ("", 204)  

    return (401)

@app.route("/webhookcallback/fieldOperationHarvest", methods = ["POST"])
def fop_har():
    if verificar_autenticacao(request.headers) == "Success":
        processar_request(request, "fop_har")
        return ("", 204)  

    return (401)

@app.route("/webhookcallback/fieldOperationSeeding", methods = ["POST"])
def fop_see():
    if verificar_autenticacao(request.headers) == "Success":
        processar_request(request, "fop_see")
        return ("", 204)  

    return (401)

@app.route("/webhookcallback/fieldOperationTillage", methods = ["POST"])
def fop_til():
    if verificar_autenticacao(request.headers) == "Success":
        processar_request(request, "fop_til")
        return ("", 204)  

    return (401)

#===================================================================================================#

certificado_https = ('/home/opc/app-2/webserver/cert/certificate.crt','/home/opc/app-2/webserver/cert/cert.key')


if __name__ == "__main__":
    app.run(host = "0.0.0.0", port = 6001, ssl_context=certificado_https)

#===================================================================================================#