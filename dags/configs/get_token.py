import json
import logging
import requests
import pandas as pd
from configs import config as cfg

def refresh_token_function(refresh_token: str) -> str:
    #=================================================================#
    #Dados necessários para compor o conteúdo do método POST:

    CLIENT_ID = cfg.API_CLIENT_ID
    CLIENT_SECRET = cfg.API_CLIENT_SECRET 
    CLIENT_REDIRECT_URI = cfg.API_CLIENT_REDIRECT_URI 
    SCOPES_REQUEST = cfg.API_SCOPES_REQUEST

    #=================================================================#
    #Dados necessários para fazer o request:

    url = "https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/v1/token"
    header = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    parameters = {"grant_type": "refresh_token",
           "refresh_token" : refresh_token,
           "redirect_uri": CLIENT_REDIRECT_URI,
           "scope": SCOPES_REQUEST,
           "client_id": CLIENT_ID,
           "client_secret": CLIENT_SECRET}
    
    #=================================================================#

    post_refresh = requests.post(url, data = parameters, headers=header) #realiza o método post
    request = post_refresh.text #obtém o conteúdo de resposta do método
    request_json = json.loads(request) #carrega o conteúdo do método post para um objeto json
    request_df = pd.json_normalize(request_json, errors = "ignore") #transforma o objeto json em um dataframe

    access_token = request_df["access_token"].iloc[0] #acessa a coluna "access_token" e obtém o token de acesso atualizado

    return access_token #retorna o token de acesso






