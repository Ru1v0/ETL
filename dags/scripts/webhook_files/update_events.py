import json
import requests
import logging
import pandas as pd
import concurrent.futures
import configs.config as main_cfg
from functools import partial
from configs.database import Autonomous
from scripts.webhook_files.aux.dicts import formatacao_dict
from configs.get_token import refresh_token_function



def formatar_json(json_data, lista_parametros, lista_orgs):
  json_data["filters"][0]["values"] = lista_orgs #OrgList
  json_data["eventTypeId"] = lista_parametros[0] #fieldOperation
  json_data["filters"][1]["values"] = [lista_parametros[1]] #application, harvest, seeding ou tillage
  json_data["targetEndpoint"]["uri"] = lista_parametros[2] #endpoint
  json_data["displayName"] = f"{lista_parametros[3]} Field Ops Subscription" #application, harvest, seeding tillage
  json_data["status"] = "Active"
  return json_data

def verificar_orgs(tuple_ids, headers_request):
  url_request = f"https://partnerapi.deere.com/platform/organizations/{tuple_ids[0]}/fields/"#{tuple_ids[0]}/fieldOperations"

  request = requests.get(url = url_request, headers = headers_request)

  if request.status_code == 200 and tuple_ids != None:
    return tuple_ids[0]

def terminar_inscricoes():
  headers_request = {'Authorization': f'Bearer {refresh_token_function(main_cfg.REFRESH_TOKEN)}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'Content-Type': 'application/vnd.deere.axiom.v3+json'}

  url_request = "https://partnerapi.deere.com/platform/eventSubscriptions;start={};count=10"
  pagina = 0
  conteudo = 10

  requests_inscricaos = requests.get(url = url_request.format(0), headers = headers_request)
  inscricaos_json = json.loads(requests_inscricaos.text)["values"]
  len_inscricao = json.loads(requests_inscricaos.text)["total"]
  lista_json = [inscricaos_json]

  while len_inscricao >= conteudo:
    pagina += 10
    conteudo += 10
    requests_inscricaos = requests.get(url = url_request.format(pagina), headers = headers_request)
    json_inscricaos = json.loads(requests_inscricaos.text)["values"]
    lista_json.append(json_inscricaos)

  lista_terminar_inscricoes = []

  for json_inscricoes in lista_json:
      for inscricao in json_inscricoes:
          if inscricao["status"] == "Active":
              lista_terminar_inscricoes.append(inscricao)

  for inscricao in lista_terminar_inscricoes:
    inscricao["status"] = "Terminated"
    id_inscricao = inscricao["id"]

    request = requests.put(url = f"https://partnerapi.deere.com/platform/eventSubscriptions/{id_inscricao}", headers=headers_request, json = inscricao)
    logging.info(f"Inscrição {id_inscricao} terminada. Código de Retorno {request.status_code}")











