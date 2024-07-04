# CONFIG PRINCIPAL
API_AUTH_TOKEN ='CuEa-e5gsjL5iZQNcbfezxAe5X4oJrriDN86Uxcf_Qk'      # API: Get Tokens
API_LOAD_TYPE = 'F'     # I = Incremental / F = Full

# API CONNECTION AND REQUEST CFG;
API_CLIENT_ID = '0oa4fbyfvn40lTMEr5d7' 
API_CLIENT_SECRET = 'd1112aab2af44ea8ad58d01d5b872cab' 
API_CLIENT_REDIRECT_URI = 'http://portal.iguacumaquinas.com.br:7084/rest/IGMAQ001'
API_WELL_KNOWN_URL = 'https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/.well-known/oauth-authorization-server'
API_PLATFORM = 'https://partnerapi.deere.com/platform'
API_STATE = "Some Unique Identifier"
API_CATALOG_URI = 'https://partnerapi.deere.com/platform/organizations'

# WEBSCRAP CFG;
credentials_table = 'PROTHEUS.ZZE010@PRODPROT'
api_token_table = 'PROTHEUS.ZMS010@PRODPROT'

AUTH_URL = 'https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/v1/authorize?response_type=code&client_id=0oa4fbyfvn40lTMEr5d7&redirect_uri=http%3A%2F%2Fportal.iguacumaquinas.com.br%3A7084%2Frest%2FIGMAQ001&scope=eq2+offline_access+eq1+ag2+ag1+files&state=Some+Unique+Identifier'
AUTH_SMS_URL= 'https://sso.johndeere.com/app/okta_org2org/exkadduns11A6fK5n1t7/sso/saml'

xpath_login_field = '//*[@id="idp-discovery-username"]'
xpath_password_field = '//*[@id="input29"]'
xpath_next_bottom = '//*[@id="idp-discovery-submit"]'
xpath_signin_bottom = '//*[@id="form21"]/div[2]/input'
xpath_input_token_field    = '//*[@id="input72"]'
xpath_generate_token_bottom = '//*[@id="form46"]/div[2]/input'
xpath_submit_token_bottom   = '//*[@id="form64"]/div[2]/input'

# Database: Credentials
DB_DW_USER = 'DW'
DB_DW_PASS = 'D4t4W2022_OciAut#'
DB_STG_USER = 'STAGE'
DB_STG_PASS = 'ST4g3_OciAut#'
DB_CTRL_USER = 'CONTROLE'
DB_CTRL_PASS = 'C0tr0l3_Oci#'
ADW_DSN  = 'dwig_low'
ADW_CFG = '/opt/oracle/instantclient_21_10/network/admin'

# TABLES API;
TABLES_CAD = ["organizations", "clients", "chemicals", "varieties", "fertilizers", "operators", "machines", "farms", "fields", "alerts"]
TABLES_FOP = ["boundaries", "fieldoperations_harvest", "fieldoperations_applications", "fieldoperations_seeding", "fieldoperations_tillage"]
TABLES_MEA = ["measurementtypes_harvest", "measurementtypes_applications", "measurementtypes_seeding", "measurementtypes_tillage"]