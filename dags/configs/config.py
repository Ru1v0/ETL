# Extraction
TABLES_EXTRACTION = [
    "organizations",
    "chemicals",
    "clients",
    "fertilizers",
    "machines",
    "operators",
    "varieties",
    "alerts",
    "farms",
    "fields",
    "boundaries",
    "machinesMeasurements",
    "machinesMeasurementsIntervals",
    "machinesMeasurementsBuckets",
    "fieldOperationsHarvest",
    "fieldOperationsApplications",
    "fieldOperationsSeeding",
    "fieldOperationsTillage",
    "measurementTypesHarvest",
    "measurementTypesSeeding",
    "measurementTypesTillage",
    "measurementTypesApplications"
]

# Refresh Token
REFRESH_TOKEN = "vMPpfFyuHTZkj7EabGQRpY6-mwbXG3xRVdw_zdnNV_o"

#Authorization Alias
AUTH_TOKEN_ALIAS = "!@#"

# I = Incremental / F = Full
API_LOAD_TYPE = 'F'

AUTH_URL = 'https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/v1/authorize?response_type=code&client_id=0oa4fbyfvn40lTMEr5d7&redirect_uri=http%3A%2F%2Fportal.iguacumaquinas.com.br%3A7084%2Frest%2FIGMAQ001&scope=eq2+offline_access+eq1+ag2+ag1+files&state=Some+Unique+Identifier'
AUTH_SMS_URL= 'https://sso.johndeere.com/app/okta_org2org/exkadduns11A6fK5n1t7/sso/saml'

# CONECT API SESSION ENVIRONMENTS;
API_CLIENT_ID = '0oa4fbyfvn40lTMEr5d7' 
API_CLIENT_SECRET = 'd1112aab2af44ea8ad58d01d5b872cab' 
API_CLIENT_REDIRECT_URI = 'http://portal.iguacumaquinas.com.br:7084/rest/IGMAQ001'
API_WELL_KNOWN_URL = 'https://signin.johndeere.com/oauth2/aus78tnlaysMraFhC1t7/.well-known/oauth-authorization-server'
API_SCOPES_REQUEST = {"offline_access ag1 ag2 eq1 eq2 files"}

# Database: Credentials
DB_DW_USER = 'DW'
DB_DW_PASS = 'D4t4W2022_OciAut#'

DB_STG_USER = 'STAGE'
DB_STG_PASS = 'ST4g3_OciAut#'

DB_CTRL_USER = 'CONTROLE'
DB_CTRL_PASS = 'C0tr0l3_Oci#'

ADW_DSN  = 'dwig_low'
ADW_CFG = '/opt/oracle/instantclient_21_10/network/admin'
ADW_CFG_PRD  = '/usr/lib/oracle/19.17/client64/lib/network/admin/'

