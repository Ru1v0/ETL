stage_dict = {
    "fop_app" : "RAW_DATA_FIELDOPERATIONS_APPLICATIONS",
    "fop_har" : "RAW_DATA_FIELDOPERATIONS_HARVEST",
    "fop_see" : "RAW_DATA_FIELDOPERATIONS_SEEDING",
    "fop_til" : "RAW_DATA_FIELDOPERATIONS_TILLAGE",
    "measurementTypes_harvest"        : "RAW_DATA_MEASUREMENTTYPES_HARVEST",
    "measurementTypes_application"    : "RAW_DATA_MEASUREMENTTYPES_APPLICATIONS",
    "measurementTypes_seeding"        : "RAW_DATA_MEASUREMENTTYPES_SEEDING",
    "measurementTypes_tillage"        : "RAW_DATA_MEASUREMENTTYPES_TILLAGE"
}

event_subscription_id = {
    "fop_app" : "7643333e-6e9a-4f7b-bd27-a09c544cd97f",
    "fop_har" : "0f4fcead-b2d7-4f47-a8c1-c12e6dfcfef9",
    "fop_see" : "341e18ca-4840-444e-a146-a48aa22108d9",
    "fop_til" : "b53552b8-6fe8-4911-abe5-5f52ce89458e"
}

formatacao_dict = {
    "applications"                      : ["fieldOperation", "application", "https://dw.iguacumaquinas.com.br:6001/webhookcallback/fieldOperationApplication", "Applications"],
    "harvest"                           : ["fieldOperation", "harvest", "https://dw.iguacumaquinas.com.br:6001/webhookcallback/fieldOperationHarvest", "Harvest"],
    "seeding"                           : ["fieldOperation", "seeding", "https://dw.iguacumaquinas.com.br:6001/webhookcallback/fieldOperationSeeding", "Seeding"],
    "tillage"                           : ["fieldOperation", "tillage", "https://dw.iguacumaquinas.com.br:6001/webhookcallback/fieldOperationTillage", "Tillage"],
  }

remove_columns = {
    "fop_har"   :     ['varieties', 'modifiedTime', 'productId'],
    "fop_app"   :     ['links', 'product.components', 'org_id', 'product.productId'],
    "fop_see"   :     ['varieties', 'modifiedTime', 'productId'],
    "fop_til"   :     ['links', 'org_id', 'productId'],
    "measurementTypes_harvest"          :     ['links', 'varietyTotals', 'productTotals'],
    "measurementTypes_application"      :     ['links', 'varietyTotals', 'productTotals'],
    "measurementTypes_seeding"          :     ['links', 'varietyTotals', 'productTotals'],
    "measurementTypes_tillage"          :     ['links', 'varietyTotals', 'productTotals']
}

meta_dict = {
    "measurementTypes_harvest"                           :			        ['measurementName', "measurementCategory", ['averageSpeed','@type'], ['averageSpeed','value'], ['averageSpeed','unitId'], ['averageSpeed','variableRepresentation']],
    "measurementTypes_application"                       :		            ['measurementName', "measurementCategory", ['area', '@type'], ['area', 'value'], ['area', 'unitId'], ['area', 'variableRepresentation'], ['averageSpeed', '@type'], ['averageSpeed', 'value'], ['averageSpeed', 'unitId'], ['averageSpeed', 'variableRepresentation']],
    "measurementTypes_seeding"                           :			        [],
    "measurementTypes_tillage"                           :			        []
}

record_path_dict = {
    "measurementTypes_harvest"                           :			        ['varietyTotals'],
    "measurementTypes_application"                       :		            ['productTotals'],
    "measurementTypes_seeding"                           :			        ['values'],
    "measurementTypes_tillage"                           :			        ['values']
}