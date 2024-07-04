recordPath_dict = {
        "organizations":				        ['values'],
        "chemicals": 					        ['values'],
        "clients": 					        ['values'],
        "fertilizers": 					        ['values'],
        "machines": 					        ['values'],
        "operators": 					        ['values'],
        "varieties": 					        ['values'],
        "alerts": 					        ['values'],
        "farms": 					        ['values'],
        "fields": 					        ['values'],
        "boundaries": 					        ['values'],
        "machinesMeasurements": 			        ['values'],
        "machinesMeasurementsIntervals": 	                ['values', 'series', 'intervals'],
        "machinesMeasurementsBuckets": 		                ['values', 'series', 'intervals', 'buckets', 'buckets'],
        "fieldOperationsHarvest":			        ['varieties'],
        "fieldOperationsApplications":		                ['values'],
        "fieldOperationsSeeding":			        ['varieties'],
        "fieldOperationsTillage":			        ['values'],
        "measurementTypesHarvest":			        ['varietyTotals'],
        "measurementTypesApplications":		                ['productTotals'],
        "measurementTypesSeeding":			        ['values'],
        "measurementTypesTillage":			        ['values']
        }

params_dict = {
        "organizations":					{},
        "chemicals": 						{},
        "clients": 						{},
        "fertilizers": 						{},
        "machines": 						{},
        "operators": 						{},
        "varieties": 						{},
        "alerts": 						{},
        "farms": 						{},
        "fields": 						{},
        "boundaries": 						{},
        "machinesMeasurements": 			        {"embed": "measurementDefinition"},
        "machinesMeasurementsIntervals": 	                {"embed": "measurementDefinition"},
        "machinesMeasurementsBuckets":		                {"embed": "measurementDefinition"},
        "fieldOperationsHarvest" : 			        {"fieldOperationType": "HARVEST"},
        "fieldOperationsApplications" : 	                {"fieldOperationType": "APPLICATION"},
        "fieldOperationsSeeding" : 			        {"fieldOperationType": "SEEDING"},
        "fieldOperationsTillage" : 			        {"fieldOperationType": "TILLAGE"},
        "measurementTypesHarvest":	                        {},
        "measurementTypesApplications":	                        {},
        "measurementTypesSeeding":	                        {},
        "measurementTypesTillage":	                        {}
        }

meta_dict = {
        "organizations":					[],			
        "chemicals": 						[],
        "clients": 						[],
        "fertilizers": 						[],
        "machines": 						[],
        "operators": 						[],
        "varieties": 						[],
        "alerts": 						[],
        "farms": 						[],
        "fields": 						[],
        "boundaries": 						[],
        "machinesMeasurements": 			        [],
        "machinesMeasurementsIntervals":	                [],
        "machinesMeasurementsBuckets": 		                [],
        "fieldOperationsHarvest":			        ['id', 'orgId', 'startDate', 'endDate', 'fieldOperationType', 'cropSeason', 'adaptMachineType'],
        "fieldOperationsApplications":		                [],
        "fieldOperationsSeeding":			        ['id', 'orgId', 'startDate', 'endDate', 'fieldOperationType', 'cropSeason', 'adaptMachineType'],
        "fieldOperationsTillage":			        [],
        "measurementTypesHarvest":			        ['measurementName', "measurementCategory", ['averageSpeed','@type'], ['averageSpeed','value'], ['averageSpeed','unitId'], ['averageSpeed','variableRepresentation']],
        "measurementTypesApplications":		                ['measurementName', "measurementCategory", ['area', '@type'], ['area', 'value'], ['area', 'unitId'], ['area', 'variableRepresentation'], ['averageSpeed', '@type'], ['averageSpeed', 'value'], ['averageSpeed', 'unitId'], ['averageSpeed', 'variableRepresentation']],
        "measurementTypesSeeding":			        [],
        "measurementTypesTillage":			        []
        }

remove_column_dict = {
        "organizations":                                        ['links', 'machineCategories.machineCategories'],
        "chemicals": 					        ['links', 'machineCategories.machineCategories'],
        "clients": 				                ['links', 'machineCategories.machineCategories'],
        "fertilizers": 					        ['links', 'machineCategories.machineCategories'],
        "machines": 					        ['links', 'machineCategories.machineCategories'],
        "operators": 					        ['links', 'machineCategories.machineCategories'],
        "varieties": 					        ['links', 'machineCategories.machineCategories'],
        "alerts": 						['links'],
        "farms": 						['links'],
        "fields": 						['links'],
        "boundaries": 					        ['links', 'archived', 'multipolygons'],
        "machinesMeasurements": 		                ['links'],
        "machinesMeasurementsIntervals":                        ['links'],
        "machinesMeasurementsBuckets": 	                        ['links'],
        "fieldOperationsHarvest":		                ['links', 'varieties', 'multipolygons', 'org_id'],
        "fieldOperationsApplications":	                        ['links', 'product.components', 'org_id'],
        "fieldOperationsSeeding":		                ['links', 'varieties', 'org_id'],
        "fieldOperationsTillage":		                ['links', 'org_id'],
        "measurementTypesHarvest":		                ['links', 'varietyTotals', 'productTotals'],
        "measurementTypesApplications":	                        ['links', 'varietyTotals', 'productTotals'],    
        "measurementTypesSeeding":		                ['links', 'varietyTotals', 'productTotals'],
        "measurementTypesTillage":		                ['links', 'varietyTotals', 'productTotals'],
        }

include_column_dict = { #primeira elemento = nome do campo no dataframe final; segunda elemento: nome da chave no select do dataframe
        "organizations":                                        [],
        "chemicals": 						["org_id", "id"],
        "clients":                                              ["org_id", "id"],
        "fertilizers": 						["org_id", "id"],
        "machines": 						["org_id", "id"],
        "operators": 						["org_id", "id"],
        "varieties": 						["org_id", "id"],
        "alerts" : 						["machine_id", "id"],
        "farms" : 						["client_id", "id"],
        "fields" : 						["farm_id", "id"],
        "boundaries" : 						["field_id", "id"],
        "machinesMeasurements": 	                        ["machine_id", "id"],
        "machinesMeasurementsIntervals":                        ["machine_id", "id"],
        "machinesMeasurementsBuckets": 	                        ["machine_id", "id"],
        "fieldOperationsHarvest": 			        ["field_id", "id"],
        "fieldOperationsApplications": 		                ["field_id", "id"],
        "fieldOperationsSeeding": 			        ["field_id", "id"],
        "fieldOperationsTillage": 			        ["field_id", "id"],
        "measurementTypesHarvest": 			        ["fop_id", "id"],
        "measurementTypesApplications": 	                ["fop_id", "id"],
        "measurementTypesSeeding": 			        ["fop_id", "id"],
        "measurementTypesTillage": 			        ["fop_id", "id"]
        }

stage_include_dict = {
        "organizations":                                        "RAW_DATA_ORGANIZATIONS",
        "chemicals": 			                        "RAW_DATA_CHEMICALS",                        
        "clients": 			                        "RAW_DATA_CLIENTS",
        "fertilizers": 			                        "RAW_DATA_FERTILIZERS",
        "machines": 			                        "RAW_DATA_MACHINES",
        "operators": 			                        "RAW_DATA_OPERATORS",
        "varieties": 	        	                        "RAW_DATA_VARIETIES",
        "alerts" : 			                        "RAW_DATA_ALERTS",
        "farms" : 			                        "RAW_DATA_FARMS",
        "fields" : 			                        "RAW_DATA_FIELDS",
        "boundaries" : 			                        "RAW_DATA_BOUNDARIES",
        "machinesMeasurements": 	                        "RAW_DATA_MEASUREMENT_MACHINES",
        "machinesMeasurementsIntervals":                        "RAW_DATA_MEASUREMENT_MACHINES_INTERVALS",
        "machinesMeasurementsBuckets": 	                        "RAW_DATA_MEASUREMENT_MACHINES_BUCKETS",
        "fieldOperationsHarvest": 	                        "RAW_DATA_FIELDOPERATIONS_HARVEST",
        "fieldOperationsApplications": 	                        "RAW_DATA_FIELDOPERATIONS_APPLICATIONS",
        "fieldOperationsSeeding": 	                        "RAW_DATA_FIELDOPERATIONS_SEEDING",
        "fieldOperationsTillage": 	                        "RAW_DATA_FIELDOPERATIONS_TILLAGE",
        "measurementTypesHarvest": 	                        "RAW_DATA_MEASUREMENTTYPES_HARVEST",
        "measurementTypesApplications":                         "RAW_DATA_MEASUREMENTTYPES_APPLICATIONS",
        "measurementTypesSeeding":                              "RAW_DATA_MEASUREMENTTYPES_SEEDING",
        "measurementTypesTillage":                              "RAW_DATA_MEASUREMENTTYPES_TILLAGE"
    
}



url_tabelas = {
        "organizations": "",               
        "chemicals": 	                                        "https://partnerapi.deere.com/platform/organizations/{}/chemicals",	                #orgId        	        
        "clients": 			                        "https://partnerapi.deere.com/platform/organizations/{}/clients",                       #orgId
        "fertilizers": 			                        "https://partnerapi.deere.com/platform/organizations/{}/fertilizers",                   #orgId        
        "machines": 			                        "https://partnerapi.deere.com/platform/organizations/{}/machines",                      #orgId
        "operators": 			                        "https://partnerapi.deere.com/platform/organizations/{}/operators",                     #orgId
        "varieties": 	        	                        "https://partnerapi.deere.com/platform/organizations/{}/varieties",                     #orgId
        "alerts" : 			                        "https://partnerapi.deere.com/platform/machines/{}/alerts",                             #machineId
        "farms" : 			                        "https://partnerapi.deere.com/platform/organizations/{}/clients/{}/farms",              #orgId, clientId
        "fields" : 			                        "https://partnerapi.deere.com/platform/organizations/{}/farms/{}/fields",               #orgId, farmId
        "boundaries" : 			                        "https://partnerapi.deere.com/platform/organizations/{}/fields/{}/boundaries",          #orgId, fieldId
        "machinesMeasurements": 	                        "https://partnerapi.deere.com/platform/machines/{}/machineMeasurements",                #machineId
        "machinesMeasurementsIntervals":                        "https://partnerapi.deere.com/platform/machines/{}/machineMeasurements",                #machineId
        "machinesMeasurementsBuckets": 	                        "https://partnerapi.deere.com/platform/machines/{}/machineMeasurements",                #machineId
        "fieldOperationsHarvest": 	                        "https://partnerapi.deere.com/platform/organizations/{}/fields/{}/fieldOperations",     #orgId, fieldId                        
        "fieldOperationsApplications": 	                        "https://partnerapi.deere.com/platform/organizations/{}/fields/{}/fieldOperations",     #orgId, fieldId
        "fieldOperationsSeeding": 	                        "https://partnerapi.deere.com/platform/organizations/{}/fields/{}/fieldOperations",     #orgId, fieldId
        "fieldOperationsTillage": 	                        "https://partnerapi.deere.com/platform/organizations/{}/fields/{}/fieldOperations",     #orgId, fieldId
        "measurementTypesHarvest": 	                        "https://partnerapi.deere.com/platform/fieldOperations/{}/measurementTypes",            #fieldOperationId
        "measurementTypesApplications":                         "https://partnerapi.deere.com/platform/fieldOperations/{}/measurementTypes",            #fieldOperationId
        "measurementTypesSeeding":                              "https://partnerapi.deere.com/platform/fieldOperations/{}/measurementTypes",            #fieldOperationId
        "measurementTypesTillage":                              "https://partnerapi.deere.com/platform/fieldOperations/{}/measurementTypes"             #fieldOperationId
}

url_valores = {
        "organizations": "",             
        "chemicals":                            ["id"],             
        "clients": 		                ["id"],
        "fertilizers": 		                ["id"],
        "machines": 		                ["id"],
        "operators": 		                ["id"],
        "varieties": 	                        ["id"],
        "alerts" : 		                ["id"],                         #id = machineId
        "farms" : 			        ["org_id", "id"],               #id = clientId
        "fields" : 			        ["org_id", "id"],               #id = farmdId
        "boundaries" : 			        ["org_id", "id"],               #id = fieldId
        "machinesMeasurements":                 ["id"],                         #id = machineId                         
        "machinesMeasurementsIntervals":        ["id"],                         #id = machineId
        "machinesMeasurementsBuckets": 	        ["id"],                         #id = machineId
        "fieldOperationsHarvest": 	        ["org_id", "id"],               #id = fieldId       
        "fieldOperationsApplications": 	        ["org_id", "id"],               #id = fieldId
        "fieldOperationsSeeding": 	        ["org_id", "id"],               #id = fieldId
        "fieldOperationsTillage": 	        ["org_id", "id"],               #id = fieldId
        "measurementTypesHarvest": 	        ["id"],                         #id = fopId
        "measurementTypesApplications":         ["id"],                         #id = fopId
        "measurementTypesSeeding":              ["id"],                         #id = fopId
        "measurementTypesTillage":              ["id"]                          #id = fopId
}

select_tables = {
       "organizations":                         "RAW_DATA_ORGANIZATIONS",            
       "chemicals":                             "RAW_DATA_ORGANIZATIONS", 
       "clients": 	                        "RAW_DATA_ORGANIZATIONS",	        
       "fertilizers": 	                        "RAW_DATA_ORGANIZATIONS",	        
       "machines": 	                        "RAW_DATA_ORGANIZATIONS",	        
       "operators": 	                        "RAW_DATA_ORGANIZATIONS",	        
       "varieties": 	                        "RAW_DATA_ORGANIZATIONS",                
       "alerts" :                               "VW_RAW_DATA_MACHINES",
       "farms" : 	                        "VW_RAW_DATA_CLIENTS",	
       "fields" :                               "VW_RAW_DATA_FARMS",	
       "boundaries" : 	                        "VW_RAW_DATA_FIELDS",		
       "machinesMeasurements":                  "VW_RAW_DATA_MACHINES",      
       "machinesMeasurementsIntervals":         "VW_RAW_DATA_MACHINES",
       "machinesMeasurementsBuckets":           "VW_RAW_DATA_MACHINES",	
       "fieldOperationsHarvest":                "VW_RAW_DATA_FIELDS",
       "fieldOperationsApplications":           "VW_RAW_DATA_FIELDS", 	
       "fieldOperationsSeeding":                "VW_RAW_DATA_FIELDS",	
       "fieldOperationsTillage": 	        "VW_RAW_DATA_FIELDS",        
       "measurementTypesHarvest": 	        "VW_RAW_DATA_FIELDOPERATIONS_HARVEST",
       "measurementTypesApplications":          "VW_RAW_DATA_FIELDOPERATIONS_APPLICATIONS",
       "measurementTypesSeeding":               "VW_RAW_DATA_FIELDOPERATIONS_SEEDING",
       "measurementTypesTillage":               "VW_RAW_DATA_FIELDOPERATIONS_TILLAGE"
}