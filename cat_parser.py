import pandas as pd
import warnings
import csv
import json
import math
import os
import re

warnings.simplefilter('ignore') 



input_file_dir_path = os.getcwd() + "\\Input\\"
config_file_path = os.getcwd() + "\\config\\CAT_2d_schema.json"
global_schema = json.load(open(config_file_path, 'r'))
output_file_path = os.getcwd() + "\\Output"
error_logs = []
duplicate_checker = set()

    
def determine_file_type(file):
    if file[-5:].lower() == '.json':
        return 'JSON'
    elif file[-4:].lower() == '.csv':
        return 'CSV'
    else:
        raise Exception(ValueError, f"Cannot read file : {file} - File is not CSV or JSON format")
    
def required_fields_check(record, event_schema):
    #error_col_list = ['Exception Type', 'Attribute Name', 'Value']

    schema_of_fields = event_schema.get("fields")
    for field in schema_of_fields:
        field_name = field["name"]
        if field.get("required") == 'Required' and (record.get(field_name) is None or record.get(field_name) == ''):
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Missing Field', 'Attribute Name': field_name, 'Value': '', 'Event Type': event_schema["eventName"], 'Error Code': 'E01','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
            
def allowed_values_check(record, event_schema):
    schema_of_fields = event_schema.get("fields")
    schema_of_enums = global_schema["choices"]
    for field in schema_of_fields:
        field_name = field["name"]
        if schema_of_enums.get(field_name) is None:
            continue
        enum_list = schema_of_enums.get(field_name)
        #Accounting for empty field and permissible value "NA" in below line
        if (record.get(field_name) == '' or record.get(field_name) is None):
            continue
        if (record.get(field_name)) not in enum_list:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Value not in list of permissible values', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E02','Allowed Values': ','.join(enum_list),'Expected Data Type': '', 'Maximum Field Length': ''})

def data_type_check(record, event_schema):
    schema_of_fields = event_schema.get("fields")
    list_of_datatypes = global_schema["dataTypes"]
    for field in schema_of_fields:
        if not field["required"] == 'Required':
            field_name = field["name"]
            field_datatype = field["dataType"]
            specific_data_type_schema = [x for x in list_of_datatypes if x.get("dataType") == field_datatype]
            if not specific_data_type_schema:
                continue
            specific_data_type_schema = specific_data_type_schema[0]
            specific_JS0N_data_type = specific_data_type_schema.get("JSONDataType")
            
            if (record.get(field_name) == '' or record.get(field_name) is None):
                continue
            
            if field_datatype == 'Timestamp' and not(isinstance(record.get(field_name), (str,int,float))):
                key_column, key_value = get_event_key(record)
                event_date = getEventDate(record)
                error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Incorrect Datatype', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E03','Allowed Values': '','Expected Data Type': 'STRING or NUMBER', 'Maximum Field Length': ''})
            elif specific_JS0N_data_type == 'STRING' and not(isinstance(record.get(field_name), str)):
                key_column, key_value = get_event_key(record)
                event_date = getEventDate(record)
                error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Incorrect Datatype', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E03','Allowed Values': '','Expected Data Type': 'STRING', 'Maximum Field Length': ''})
            elif specific_JS0N_data_type == 'NUMBER' and not(isinstance(record.get(field_name), (int,float))):
                key_column, key_value = get_event_key(record)
                event_date = getEventDate(record)
                error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Incorrect Datatype', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E03','Allowed Values': '','Expected Data Type': 'NUMBER', 'Maximum Field Length': ''})
            elif specific_JS0N_data_type == 'BOOLEAN' and not(isinstance(record.get(field_name), bool)):
                key_column, key_value = get_event_key(record)
                event_date = getEventDate(record)
                error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Incorrect Datatype', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E03','Allowed Values': '','Expected Data Type': 'BOOLEAN', 'Maximum Field Length': ''})

def max_length_check(record, event_schema):
    schema_of_fields = event_schema.get("fields")
    list_of_datatypes = global_schema["dataTypes"]
    for field in schema_of_fields:
        if not field["required"] == 'Required':
            field_name = field["name"]
            field_datatype = field["dataType"]
            specific_data_type_schema = [x for x in list_of_datatypes if x.get("dataType") == field_datatype]
            if not specific_data_type_schema:
                continue
            specific_data_type_schema = specific_data_type_schema[0]
            field_max_length = 0
            if specific_data_type_schema.get("maxLength") is not None:
                field_max_length = specific_data_type_schema.get("maxLength")
            length_of_field_data = 0
            if record.get(field_name) is not None:
                length_of_field_data = len(str(record.get(field_name)))
            if(length_of_field_data>field_max_length):
                key_column, key_value = get_event_key(record)
                event_date = getEventDate(record)
                error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Data Quality Error','Exception': 'Field Length Exceeds maximum allowed length', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Event Type': event_schema["eventName"], 'Error Code': 'E01','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': field_max_length})
        

def export_to_csv(error_logs):
    if error_logs:
        export_logs = pd.DataFrame(error_logs)
        breakpoint()
        export_logs.to_csv(output_file_path+ "\\error_log.csv", index = False)
    else:
        print("No error logs")

            
def format_cat_data(filepath, csv_bool):
    
    for line in open(filepath, 'r'):

        line = line.replace('\n','')

        cat_event = {}
        event_schema = []

        # For validation only two things are required - the cat_event and the event_schema 
        # which will be used to validate the event

        # If json
        if not csv_bool:

            # Read event using json module to convert to dictionary
            cat_event = json.loads(line)
            event_type = cat_event['type']
            event_schema = [event for event in global_schema['eventDefinitions'] if event['eventName'] == event_type][0]

        # If csv
        elif csv_bool:

            # Split line by delimiter
            cat_event_temp = line.split(',')
            event_type = cat_event_temp[3]
            event_schema = [event for event in global_schema['eventDefinitions'] if event['eventName'] == event_type][0]

            # use event schema to convert csv to dictionary
            for field in event_schema['fields']:
                cat_event[field['name']] = cat_event_temp[int(field['position']) - 1]


        yield cat_event, event_schema

def check_duplicate_keys_same_day(record, event_schema):
    
    if record.get('firmROEID'):
        if record.get('firmROEID') in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate firmROEID', 'Attribute Name': 'firmROEID', 'Value': record.get('firmROEID'), 'Event Type': event_schema["eventName"], 'Error Code': '3002','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('firmROEID'))
            

    if record.get('orderID'):
        if (record.get('orderID') + '_' + record.get('orderKeyDate')) in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate orderID', 'Attribute Name': 'orderID', 'Value': record.get('orderID'), 'Event Type': event_schema["eventName"], 'Error Code': '3004','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('orderID') + '_' + record.get('orderKeyDate'))

    if record.get('tradeID'):
        if (record.get('tradeID') + '_' + record.get('tradeKeyDate')) in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate tradeID', 'Attribute Name': 'tradeID', 'Value': record.get('tradeID'), 'Event Type': event_schema["eventName"], 'Error Code': '3010','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('tradeID') + '_' + record.get('tradeKeyDate'))

    if record.get('fulfillmentID'):
        if (record.get('fulfillmentID') + '_' + record.get('fillKeyDate')) in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate fulfillmentID', 'Attribute Name': 'fulfillmentID', 'Value': record.get('fulfillmentID'), 'Event Type': event_schema["eventName"], 'Error Code': '3012','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('fulfillmentID') + '_' + record.get('fillKeyDate'))
        
    if record.get('quoteID'):
        if (record.get('quoteID') + '_' + record.get('quoteKeyDate') + '_' + record.get('RFQID')) in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate quoteID', 'Attribute Name': 'quoteID', 'Value': record.get('quoteID'), 'Event Type': event_schema["eventName"], 'Error Code': '3016','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('quoteID') + '_' + record.get('quoteKeyDate') + '_' + record.get('RFQID'))
        
    if record.get('allocationID'):
        if (record.get('allocationID') + '_' + record.get('allocationKeyDate')) in duplicate_checker:
            key_column, key_value = get_event_key(record)
            event_date = getEventDate(record)
            error_logs.append({'Key Column':key_column, 'Key Value': key_value,'Event Date':event_date,'Error Type':'Linkage Error','Exception': 'Intra-Linkage Error: Duplicate allocationID', 'Attribute Name': 'allocationID', 'Value': record.get('allocationID'), 'Event Type': event_schema["eventName"], 'Error Code': '3020','Allowed Values': '','Expected Data Type': '', 'Maximum Field Length': ''})
        else:
            duplicate_checker.add(record.get('allocationID') + '_' + record.get('allocationKeyDate'))

def get_event_key(record):
   keyColumn = ''
   keyValue =''
   
   if record.get('orderID'):
       keyColumn = 'orderID'
       keyValue = record.get('orderID')
    
   if record.get('tradeID'):
       keyColumn = 'tradeID'
       keyValue = record.get('tradeID')
   
   if record.get('fulfillmentID'):
       keyColumn = 'fulfillmentID'
       keyValue = record.get('fulfillmentID')
   
   if record.get('quoteID'):
       keyColumn = 'quoteID'
       keyValue = record.get('quoteID')
   
   if record.get('allocationID'):
       keyColumn = 'allocationID'
       keyValue = record.get('allocationID')      
    
   return keyColumn, keyValue

def getEventDate(record):

    return record.get('eventTimestamp').split('T')[0]



def main():

    cat_file_re = re.compile('^[0-9]{2,4}_[A-Z]{2,4}_[0-9]{8}_TEST_OrderEvents_[0-9]{6}\.(json|JSON|CSV|csv)$')

    for file in os.listdir(input_file_dir_path):

        # only open files that match the cat file pattern regex
        # if not cat_file_re.match(file):
        #      continue
        if not file == 'ZUnitTest.json':
            continue

        # File type is determined as JSON records do not require enhancement whereas CSV records do
        file_type = determine_file_type(file)


        csv_bool = None
        if file_type == 'JSON': csv_bool = False
        elif file_type == 'CSV': csv_bool = True


        formatted_cat_data = format_cat_data(input_file_dir_path+file, csv_bool)
                
        for cat_event, event_schema in formatted_cat_data:
            required_fields_check(cat_event, event_schema)
            allowed_values_check(cat_event,event_schema)
            data_type_check(cat_event,event_schema)
            max_length_check(cat_event,event_schema)
            check_duplicate_keys_same_day(cat_event,event_schema)
        export_to_csv(error_logs)

            
if __name__ == "__main__":
    main()
    

