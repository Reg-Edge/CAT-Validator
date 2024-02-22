import csv
import json
import math
import os
import pandas as pd

input_file_dir_path = os.getcwd() + "\\Input"
config_file_path = os.getcwd() + "\\config\\CAT_2d_schema.json"
global_schema = json.load(open(config_file_path, 'r'))
output_file_path = os.getcwd() + "\\Output"
error_logs = []

def determine_file_type(filename):
    try:
        with open(filename, 'r') as file:
            try:
                json.load(file)
                return 'JSON'
            except json.JSONDecodeError:
                pass
            try:
                csv.Sniffer().sniff(file.read(1024))
                return 'CSV'
            except csv.Error:
                pass
            return None
    except FileNotFoundError:
        return None
    
def determine_file_type_using_name(file):
    file_name,file_ext = os.path.splitext(file)
    if file_ext.lower() == '.json':
        return 'JSON'
    elif file_ext.lower == '.csv':
        return 'CSV'
    else:
        return None
    
def json_input_parse(input_file):
    #list of dicts
    list_of_parsed_dicts = []
    file_path = input_file_dir_path + "\\" + input_file
    with open(file_path, 'r') as file:
        for line in file:
            try:
                data = json.loads(line.strip())
                list_of_parsed_dicts.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
    return list_of_parsed_dicts

def csv_input_parse(input_file):
    #list of dicts
    list_of_parsed_dicts = []
    file_path = input_file_dir_path + "\\" + input_file
    with open(file_path, 'r') as file:
        read_csv = csv.DictReader(file)
        for row in read_csv:
            list_of_parsed_dicts.append(row)
    return list_of_parsed_dicts

def fetch_event_schema(record):
    event = record.get("type")

    event_defs = global_schema['eventDefinitions']

    event_schema = [x for x in event_defs if x["eventName"] == event][0]
        
    return event_schema

def required_fields_check(record, event_schema):
    #error_col_list = ['Exception Type', 'Attribute Name', 'Value']
    #error_messages = pd.DataFrame(columns = error_col_list)

    schema_of_fields = event_schema.get("fields")
    for field in schema_of_fields:
        field_name = field["name"]
        if field.get("required") == 'Required' and (record.get(field_name) is None or record.get(field_name) == ''):
            #error_messages = error_messages.append({'Exception Type': 'Required field check', 'Attribute Name': field_name, 'Value': ''})
            # error_messages['Attribute Name'] = field_name
            # breakpoint()
            # error_messages['Exception Type'] = 'Required field check'
            # error_messages['Value'] = ''
            error_logs.append({'Exception Type': 'Required field check', 'Attribute Name': field_name, 'Value': '', 'Allowed Values': ''})
        #error_messages = pd.DataFrame(columns = error_col_list)
            
def allowed_values_check(record, event_schema):
    schema_of_fields = event_schema.get("fields")
    schema_of_enums = global_schema["choices"]
    for field in schema_of_fields:
        field_name = field["name"]
        if schema_of_enums.get(field_name) is None:
            continue
        enum_list = schema_of_enums.get(field_name)
        #Accounting for empty field and permissible value "NA" in below line
        if (record.get(field_name) == '' or record.get(field_name) is None) and ("NA" in enum_list):
            continue
        if (record.get(field_name)) not in enum_list:
            error_logs.append({'Exception Type': 'Allowed Values Check', 'Attribute Name': field_name, 'Value': record.get(field_name), 'Allowed Values': ",".join(enum_list)})





        

def export_to_csv(error_logs):
    if error_logs:
        export_logs = pd.DataFrame(error_logs)
        export_logs.to_csv(output_file_path+ "\\error_log.csv", index = False)
    else:
        print("No error logs")

    


    # for k,v in record.items():
    #     specific_field_schema = [x for x in schema_of_fields if x["name"] == k][0]
    #     if specific_field_schema.get("required") == 'Required' and v == '':
            
    
        



                
def main():
    unified_format = []
    for file in os.listdir(input_file_dir_path):
        if file != 'ZUnitTest.json':
            continue
        if determine_file_type_using_name(file) == 'JSON':
            unified_format = json_input_parse(file)
        elif determine_file_type_using_name(file) == 'CSV':
            unified_format = csv_input_parse(file)
        for record in unified_format:
            event_schema = fetch_event_schema(record)
            required_fields_check(record,event_schema)
            allowed_values_check(record,event_schema)
    export_to_csv(error_logs)


            


    


if __name__ == "__main__":
    main()
    

