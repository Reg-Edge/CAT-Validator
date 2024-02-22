import csv
import json
import os
import pandas as pd

input_file_dir_path = os.getcwd() + "\\Input"
config_file_path = os.getcwd() + "\\config\\CAT_2d_schema.json"
global_schema = json.load(open(config_file_path, 'r'))

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
    error_logs = []
    error_col_list = ['Exception Type', 'Atribute Name', 'Value']
    error_messages = pd.DataFrame(columns = error_col_list )

    schema_of_fields = event_schema.get("fields")
    for k,v in record.items():
        specific_field_schema = [x for x in schema_of_fields if x["name"] == k][0]
        if specific_field_schema.get("required") == 'Required' and v == '':
            error_messages['Exception Type'] = 'Required field check'
            error_messages['Attribute Name'] = k
            error_messages['Value'] = v
            error_logs.append(error_messages)
        error_messages = pd.DataFrame(columns = error_col_list)
    
        



                
def main():
    unified_format = []
    for file in os.listdir(input_file_dir_path):
        breakpoint()
        if determine_file_type_using_name(file) == 'JSON':
            unified_format = json_input_parse(file)
        elif determine_file_type_using_name(file) == 'CSV':
            unified_format = csv_input_parse(file)
        for record in unified_format:
            event_schema = fetch_event_schema(record)
            required_fields_check(record,event_schema)

            


    


if __name__ == "__main__":
    main()
    

