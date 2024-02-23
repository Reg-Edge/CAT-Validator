import pandas as pd
import warnings
import csv
import json
import os
import re

warnings.simplefilter('ignore') 



input_file_dir_path = os.getcwd() + "\\Input\\"
config_file_path = os.getcwd() + "\\config\\CAT_2d_schema.json"
global_schema = json.load(open(config_file_path, 'r'))

    
def determine_file_type(file):
    if file[-5:].lower() == '.json':
        return 'JSON'
    elif file[-4:].lower() == '.csv':
        return 'CSV'
    else:
        raise Exception(ValueError, f"Cannot read file : {file} - File is not CSV or JSON format")
    

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


def required_fields_check(cat_event, event_schema):
    print(cat_event, event_schema)
    # error_logs = []
    # error_col_list = ['Exception Type', 'Atribute Name', 'Value']
    # error_messages = pd.DataFrame(columns = error_col_list )

    # schema_of_fields = event_schema.get("fields")
    # for k,v in cat_event.items():
    #     specific_field_schema = [x for x in schema_of_fields if x["name"] == k][0]
    #     if specific_field_schema.get("required") == 'Required' and v == '':
    #         error_messages['Exception Type'] = 'Required field check'
    #         error_messages['Attribute Name'] = k
    #         error_messages['Value'] = v
    #         error_logs.append(error_messages)
    #     error_messages = pd.DataFrame(columns = error_col_list)
    
        


def main():

    cat_file_re = re.compile('^[0-9]{2,4}_[A-Z]{2,4}_[0-9]{8}_TEST_OrderEvents_[0-9]{6}\.(json|JSON|CSV|csv)$')

    for file in os.listdir(input_file_dir_path):

        # only open files that match the cat file pattern regex
        if not cat_file_re.match(file):
             continue

        # File type is determined as JSON records do not require enhancement whereas CSV records do
        file_type = determine_file_type(file)


        csv_bool = None
        if file_type == 'JSON': csv_bool = False
        elif file_type == 'CSV': csv_bool = True

        for cat_event, event_schema in format_cat_data(input_file_dir_path+file, csv_bool):
            required_fields_check(cat_event, event_schema)

        # Open file and iterate through each line




            


    


if __name__ == "__main__":
    main()
    

