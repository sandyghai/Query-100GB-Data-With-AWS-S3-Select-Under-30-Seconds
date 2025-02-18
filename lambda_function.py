#!/usr/bin/env python3

import json
import time
from s3_select import S3Select

print('Loading Function')

bucket = "REPLACE-WITH-YOUR-BUCKET-NAME"

def lambda_handler(event, context):
    
    print("Recieved event: " + json.dumps(event))
    
    try:
                        
        response = search(event)
            
        return response
       
    except Exception as e:
        print(e)
        
def search(event):
    
    print("Inside search")
    t0 = time.time()
    print("event", event)
    payload_response = {'status': 'error'}
    KEY = event['Dataset']
        
    try:
                
        s3_keys =  {}
        
        s3_search_kwargs = {"bucket": bucket}
        s3_select_client = S3Select(**s3_search_kwargs)

        if 'PartId' in event:
            part_id = str(event['PartId'])
            s3_select_client.partid = part_id
        
        if 'ShowTime' in event:
            s3_select_client.search_time = True
        
        if 'ShowContent' in event:
            s3_select_client.display_content = True
            
        if 'Search' in  event:
            search = event['Search']
            
            search_items =[]
            for e in search:
                search_items.append({"key":KEY, "value": {"item":e}})
            
            payload_response['status'] = 'ok'

            if len(search_items) > 0:
                s3_select_client.set_search_items(search_items)
                contents = s3_select_client.list_s3()
                file_searched  = s3_select_client.search_result_count - 1
                payload_response['result'] = { 'file_count': file_searched}
                if contents:
                    row_count = s3_select_client.process_csv_contents(contents)
                    payload_response['result']['row_count'] = row_count
                                
    except Exception as e:
        print("Exception - ", e)
        payload_response['status'] = 'error'
        
    finally:
        t1 = time.time()
        print("Total Search {0} seconds".format(t1-t0))
        return payload_response
    