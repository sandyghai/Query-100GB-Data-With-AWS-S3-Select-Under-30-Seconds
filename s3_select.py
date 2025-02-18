import sys, os
import time
import boto3
import threading
import botocore
import csv
import io

connection_pool = 1000
config = botocore.config.Config(max_pool_connections=connection_pool)
s3_client = boto3.client('s3', region_name="ap-southeast-2", config=config)

class S3Select:
    
    def __init__(self, **kwargs):
        self.search_result = False
        self.search_result_count = 0
        self.bucket=kwargs['bucket']
        self.partid = "/"
        self.display_content = False
        self.search_time = False
        
    def set_search_items(self, search_items):
        self.search_items = search_items
    
    def _check_unicode(self, text):
        try:
            text = text.encode("utf-8").decode("utf-8")
        except Exception as e:
            print(e)
        finally:
            return text
            
    def _sqlS3(self, **kwargs):
        
        contents = None
        t0 = time.time()
        
        try:
            
            value = str(kwargs['value'])
            item = str(kwargs['value']['item'])
            
            sql = "select s._1,s._2,s._3,s._4 from s3object s where s._1='" + item + "'"
                        
            response = s3_client.select_object_content(
                Bucket=self.bucket,
                Key=kwargs['search_file_key'],
                ExpressionType='SQL',
                Expression=sql,
                InputSerialization={'CSV': {"FileHeaderInfo": "None"}},
                OutputSerialization={'CSV': {}},
                RequestProgress={ 'Enabled': False}
                )
            
            for event in response['Payload']:
                if 'Records' in event:
                    contents = event['Records']['Payload'].decode("utf-8")
                    
                    if contents:
                        if self.search_result:
                            self.search_result+= contents
                        else:
                            self.search_result = contents
                            
                elif 'Stats' in event:
                    statsDetails = event['Stats']['Details']
                    # Some information in case required fom statsDetails
                    #print("Stats details bytesScanned: ")
                    #print(statsDetails['BytesScanned'])
                    #print("Stats details bytesProcessed:")
                    #print(statsDetails['BytesProcessed'])
                
        except Exception as e:
            print(e)
        finally:
            t1 = time.time()
            if self.search_time:
                print("S3 Select {0} seconds".format(t1-t0))

    def process_threads(self, command_array):
        
        #print(command_array)
        threads = [threading.Thread(target=self._sqlS3, kwargs=command) for command in command_array]
        
        for thread in threads:
            self.search_result_count += 1
            thread.start()
        
        for thread in threads:
            thread.join()

    def list_s3(self, list_max_keys = False):
        
        t0 = time.time()               
        try:
            
            command_array = []
            max_keys = connection_pool
            if list_max_keys:
                max_keys = list_max_keys
            
            #print(self.search_items)
            for eitem in self.search_items:
    
                more_objects=True
                found_token = False
                prefix = eitem['key'] + self.partid
                
                while more_objects :
                    if found_token:
                        print("ContinuationToken :" + found_token)
                        response= s3_client.list_objects_v2(
                        Bucket=self.bucket,
                        ContinuationToken=found_token,
                        Prefix= prefix,
                        Delimiter="/",
                        MaxKeys=max_keys)
                      
                    else:
                        response= s3_client.list_objects_v2(
                        Bucket=self.bucket,
                        Prefix= prefix,
                        Delimiter="/",
                        MaxKeys=max_keys)

                    #print(response)
                    if 'Contents' in response:
                        for source in response["Contents"]:
                            search_file_key = source["Key"]
                            
                            command_array.append({'search_file_key': search_file_key, 'value': eitem['value']})
                    
                    # Now check there is more objects to list
                    if "NextContinuationToken" in response:
                        found_token = response["NextContinuationToken"]
                        more_objects = True
                    else:
                        more_objects = False
                        
            self.process_threads(command_array)
                
        except Exception as e:
            print(e)
        finally:
            t1 = time.time()
            #print("S3 Total Select {0} seconds".format(t1-t0))
            return self.search_result

    def process_csv_contents(self, contents):
    
        if contents:
            if self.display_content:
                print(contents)
             
            csv_content_string = io.StringIO(contents)
            csv.register_dialect('csvdialect', delimiter = ',', quotechar = '"', doublequote = True, skipinitialspace = True, lineterminator = '\n\r', quoting = csv.QUOTE_MINIMAL)
            csv_data = csv.reader(csv_content_string, dialect='csvdialect')
            
            rows = 0
            for row in csv_data:
                rows+=1
            
            return rows