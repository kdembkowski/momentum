######################################################################
#
# Skrypt przenosi podaną listę plików z jednego bukietu S3 do innego
#
# Podczas nieoczekiwanego błędu podczas wykonywania skryptu nie
# dojdzie do utraty danych, typu usunięcie pliku bez uprzedniego
# przekopiowania go do bukietu docelowego.
#
######################################################################

import boto3
import sys
import subprocess
import json
from datetime import datetime
from multiprocessing import Pool
s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')


# parameters
fileExtension = '.mp4'
storageClass = 'STANDARD'
sourceBucket = 'myproduction-bucket'
destinationBucket = 'mytest-bucket'
pathPrefix = 'store/'
maxPerPageItemsNumber = 1000
numberOfConcurrentlyCopiedFiles = 16


# multiprocessing
def move_single_file(fileName):
    '''
    Helper function to move a single file
    '''
    
    sBucket = sourceBucket
    dBucket = destinationBucket
       
    try:
        s3.copy_object(
            Bucket = dBucket,
            Key = fileName,
                CopySource = {
                    'Bucket': sBucket,
                    'Key': fileName
                    }
            )
    
    except Exception as e:
        print(f"Error copying {sourceBucket} + {pathPrefix} + {fileName} to {destinationBucket} + {pathPrefix} + {fileName}: {e}")
    
    try:
        s3.delete_object(
            Bucket = sBucket,
            Key = fileName
    )
    
    except Exception as e:
        print(f"Error deleting {sourceBucket} + {pathPrefix} + {fileName} to {destinationBucket} + {pathPrefix} + {fileName}: {e}")


def move_files_concurrently(fileNames, numberOfWorkers):
    '''
    Function to copy multiple files using multiprocessing for faster execution.
    '''
    
    with Pool(processes = numberOfWorkers) as pool:
        pool.map(move_single_file, fileNames)
    
    print("All files moved successfully.")


#pagination
paginator = s3.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket = sourceBucket, Prefix = pathPrefix, PaginationConfig = {'PageSize': maxPerPageItemsNumber}):
    
    fileNames = []
    
    if 'Contents' in page:
        for objectS3 in page['Contents']:
            if ((objectS3['Key'].endswith(fileExtension)) and (objectS3['StorageClass'] == storageClass)):
                fileNames.append(objectS3['Key'])
    
    move_files_concurrently(fileNames, numberOfConcurrentlyCopiedFiles)
