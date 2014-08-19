import boto
import boto.ec2
from os import environ
import logging
from boto.s3.connection import S3Connection
logging.basicConfig(filename="boto.log", level=logging.DEBUG)
from boto.s3.key import Key
import uuid

try:
    import dill as pickle
except:
    import pickle

import json

""" 
    s3.json is a JSON file that contains the follwing info:
    
    'HOST' : The hostname for the s3 API endpoint
     EC2_ACCESS_KEY' : AWS access key
     EC2_SECRET_KEY' : AWS private key
    
"""


class PersistentStorage():

    def __init__(self, bucket_name=None):
       	fh = open('.molns/s3.json','r')
        s3config = json.load(fh)
        self.s3 = S3Connection(is_secure=False,
                               port=8888,
                               host=s3config['HOST'],
                               aws_access_key_id=s3config['EC2_ACCESS_KEY'],
                               aws_secret_access_key=s3config['EC2_SECRET_KEY'],
                               calling_format='boto.s3.connection.OrdinaryCallingFormat'
                               )
                           

        self.bucket_name = bucket_name
        if self.bucket_name is None:
            # try reading it from the config file
            try:
                self.bucket_name = s3config['BUCKET_NAME']
            except:
                pass
        self.set_bucket(self.bucket_name)
	
    def list_buckets(self):
        all_buckets=self.s3.get_all_buckets()
        buckets = []
        for bucket in all_buckets:
            buckets.append(bucket.name)
        return buckets

    def set_bucket(self,bucket_name=None):
        if not bucket_name:
            bucket = self.s3.create_bucket("molns_bucket_{0}".format(str(uuid.uuid1())))
        else:
            try:
                bucket = self.s3.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.s3.create_bucket(bucket_name)
                except Exception, e:
                    raise
        self.bucket = bucket

    def put(self, name, data):
        k = Key(self.bucket)
        if not k:
            raise GlobalStoreException("Could not obtain key in the global store. ")
        k.key = name
        try:
            num_bytes = k.set_contents_from_string(pickle.dumps(data))
            if num_bytes == 0:
                raise GlobalStoreException("No bytes written to key.")
        except Exception, e:
            return {'status':'failed', 'error':str(e)}
        return {'status':'success'}

    def get(self, name, validate=False):
        k = Key(self.bucket,validate)
        k.key = name
        try:
            obj = pickle.loads(k.get_contents_as_string())
        except boto.exception.S3ResponseError, e:
            raise GlobalStoreException("Could not retrive object from the datastore."+str(e))
        return obj

    def delete(self, name):
        k = Key(self.bucket)
        k.key = name
        self.bucket.delete_key(k)

    def delete_all(self):
        for k in self.bucket.list():
            self.bucket.delete_key(k.key) 

class GlobalStoreException(Exception):
    pass
	

if __name__ == '__main__':

    ga = PersistentStorage('myglobalarea')
    print ga.list_buckets()
    ga.put('testtest.pyb',"fdkjshfkjdshfjdhsfkjhsdkjfhdskjf")
    print ga.get('testtest.pyb') 
    ga.delete('testtest.pyb')
    ga.put('file1', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.delete_all()
