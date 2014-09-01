""" 
  Utility module for MOLNs^2. 
  
  molnsutil contains implementations of a persisitent storage API for 
  staging objects to an Object Store in the clouds supported by MOLNs^2. 
  This can be used in MOLNs^2 to write variables that are presistent
  between sessions, provides a convenetient way to get data out of the
  system, and it also provides a means during parallel computations to 
  stage data so that it is visible to all compute engines, in contrast
  to using the local scratch space on the engines.

  molnsutilalso contains parallel implementations of common Monte Carlo computational
  workflows, such as the generaion of ensembles and esitmation of moments.
  
"""


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

#     s3.json is a JSON file that contains the follwing info:
#
#     'aws_access_key_id' : AWS access key
#     'aws_secret_access_key' : AWS private key
#   s3.json needs to be created and put in .molns/s3.json in the root of the home directory. 

import os
fh = open(os.environ['HOME']+'/.molns/s3.json','r')
s3config = json.loads(fh.read())


class PersistentStorage():
    """
       Provides an abstaction for interacting with the Object Stores
       of the supported clouds.
    """

    def __init__(self, bucket_name=None):
        
        self.s3 = S3Connection(#is_secure=False,
                               #port=8888,
                               #host=s3config['HOST'],
                               aws_access_key_id=s3config['aws_access_key_id'],
                               aws_secret_access_key=s3config['aws_secret_access_key'],
                               calling_format='boto.s3.connection.OrdinaryCallingFormat',
                               #**s3config
                               )
                           

        self.bucket_name = bucket_name
        if self.bucket_name is None:
            # try reading it from the config file
            try:
                self.bucket_name = s3config['bucket_name']
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
                    raise PersistentStorageException("Failed to create/set bucket in the object store."+str(e))
                        
        self.bucket = bucket

    def put(self, name, data):
        k = Key(self.bucket)
        if not k:
            raise PersistentStorageException("Could not obtain key in the global store. ")
        k.key = name
        try:
            num_bytes = k.set_contents_from_string(pickle.dumps(data))
            if num_bytes == 0:
                raise PersistentStorageException("No bytes written to key.")
        except Exception, e:
            return {'status':'failed', 'error':str(e)}
        return {'status':'success', 'num_bytes':num_bytes}

    def get(self, name, validate=False):
        k = Key(self.bucket,validate)
        k.key = name
        try:
            obj = pickle.loads(k.get_contents_as_string())
        except boto.exception.S3ResponseError, e:
            raise PersistentStorageException("Could not retrive object from the datastore."+str(e))
        return obj

    def delete(self, name):
        k = Key(self.bucket)
        k.key = name
        self.bucket.delete_key(k)

    def delete_all(self):
        for k in self.bucket.list():
            self.bucket.delete_key(k.key) 



class DistributedEnsemble():
    """ A distributed ensemble based on a pyurdme model. """

    def __init__(self, model=None, number_of_realizations=1, persistent=False):
        """ """
        self.model = model
        self.number_of_realizations = number_of_realizations
        self.persistent = persistent
    
    def set_model(model):
        self.model = model

    def add_realizations(self, number_of_realizations=1):
        """ Add a number of realizations to the ensemble. """

    def mean(self, g=None, number_of_realizations=None):
        """ Compute the mean of the function g(X) based on number_of_realizations realizations
            in the ensemble. """
        
        mean_value = self.moment(function_handle, number_of_realizations=number_of_realizations)

    def variance(self, g=None, number_of_realizations=None):
        """ Compute the variance (second order central moment) of the function g(X) based on number_of_realizations realizations
            in the ensemble. """

    def moment(self, g=None, order=1, number_of_realizations=None):
        """ Compute the moment of order 'order' of g(X), using number_of_realizations
            realizations in the ensemble. """

    def density(self, g=None, number_of_realizations=None):
        """ Estimate the probability density function of g(X) based on number_of_realizations realizations
            in the ensemble. """



class PersistentStorageException(Exception):
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
