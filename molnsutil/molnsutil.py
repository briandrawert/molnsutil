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

import swiftclient.client


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

env = os.environ

class LocalStorage():
    """ This class provides an abstraction for storing and reading objects on/from
        the ephemeral storage. """
    
    def __init__(self):
	self.folder_name = "/home/ubuntu/localarea"
	
    def put(self, filename, data):
        with open(self.folder_name+"/"+filename,'wb') as fh:
            fh.write(pickle.dumps(data))

    def get(self, filename):
        with open(self.folder_name+"/"+filename, 'rb') as fh:
            data = pickle.load(fh)
        return data

    def delete(self,filename):
        os.remove(self.folder_name+"/"+filename)


class S3Provider():
    def __init__(self, bucket_name):
        self.connection = S3Connection(is_secure=False,
                                 calling_format='boto.s3.connection.OrdinaryCallingFormat',
                                 **s3config['credentials']
                                 )
        self.set_bucket(bucket_name)
    
    def set_bucket(self,bucket_name=None):
        if not bucket_name:
            self.bucket_name = "molns_bucket_{0}".format(str(uuid.uuid1()))
            bucket = self.provider.create_bucket(self.bucket_name)
        else:
            self.bucket_name = bucket_name
            try:
                bucket = self.connection.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.provider.create_bucket(bucket_name)
                except Exception, e:
                    raise PersistentStorageException("Failed to create/set bucket in the object store."+str(e))
        
            self.bucket = bucket

    def create_bucket(self,bucket_name):
        return self.connection.create_bucket(bucket_name)

    def put(self, name, data):
        k = Key(self.bucket)
        if not k:
            raise PersistentStorageException("Could not obtain key in the global store. ")
        k.key = name
        try:
            num_bytes = k.set_contents_from_string(data)
            if num_bytes == 0:
                raise PersistentStorageException("No bytes written to key.")
        except Exception, e:
            return {'status':'failed', 'error':str(e)}
        return {'status':'success', 'num_bytes':num_bytes}

    def get(self, name, validate=False):
        k = Key(self.bucket,validate)
        k.key = name
        try:
            obj = k.get_contents_as_string()
        except boto.exception.S3ResponseError, e:
            raise PersistentStorageException("Could not retrive object from the datastore."+str(e))
        return obj

    def delete(self, name):
        """ Delete an object. """
        k = Key(self.bucket)
        k.key = name
        self.bucket.delete_key(k)


    def delete_all(self):
        """ Delete all objects in the global storage area. """
        for k in self.bucket.list():
            self.bucket.delete_key(k.key)

    def list(self):
        """ List all containers. """
        return self.bucket.list()







class SwiftProvider():
    def __init__(self, bucket_name):
        self.connection = swiftclient.client.Connection(auth_version=2.0,**s3config['credentials'])
        self.set_bucket(bucket_name)
    
    def set_bucket(self,bucket_name):
        self.bucket_name = bucket_name
        if not bucket_name:
            self.bucket_name = "molns_bucket_{0}".format(str(uuid.uuid1()))
            bucket = self.provider.create_bucket(self.bucket_name)
        else:
            self.bucket_name = bucket_name
            try:
                bucket = self.connection.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.create_bucket(bucket_name)
                except Exception, e:
                    raise PersistentStorageException("Failed to create/set bucket in the object store."+str(e))
            
            self.bucket = bucket


    def create_bucket(self, bucket_name):
        bucket = self.connection.put_container(bucket_name)
        return bucket

    def get_all_buckets(self):
        """ List all bucket in this provider. """

    def put(self, object_name, data):
        self.connection.put_object(self.bucket_name, object_name, data)

    def get(self, object_name, validate=False):
        (response, obj) = self.connection.get_object(self.bucket_name, object_name)
        return obj

    def delete(self, object_name):
        self.connection.delete_object(self.bucket_name, object_name)

    def delete_all(self):
        print self.connection.head_container(self.bucket_name)

    def list(self):
        """ TODO: implement. """
#print self.connection.get_container()



class PersistentStorage():
    """
       Provides an abstaction for interacting with the Object Stores
       of the supported clouds.
    """

    def __init__(self, bucket_name=None):
        #print s3config['credentials']
        
        if bucket_name is None:
            # try reading it from the config file
            try:
                bucket_name = s3config['bucket_name']
            except:
                pass
    
        if s3config['provider_type'] == 'EC2':
            self.provider = S3Provider(bucket_name)
        # self.provider = S3Provider()
        elif s3config['provider_type'] == 'OpenStack':
            self.provider = SwiftProvider(bucket_name)
        else:
            raise PersistentStorageException("Unknown provider type.")
        

    def list_buckets(self):
        all_buckets=self.provider.get_all_buckets()
        buckets = []
        for bucket in all_buckets:
            buckets.append(bucket.name)
        return buckets

    def set_bucket(self,bucket_name=None):
        if not bucket_name:
            bucket = self.provider.create_bucket("molns_bucket_{0}".format(str(uuid.uuid1())))
        else:
            try:
                bucket = self.provider.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.provider.create_bucket(bucket_name)
                except Exception, e:
                    raise PersistentStorageException("Failed to create/set bucket in the object store."+str(e))
                        
        self.bucket = bucket

    def put(self, name, data):
        self.provider.put(name, pickle.dumps(data))
    
    
    def get(self, name, validate=False):
        return pickle.loads(self.provider.get(name, validate))
    
    def delete(self, name):
        """ Delete an object. """
        self.provider.delete(name)
    
    def list(self):
        """ List all containers. """
        return self.provider.list()

    def delete_all(self):
        """ Delete all objects in the global storage area. """
        self.provider.delete_all()



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
    
    ga = PersistentStorage()
    #print ga.list_buckets()
    ga.put('testtest.pyb',"fdkjshfkjdshfjdhsfkjhsdkjfhdskjf")
    print ga.get('testtest.pyb') 
    ga.delete('testtest.pyb')
    ga.list()
    ga.put('file1', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.delete_all()
