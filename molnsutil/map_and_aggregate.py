import pickle
import molns_cloudpickle as cloudpickle
from storage_providers import PersistentStorage, LocalStorage, SharedStorage
from utils import builtin_aggregator_list_append
from molns_exceptions import MolnsUtilException, MolnsUtilStorageException
import constants


def map_and_aggregate(results, param_set_id, mapper, aggregator=None, cache_results=False,
                      local_storage_directory=None):
    """ Reduces a list of results by applying the map function 'mapper'.
        When this function is applied on an engine, it will first
        look for the result object in the local ephemeral storage (cache),
        then in the Shared area (global non-persistent), then in the
        Object Store (global persistent).

        If cache_results=True, then result objects will be written
        to the local ephemeral storage (file cache), so subsequent
        postprocessing jobs may run faster.

        """

    # If local_storage_directory is provided, then use ONLY that directory.
    if local_storage_directory is not None:
        ls = LocalStorage(local_storage_directory)
    else:
        ps = PersistentStorage()
        ss = SharedStorage()
        ls = LocalStorage()

    if aggregator is None:
        aggregator = builtin_aggregator_list_append
    num_processed = 0
    res = None

    for i, filename in enumerate(results):
        enotes = ''
        result = None
        try:
            result = ls.get(filename)
        except Exception as e:
            enotes += "In fetching from local store, caught  {0}: {1}\n".format(type(e), e)
            if local_storage_directory is not None:
                raise MolnsUtilStorageException(enotes)

        if result is None:
            try:
                result = ss.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except Exception as e:
                enotes += "In fetching from shared store, caught  {0}: {1}\n".format(type(e), e)
        if result is None:
            try:
                result = ps.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except Exception as e:
                enotes += "In fetching from global store, caught  {0}: {1}\n".format(type(e), e)
        if result is None:
            notes = "Error could not find file '{0}' in storage\n".format(filename)
            notes += enotes
            raise MolnsUtilStorageException(notes)

        try:
            mapres = mapper(result)
            res = aggregator(mapres, res)
            num_processed += 1
        except Exception as e:
            notes = "Error running mapper and aggregator, caught {0}: {1}\n".format(type(e), e)
            notes += "type(mapper) = {0}\n".format(type(mapper))
            notes += "type(aggregator) = {0}\n".format(type(aggregator))
            notes += "dir={0}\n".format(dir())
            raise MolnsUtilException(notes)

    return {'result': res, 'param_set_id': param_set_id, 'num_sucessful': num_processed,
            'num_failed': len(results) - num_processed}


if __name__ == "__main__":
    import os
    with open(constants.job_input_file_name, "rb") as inp:
        unpickled_list = pickle.load(inp)

    results_ = unpickled_list[0]
    param_set_id_ = unpickled_list[1]
    mapper_fn = unpickled_list[2]
    aggregator_fn = unpickled_list[3]
    cache_results_ = unpickled_list[4]
    local_storage_directory_ = os.path.dirname(os.path.abspath(__file__))

    try:
        result = map_and_aggregate(results_, param_set_id_, mapper_fn, aggregator_fn, cache_results_,
                                   local_storage_directory_)
        with open(constants.job_output_file_name, "wb") as output:
            cloudpickle.dump(result, output)
    except MolnsUtilException as errors:
        with open(constants.job_error_file_name, "wb") as error:
            error.write(str(errors))
