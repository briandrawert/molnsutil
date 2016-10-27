import os
import pickle
import uuid

import constants
import molns_cloudpickle as cloudpickle
from molns_exceptions import MolnsUtilException
from storage_providers import PersistentStorage, LocalStorage, SharedStorage


def run_ensemble(model_class, parameters, param_set_id, seed_base, number_of_trajectories,
                 storage_mode=constants.shared_storage, local_storage_path=None):
    """ Generates an ensemble consisting of number_of_trajectories realizations by
        running the model 'nt' number of times. The resulting result objects
        are serialized and written to one of the MOLNs storage locations, each
        assigned a random filename. The default behavior is to write the
        files to the Shared storage location (global non-persistent). Optionally, files can be
        written to the Object Store (global persistent), storage_model="Persistent"

        Returns: a list of filenames for the serialized result objects.

        """

    if storage_mode == constants.shared_storage:
        storage = SharedStorage()
    elif storage_mode == constants.persistent_storage:
        storage = PersistentStorage()
    elif storage_mode == constants.local_storage:
        storage = LocalStorage(local_storage_path)
    else:
        raise MolnsUtilException("Unknown storage type '{0}'".format(storage_mode))

    # Create the model
    try:
        model_class_cls = cloudpickle.loads(model_class)
        if parameters is not None:
            model = model_class_cls(**parameters)
        else:
            model = model_class_cls()
    except Exception as e:
        notes = "Error instantiation the model class, caught {0}: {1}\n".format(type(e), e)
        notes += "dir={0}\n".format(dir())
        raise MolnsUtilException(notes)

    # Run the solver
    filenames = []
    notes = ""

    results = model.run(seed=seed_base, number_of_trajectories=number_of_trajectories)
    if not isinstance(results, list):
        results = [results]
    for result in results:
        try:
            # We should try to thread this to hide latency in file upload...
            filename = str(uuid.uuid1())
            storage.put(filename, result)
            filenames.append(filename)
        except Exception as e:
            notes += "Error writing result {0}. Error: {1}. \n\n".format(result, str(e))
            raise MolnsUtilException(notes)

    return {'filenames': filenames, 'param_set_id': param_set_id}


if __name__ == "__main__":
    with open(constants.job_input_file_name, "rb") as inp:
        unpickled_list = pickle.load(inp)

    num_of_trajectories = unpickled_list[0]
    seed = unpickled_list[1]
    model_cls = unpickled_list[2]
    params = unpickled_list[3]
    param_set_id_ = unpickled_list[4]
    storage_mode = unpickled_list[5]

    try:
        result = run_ensemble(model_cls, params, param_set_id_, seed, num_of_trajectories, storage_mode=storage_mode,
                              local_storage_path=os.path.dirname(os.path.abspath(__file__)))
        with open(constants.job_output_file_name, "wb") as output:
            cloudpickle.dump(result, output)
    except MolnsUtilException as errors:
        with open(constants.job_error_file_name, "wb") as error:
            error.write(str(errors))
