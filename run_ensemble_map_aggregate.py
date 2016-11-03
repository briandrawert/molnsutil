import pickle

import constants
import molns_cloudpickle as cloudpickle
from molns_exceptions import MolnsUtilException
from utils import builtin_aggregator_list_append, create_model


def run_ensemble_map_and_aggregate(model_class, parameters, param_set_id, seed_base, number_of_trajectories, mapper,
                                   aggregator=None):
    """ Generate an ensemble, then run the mappers are aggregator.  This will not store the results. """

    if aggregator is None:
        aggregator = builtin_aggregator_list_append

    # Create the model
    model = create_model(model_class, parameters)

    # Run the solver
    res = None
    num_processed = 0
    results = model.run(seed=seed_base, number_of_trajectories=number_of_trajectories)
    if not isinstance(results, list):
        results = [results]

    for result in results:
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

    return {'result': res, 'param_set_id': param_set_id, 'num_successful': num_processed,
            'num_failed': number_of_trajectories - num_processed}


if __name__ == "__main__":
    with open(constants.job_input_file_name, "rb") as inp:
        unpickled_list = pickle.load(inp)

    num_of_trajectories = unpickled_list['chunk_size']
    seed = unpickled_list['seed']
    params = unpickled_list['pset']
    param_set_id_ = unpickled_list['pndx']

    if not unpickled_list.get('model_class', False):
        with open(constants.pickled_cluster_input_file, "rb") as inp:
            unpickled_cluster_input = pickle.load(inp)
            model_cls = unpickled_cluster_input['model_class']
            mapper_fn = unpickled_cluster_input['mapper']
            aggregator_fn = unpickled_cluster_input['aggregator']
    else:
        model_cls = unpickled_list['model_class']
        mapper_fn = unpickled_list['mapper']
        aggregator_fn = unpickled_list['aggregator']

    try:
        result = run_ensemble_map_and_aggregate(model_cls, params, param_set_id_, seed, num_of_trajectories,
                                                mapper_fn, aggregator_fn)
        with open(constants.job_output_file_name, "wb") as output:
            cloudpickle.dump(result, output)
    except MolnsUtilException as errors:
        with open(constants.job_error_file_name, "wb") as error:
            error.write(str(errors))
