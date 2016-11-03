import os


qsub_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "job_submission.pbs")
run_ensemble_map_and_aggregate_job_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                        "run_ensemble_map_aggregate.py")
run_ensemble_job_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "run_ensemble.py")
map_and_aggregate_job_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                           "map_and_aggregate.py")
molns_cloudpickle_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                       "molns_cloudpickle.py")
storage_providers_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                       "storage_providers.py")
molns_exceptions_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                      "molns_exceptions.py")
utils_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "utils.py")
molnstutil_init_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "__init__.py")
constants_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "constants.py")
job_input_file_name = "input"
pickled_cluster_input_file = "pickled-cluster-input-file"
job_output_file_name = "output"
job_complete_file_name = "complete"
job_error_file_name = "error"
qsub_job_name = "job.py"
local_storage = "Local"
persistent_storage = "Persistent"
shared_storage = "Shared"
