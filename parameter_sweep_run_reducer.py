def parameter_sweep_run_reducer(parameters, reducer, mapped_results):
    ret = ParameterSweepResultList()
    for param_set_id, param in enumerate(parameters):
        ret.append(ParameterSweepResult(reducer(mapped_results[param_set_id], parameters=param),
                                        parameters=param))
    return ret


class ParameterSweepResult:
    """TODO"""

    def __init__(self, result, parameters):
        self.result = result
        self.parameters = parameters

    def __str__(self):
        return "{0} => {1}".format(self.parameters, self.result)


class ParameterSweepResultList(list):
    def __str__(self):
        l = []
        for i in self:
            l.append(str(i))
        return "[{0}]".format(", ".join(l))


if __name__ == "__main__":
    import pickle
    import constants

    with open(constants.job_input_file_name, "rb") as inp:
        unpickled_list = pickle.load(inp)

    mapped_results = unpickled_list['mapped_results']

    with open("input", "rb") as inp:
            unpickled_cluster_input = pickle.load(inp)
            reducer = unpickled_cluster_input['reducer']

    try:
        result = parameter_sweep_run_reducer(reducer=reducer, mapped_results=mapped_results)

        with open("output", "wb") as output:
            pickle.dump(result, output)

    except Exception as errors:
        with open("error", "wb") as error:
            error.write(str(errors))
