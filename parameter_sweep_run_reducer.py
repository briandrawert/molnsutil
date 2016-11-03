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
