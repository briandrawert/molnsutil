#! /usr/bin/python
import pickle
import molns_cloudpickle

with open("input", "rb") as inp:
    unpickled_list = pickle.load(inp)

number_of_trajectories = unpickled_list[0]
seed = unpickled_list[1]
model = unpickled_list[2]
mapper_fn = unpickled_list[3]
aggregator_fn = unpickled_list[4]

results = model.run(number_of_trajectories=number_of_trajectories, seed=seed, show_labels=True)
if not isinstance(results, list):
    results = [results]

res = None
mapped_list = []
notes = ""

for r in results:
    try:
        mapped_list.extend(aggregator_fn(mapper_fn(r), res))
    except Exception as e:
        notes = "Error running mapper and aggregator, caught {0}: {1}\n".format(type(e), e)
        notes += "type(mapper) = {0}\n".format(type(mapper_fn))
        notes += "type(aggregator) = {0}\n".format(type(aggregator_fn))
        notes += "dir={0}\n".format(dir())


with open("output", "wb") as output:
    molns_cloudpickle.dump(mapped_list, output)

with open("error", "wb") as error:
    error.write(notes)
