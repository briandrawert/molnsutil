#!/usr/bin/env bash

# This script runs in the working directory of the reduce job and mounts pwd into the started container.
run_reducer()
{
  c_dir=`pwd`
  molnsutil_dir=`dirname ${c_dir}`

  COMMAND="sudo docker run -e PYTHONPATH=/stochss-master/app/lib -e STOCHKIT_HOME=/stochss-master/StochKit --volume ${c_dir}:/stochss-master/app/lib/w_dir/ --volume ${molnsutil_dir}:/stochss-master/app/lib/molnsutil/ -w /stochss-master/app/lib/w_dir/ --name `basename ${1}` aviralcse/stochss_qsub python parameter_sweep_run_reducer.py"

  ${COMMAND}
  echo "molnsutil dir: ${molnsutil_dir}" > complete
}

# RUN
run_reducer $1

exit