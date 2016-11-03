#!/usr/bin/env bash

# This script runs in the working directory of the reduce job and mounts pwd into the started container.
run_reducer()
{
  curr_dir=`pwd`
  molnsutil_dir=`dirname ${curr_dir}`

  COMMAND="sudo docker run -e PYTHONPATH=/stochss-master/app/lib -e STOCHKIT_HOME=/stochss-master/StochKit --volume ${curr_dir}:/stochss-master/app/lib/w_dir/ --volume ${molnsutil_dir}:/stochss-master/app/lib/molnsutil/ -w /stochss-master/app/lib/w_dir/ --name ${curr_dir} aviralcse/stochss_qsub python parameter_sweep_run_reducer.py"

  ${COMMAND}
  echo "molnsutil dir: ${molnsutil_dir}" > complete
}

# RUN
run_reducer

exit