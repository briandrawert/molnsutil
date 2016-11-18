#!/usr/bin/env bash

# This script runs in the working directory of the reduce job and mounts script directory into the started container.
run_reducer()
{
  job_working_directory=`pwd`
  c_dir=$2
  molnsutil_dir=`dirname ${c_dir}`
  container_name=$1

  COMMAND1="docker pull aviralcse/stochss_qsub"
  COMMAND2="docker run -e PYTHONPATH=/stochss-master/app/lib -e STOCHKIT_HOME=/stochss-master/StochKit -e C_INCLUDE_PATH=/usr/lib/openmpi/include -e INSTANT_SYSTEM_CALL_METHOD=SUBPROCESS --volume ${c_dir}:/stochss-master/app/lib/w_dir/ --volume ${molnsutil_dir}:/stochss-master/app/lib/molnsutil/ -w /stochss-master/app/lib/w_dir/ --name ${container_name} aviralcse/stochss_qsub python parameter_sweep_run_reducer.py ${job_working_directory}"

  ${COMMAND1}
  ${COMMAND2}
  echo ${COMMAND2} > ${c_dir}/complete

  DEL_COMMAND="docker rm -f ${container_name}"

  ${DEL_COMMAND}
}

# RUN
run_reducer $1 $2

exit