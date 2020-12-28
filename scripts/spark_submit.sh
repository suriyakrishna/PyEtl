export APPLICATION_DIRECTORY="/home/hadoop/pyetl/scripts"
export PY_FILES="${APPLICATION_DIRECTORY}/dist/pyetl-0.0.1-py2.7.egg"
export FILES="${APPLICATION_DIRECTORY}/config.json#config.json"
export MAIN_PY="${APPLICATION_DIRECTORY}/etl.py"

spark-submit --master yarn \
--deploy-mode cluster \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties \
--py-files ${PY_FILES} \
--files ${FILES} \
${MAIN_PY} -c config.json