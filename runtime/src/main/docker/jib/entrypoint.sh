
echo "starting "$@""
cmd=$1
if [ "$cmd" == "agent-runtime" ]; then
  exec java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -cp /app/resources/:/app/classes/:/app/libs/* "com.datastax.oss.sga.runtime.PodJavaRuntime" "$@"
elif [ "$cmd" == "deployer-runtime" ]; then
  exec java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -cp /app/resources/:/app/classes/:/app/libs/* "com.datastax.oss.sga.runtime.PodJavaRuntime" "$@"
fi

