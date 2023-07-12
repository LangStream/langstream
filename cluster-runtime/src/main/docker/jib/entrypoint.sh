
echo "running.."
ls -la /app/resources/
ls -la /app/classes/
ls -la /app/libs/
exec java ${JAVA_OPTS} -XX:+AlwaysPreTouch -Djava.security.egd=file:/dev/./urandom -cp /app/resources/:/app/classes/:/app/libs/* "com.datastax.oss.sga.cluster.runtime.ClusterRuntimeRunner" "$@"