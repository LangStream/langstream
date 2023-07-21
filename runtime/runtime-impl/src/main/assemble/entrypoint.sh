#!/bin/bash
exec java ${JAVA_OPTS} -cp "/app/lib/*" "com.datastax.oss.sga.runtime.Main" "$@"