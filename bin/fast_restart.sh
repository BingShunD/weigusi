#!/usr/bin/env bash

DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
source $DATASYSNC_HOME/bin/env.sh
cd $DATASYSNC_HOME

pid=`ps -ef | grep name=$PROJECT | grep -v grep | awk '{print $2}'`
nohup kill -TERM ${pid} &

port=` cat conf/application.conf| grep -A 4 server | grep port | awk -F "=" '{print $2}' | awk '{sub("^ *","");sub(" *$","");print}'`

JAR="datasysnc-assembly-0.1.0.jar"
EXECUTE_JAR=`ls -l $JAR | awk '{print $NF}'`
retry=1
while [[ true ]]
do
    #port_listen=`netstat -apnt | grep $port | grep LISTEN`
    port_listen=`lsof -i:$port | grep LISTEN`
    if [[ ! $port_listen ]]; then
        nohup java -Dname=$PROJECT -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:./gc.log -verbose:gc -Duser.timezone=GMT+08 -jar $EXECUTE_JAR >> ${LOG_FILE} 2>&1 &
        break
    else
        echo "waiting for port $port available, $retry"
        sleep 1
        retry=`expr $retry + 1`
    fi
done


pid=`ps -ef | grep name=$PROJECT | grep -v grep | awk '{print $2}'`
if [[ ! $pid ]]; then
    echo "Pentagon start failed!"
    exit 100
fi
