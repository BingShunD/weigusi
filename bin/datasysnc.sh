#!/bin/bash

# 定义变量
JAVA_HOME=/data/application/service/jdk
CLASSPATH=$CLASSPATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib
PATH=$PATH:$JAVA_HOME/bin:$JAVA_HOME/jre/bin:/data/application/service/redis/bin
export JAVA_HOME CLASSPATH PATH

DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
source ${DATASYSNC_HOME}/bin/env.sh
cd ${DATASYSNC_HOME}
pid=`ps -ef | grep name=${PROJECT} | grep -v grep | awk '{print $2}'`
port=` cat conf/application.conf| grep -A 4 server | grep port | awk -F "=" '{print $2}' | awk '{sub("^ *","");sub(" *$","");print}'`
JAR="datasysnc.jar"
EXECUTE_JAR=`ls -l $JAR | awk '{print $NF}'`


status(){
    if [[ $pid ]]
    then
        ps aux |grep -v "grep" |grep $pid
    else
        ps aux |grep -v "grep" |grep name=$PROJECT
    fi
}

start(){
    if [[ $pid ]]; then
        echo "${PROJECT} already started, stop it first!"
        exit 100
    fi
    cd $DATASYSNC_HOME
    retry=1
    while [[ $retry -lt 10 ]]
    do
        port_listen=`/usr/sbin/lsof -i:$port | grep LISTEN`
        if [[ $port_listen ]]; then
            echo "waiting for port $port available, $retry"
            sleep 1
            retry=`expr $retry + 1`
        else
            nohup java -Dname=$PROJECT -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:./gc.log -verbose:gc -jar $EXECUTE_JAR &>>${LOG_FILE} &
            break
        fi
    done
    sleep 2
    status
}

stop(){
    if [[ $pid ]]; then
        kill -TERM ${pid}
	sleep 5
        # 杀进程是耗时间的，等等吧⊙﹏⊙
        retry=1
        while [[ $retry -lt 10 ]]
        do
            pid=`ps -ef | grep name=$PROJECT | grep -v grep | awk '{print $2}'`
            if [[ $pid ]]; then
                echo "waiting for ${PROJECT} to exit, $retry"
                sleep 1
                retry=`expr $retry + 1`
            else
                echo "${PROJECT} stopped"
                break
            fi
        done
    else
        echo "${PROJECT} is not running"
    fi
}

restart(){
    stop
    start
}

case "$1" in
    start)
        start ;;
    stop)
        stop ;;
    status)
        status ;;
    restart)
        restart ;;
    *)
        echo $"Usage: $0 {start|stop|restart|status}"
        exit 2
esac

