#!/bin/bash
if [[ $# -lt 2 ]]; then
    echo 'Usage: debug.sh datasysnc_port debug_port'
    exit 1
fi

DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
source $DATASYSNC_HOME/bin/env.sh

pid=`ps -ef | grep name=$PROJECT | grep -v grep | awk '{print $2}'`
if [[ $pid ]]; then
    echo "${PROJECT} already started, stop it first!"
    exit 100
fi

cd $DATASYSNC_HOME

java -Dname=$PROJECT -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=$2 -jar $DATASYSNC_JAR
