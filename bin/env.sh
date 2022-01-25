#!/bin/bash
export PROJECT=datasysnc-`whoami`

DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
cd ${DATASYSNC_HOME}
LOG_DIR=${DATASYSNC_HOME}/logs

DATE=`date +%F`
export LOG_FILE=${LOG_DIR}/boot_${DATE}.log

if [ ! -d ${LOG_DIR} ]; then
    mkdir ${LOG_DIR}
fi

DATASYSNC_JAR=target/scala-2.13/pentagon-assembly-*.jar
if [ ! -f $DATASYSNC_JAR ]; then
    DATASYSNC_JAR=pentagon-assembly-0.1.0.jar
fi
export DATASYSNC_JAR
