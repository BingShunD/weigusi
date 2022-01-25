#!/usr/bin/env bash

set -x
DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
cd $DATASYSNC_HOME

if [[ ! $1 ]]; then
    echo "please specify a jar file. For example:   sh upgrade.sh datasysnc-20160816.jar"
    exit 101
fi

if [[ -f $1 ]]; then
    JAR="datasysnc.jar"
    rm $JAR
    ln -s $1 $JAR

    sh ${DATASYSNC_HOME}/bin/fast_restart.sh
else
    echo "invalid file -> $1"
fi
