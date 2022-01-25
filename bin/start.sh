#!/bin/bash

set -x

DATASYSNC_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
sh ${DATASYSNC_HOME}/bin/datasysnc.sh start
