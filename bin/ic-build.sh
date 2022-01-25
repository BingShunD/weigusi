#!/usr/bin/env bash

CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $CURDIR

HOME=$CURDIR/..

BRANCH=$(git branch | grep "*")
BRANCH=${BRANCH:2}
echo $BRANCH

cd $HOME

sh bin/assembly

mkdir -p $HOME/target/bdp/pentagon-$BRANCH
mkdir -p $HOME/target/bdp/pentagon-$BRANCH/bin
mkdir -p $HOME/target/bdp/pentagon-$BRANCH/conf
mkdir -p $HOME/target/bdp/pentagon-$BRANCH/logs

mv $HOME/datasysnc*.jar $HOME/target/bdp/datasysnc-$BRANCH
if [ `unzip -l $BASEDIR/target/bdp/mobius-$BRANCH/datasysnc-assembly-0.1.0.jar | grep "datasysnc-version-info.properties" | wc -l` -eq 0 ];then
  echo "no datasysnc-version-info.properties file found"
  # 手动创建version文件
  mkdir tempfolder
  unzip -d tempfolder $BASEDIR/target/bdp/mobius-$BRANCH/datasysnc-assembly-0.1.0.jar >/dev/null
  sh ${BASEDIR}/tools/datasysnc-build-info tempfolder/ '1.0'
  jar cvf datasysnc-assembly-0.1.0.jar -C tempfolder/ . >/dev/null
  mv mobius_2.11-1.0.jar $BASEDIR/target/bdp/mobius-$BRANCH/datasysnc-assembly-0.1.0.jar
  rm -rf tempfolder
  rm -f datasysnc-assembly-0.1.0.jar
fi

cp $HOME/conf/ic.conf  $HOME/target/bdp/datasysnc-$BRANCH/conf/application.conf
cp $HOME/conf/log4j.properties $HOME/target/bdp/datasysnc-$BRANCH/conf/

cp $HOME/bin/{datasysnc.sh,restart.sh,start.sh,stop.sh,env.sh,upgrade.sh} $HOME/target/bdp/datasysnc-$BRANCH/bin


cd $HOME/target/bdp/
tar zcvf datasysnc-$BRANCH.tar.gz datasysnc-$BRANCH