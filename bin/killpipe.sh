#! /bin/bash
#
# set -x
prog=`basename $0`
mpiexeccmd=mpiexec
mpdcmd=mpd
runid=$1
if [ -z "$runid" ]; then 
   runid=$mpiexeccmd
fi

pypids=`pgrep python`
pypids=`echo $pypids | sed -e 's/ /,/g'`
if [ -n "$pypids" ]; then
    if [ -n "$runid" -a -n "$pypids" ]; then
        mpiexecpids=`ps -uww -p $pypids | grep -v killPipeline | grep $runid | awk '{print $2}'`
    fi

    if [ -n "$mpiexecpids" ]; then
        kill $mpiexecpids
    fi

    mpdpids=`pgrep -P $pypids -f $mpdcmd`
    if [ -n "$mpdpids" ];then
        kill $mpdpids
    fi  
fi
