#!/bin/bash

case $1 in
start)
    echo "starting kline service..."
    pid=$(ps -ef | grep "python kline_handler" | grep -v grep | awk '{print $2}')
    for i in ${pid[@]}; do
      echo "kill -9 $i"
      kill -9 $i
      if [ $? -eq 0 ]; then
        echo "$i kill success"
      else
        echo "kill failed"
        exit 1
      fi
    done
    
    # restart
    echo "----------"
    echo $(date)
    nohup python kline_handler.py --depth_source=kafka > /opt/logs/kline/main.log 2>&1 &
    ;;
status)
    pid=$(ps -ef | grep "python kline_handler" | grep -v grep | awk '{print $2}')
    pids=($pid)
    if [ ${#pids[@]} -gt 0 ]; then
        echo "service is running. the number of alive service is: ${#pids[@]}"
    else
        echo "service is not running. the number of alive service is: ${#pids[@]}"
    fi
    ;;
stop)
    pid=$(ps -ef | grep "python kline_handler" | grep -v grep | awk '{print $2}')
    for i in ${pid[@]}; do
      echo "kill -9 $i"
      kill -9 $i
      if [ $? -eq 0 ]; then
        echo "$i kill success"
      else
        echo "kill failed"
        exit 1
      fi
    done
    ;;
*)
    echo "Usage: $0 {start|stop}" >&2
esac
