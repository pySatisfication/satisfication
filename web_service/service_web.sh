#!/bin/bash

case $1 in
start)
    echo "starting web service..."
    pid=$(ps -ef | grep "uvicorn main:app" | grep -v grep | awk '{print $2}')
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
    uvicorn main:app --reload --host 192.168.1.200 --port 8000 > /opt/logs/web/main.log &
    ;;
status)
    pid=$(ps -ef | grep "uvicorn main:app" | grep -v grep | awk '{print $2}')
    pids=($pid)
    if [ ${#pids[@]} -gt 0 ]; then
        echo "service is running. the number of alive service is: ${#pids[@]}"
    else
        echo "service is not running. the number of alive service is: ${#pids[@]}"
    fi
    ;;
stop)
    pid=$(ps -ef | grep "uvicorn main:app" | grep -v grep | awk '{print $2}')
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
