
rm -f k_line_${1}.csv

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

python kline_handler.py --depth_source=../data/${1}.csv &
