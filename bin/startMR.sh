#!/bin/bash
java -cp mymr-1.0.jar com.ksc.wordcount.thrift.thriftserver > /dev/null 2>&1 &

file_path="master.conf"

if [ ! -f "$file_path" ]; then
  echo "配置文件不存在: $file_path"
  exit 1
fi

if [ ! -s "$file_path" ]; then
  echo "配置文件为空: $file_path"
  exit 1
fi

while IFS= read -r line || [[ -n "$line" ]]; do
  # 跳过注释行和空行
  if [[ $line == \#* ]] || [[ -z $line ]]; then
    continue
  fi

  echo "读取行: $line"

  # 使用空格作为分隔符将行内容分割为数组
  data=($line)
  echo "第一个元素: ${data[0]}"
  echo "第二个元素: ${data[1]}"
  java -cp mymr-1.0.jar com.ksc.wordcount.driver.MergeURLTopNDriver > /dev/null 2>&1 &

  # 遍历解析出的数据
  for item in "${data[@]}"; do
    echo "解析数据: $item"
  done

  echo "------------------"
done < "$file_path"

file_path="slave.conf"

if [ ! -f "$file_path" ]; then
  echo "配置文件不存在: $file_path"
  exit 1
fi

if [ ! -s "$file_path" ]; then
  echo "配置文件为空: $file_path"
  exit 1
fi

while IFS= read -r line || [[ -n "$line" ]]; do
  # 跳过注释行和空行
  if [[ $line == \#* ]] || [[ -z $line ]]; then
    continue
  fi

  echo "读取行: $line"

  # 使用空格作为分隔符将行内容分割为数组
  data1=($line)
  echo "第一个元素: ${data1[0]}"
  echo "第二个元素: ${data1[1]}"
  echo "第二个元素: ${data1[4]}"

  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
  echo "脚本文件所在目录: $script_dir"
  # SSH连接到目标主机并切换到目录
  ssh "${data1[0]}" "cd $script_dir && echo '切换到目录：$script_dir' && java -cp mymr-1.0.jar com.ksc.wordcount.worker.Executor ${data1[0]} ${data1[1]} ${data1[2]} ${data1[3]} ${data1[4]}" > /dev/null 2>&1 &
  java -cp mymr-1.0.jar com.ksc.wordcount.worker.Executor ${data1[0]} ${data1[1]} ${data1[2]} ${data1[3]} ${data1[4]} > /dev/null 2>&1 &

  # 遍历解析出的数据
  for item in "${data1[@]}"; do
    echo "解析数据: $item"
  done
  echo "------------------"
done < "$file_path"
