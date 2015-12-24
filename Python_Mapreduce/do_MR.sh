#!/bin/sh

## 删除本地文件
rm -f /home/hadoop/data_files/*

## python 获取远程数据文件
python get_remote_data.py

## 执行MR程序
hdfs dfs -rm -f /data/*
hadoop jar load-Data-4.6.jar  /data/kunyan_c  /output/kunyan6.2