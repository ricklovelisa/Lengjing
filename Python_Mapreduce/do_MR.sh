#!/bin/sh

## ɾ�������ļ�
rm -f /home/hadoop/data_files/*

## python ��ȡԶ�������ļ�
python get_remote_data.py

## ִ��MR����
hdfs dfs -rm -f /data/*
hadoop jar load-Data-4.6.jar  /data/kunyan_c  /output/kunyan6.2