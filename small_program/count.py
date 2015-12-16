#!/usr/bin/python
#coding: utf-8

file_path = "D:/WorkSpace/Python_workspace/Lengjing/data/"
data_file = open(file_path+'kunyan_2015101804')
a = []
for line in data_file:
    a.append(line.readlines())