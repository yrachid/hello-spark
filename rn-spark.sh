#!/bin/bash

cp sample.{csv,txt} /tmp
cp -r sample.parquet /tmp

sbt clean package && spark-submit \
  --master local[*] \
  --class ${1:-main.HelloSpark} \
  target/scala-2.11/data-transformations_2.11-0.1.jar

rm /tmp/sample.{csv,txt}
rm -r /tmp/sample.parquet
