#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
index_date=`date -d "$1" +%Y%m%d`
dt=`date -d "$1" +%Y-%m-%d`

echo $index_date
echo $dt



##流量明细索引模板
flowTemplate=ads_polaris_brand_fans_stats_v1

curl -XDELETE " http://ads-es1.jd.local:9200/${flowTemplate}"
curl -XPUT " http://ads-es1.jd.local:9200/${flowTemplate}"

  spark-submit --class es.support.dataf.Fans2es \
    --master yarn \
    --driver-memory 8G \
    --num-executors 200 \
    --executor-cores 4 \
    --queue root.bdp_jmart_ad.jd_ad_dev \
    --executor-memory 16G \
    --conf spark.sql.shuffle.partitions=1600 \
    --conf spark.default.parallelism=1600 \
    --conf spark.blacklist.enabled=true \
    --conf spark.speculation=true \
    $basepath/esSupport-1.0-SNAPSHOT-jar-with-dependencies.jar $dt  > spark.log