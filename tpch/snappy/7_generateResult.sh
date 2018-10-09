#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries$UseIndex
mkdir $directory
leadDir=/mnt/dlequoc/snappydata-0.9-bin/work/stream10-lead-1
cp $leadDir/*.out $directory/

latestProp=$directory/latestProp.props

cd $SnappyData
#cd ../../..
#echo snappyData = $(git rev-parse HEAD)_$(git log -1 --format=%cd) > $latestProp
#cd spark
#echo snappy-spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
#cd ../store
#echo snappy-store = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
#cd ../aqp
#echo snappy-aqp = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
#cd ../spark-jobserver
#echo spark-jobserver = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp
cd $SnappyData/benchmark/snappy/

echo SPARK_PROPERTIES = $sparkProperties >> $latestProp
echo SPARK_SQL_PROPERTIES = $sparkSqlProperties >> $latestProp
echo ServerMemory = $serverMemory >> $latestProp
echo LineItem_Order_NoOfBuckets = $buckets_Order_Lineitem >> $latestProp
echo Cutomer_Part_PartSupp_NoOfBuckets = $buckets_Cust_Part_PartSupp >> $latestProp
echo IsColumn_Nation_Region_Supp = $Nation_Region_Supp_col >> $latestProp
echo Nation_Region_Supp_NoOfBuckets = $buckets_Nation_Region_Supp >> $latestProp
echo UseIndex = $UseIndex >> $latestProp
echo DataSize = $dataSize >> $latestProp
echo WarmUp = $WarmupRuns >> $latestProp
echo AverageRuns = $AverageRuns >> $latestProp
echo LOCATOR = $locator >> $latestProp
echo LEAD = $leads >> $latestProp
for element in "${servers[@]}";
  do
       echo SERVERS = $element >> $latestProp
  done


for i in $directory/*.out
do
   cat $latestProp >> $i
done



echo "******************Performance Result Generated*****************"

