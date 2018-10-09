#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize$queries$UseIndex
#mkdir $directory
echo $outputLocation/
#cp $leadDir/* $directory/
echo $leadDir/
echo $directory/

#latestProp=$directory/latestProp.props

echo $SnappyData
