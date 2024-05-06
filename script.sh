#!/bin/bash
kvsWorkers=1  # number of kvs workers to launch
flameWorkers=1  # number of flame workers to launch

rm -r worker1
rm *.jar

#javac -d classes --source-path src src/Indexer/jobs/*.java
#sleep 1
#jar cf indexer.jar classes/Indexer/jobs/indexer.class
#sleep 1
#jar cf pageRank.jar classes/Indexer/jobs/pageRank.class
#sleep 1

javac -cp src: src/*.java
sleep 1

javac -cp src: src/Webserver/*.java
sleep 1
javac -cp src: src/KVS/kvs/*.java
sleep 1

echo "cd $(pwd); java -cp src: KVS.kvs.Coordinator 8000" > kvscoordinator.sh
chmod +x kvscoordinator.sh
open -a Terminal kvscoordinator.sh

sleep 2

for i in `seq 1 $kvsWorkers`
do
    dir=worker$i
    if [ ! -d $dir ]
    then
        mkdir $dir
    fi
    echo "cd $(pwd); java -cp src: KVS.kvs.Worker $((8000+$i)) $dir 18.210.17.43:8000" > kvsworker$i.sh
    chmod +x kvsworker$i.sh
    open -a Terminal kvsworker$i.sh
done



#javac -cp lib/KVS.jar:lib/Webserver.jar:src sr
#sleep 1
#
#javac --source-path src src/KVS/kvs/*.java
#sleep 1
#
#javac --source-path src src/Flame/flame/*.java
#sleep 1
#
#javac --source-path src -d bin $(find src -name '*.java')

#echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar KVS.kvs.Coordinator 8000" > kvscoordinator.sh
#chmod +x kvscoordinator.sh
#open -a Terminal kvscoordinator.sh
#
#sleep 2
#
#for i in `seq 1 $kvsWorkers`
#do
#    dir=worker$i
#    if [ ! -d $dir ]
#    then
#        mkdir $dir
#    fi
#    echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar KVS.kvs.Worker $((8000+$i)) $dir localhost:8000" > kvsworker$i.sh
#    chmod +x kvsworker$i.sh
#    open -a Terminal kvsworker$i.sh
#done

#echo "cd $(pwd); java -cp bin: Flame.flame.Coordinator 9000 localhost:8000" > flamecoordinator.sh
#chmod +x flamecoordinator.sh
#open -a Terminal flamecoordinator.sh
#
#sleep 2
#
#for i in `seq 1 $flameWorkers`
#do
#    echo "cd $(pwd); java -cp bin: Flame.flame.Worker $((9000+$i)) localhost:9000" > flameworker$i.sh
#    chmod +x flameworker$i.sh
#    open -a Terminal flameworker$i.sh
#done