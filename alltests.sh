#!/bin/bash

echo Testing $MONGOURI
rm *.log
for n in 10000 50000
do
 for i in 5 15
   do
    for s in 4000 12000
    do
   
      java -jar OrderBench.jar -u "$MONGOURI" -l -t 64 -s $s -n $n -i $i -z 20000 | tee "${n}-${i}-${s}.log"
    done
   done
done
