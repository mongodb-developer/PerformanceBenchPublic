# SequernceServer

Install MAven

build with: mvn package
run with: Java -jar TSTest.jar
log in: TSTest.log or to screen 

usage: 


 java -jar TSTest.jar <uri> <readings per device> <nuber of threads>  <devices per thread>

 Results: 


 java -jar TSTest.jar mongodb://localhost 50000 10 10



 localhost (Macbook Pro, i7), MDB 5.0

use tstest

 db.readings.drop()
db.createCollection("readings");
db.readings.createIndex({sensor:1,ts:1})

 java -jar TSTest.jar mongodb://localhost 50000 10 10


16:39:10.531 [main] INFO  com.mongodb.jlp.tstest.TSTest - 69 - Time Taken 53s


 db.readings.drop()
db.createCollection("readings", {
  timeseries: {
    timeField: "ts",metaField: "sensor"
  },
});

17:04:30.610 [main] INFO  com.mongodb.jlp.tstest.TSTest - 69 - Time Taken 239149 ms

//Self Bucketing


db.readings.drop()
db.readings.createIndex({device:1,size:1})

08:32:56.186 [main] INFO  com.mongodb.jlp.tstest.TSTest - 69 - Time Taken 537179 ms




Testing M10 Cluster
------------------------

Client is Same AWS Region - 8 Cores, 16GB RAM, Java client is relatively efficient

11-13% CPU on cluient so server is limiting factor - server at 100% CPU, CPU limited


3000 Devices - 1,000 Readings per device.


 java -jar TSTest.jar  'mongodb+srv://mongoadmin:test123@cluster0.tzfdn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority' 1000 30 100 1

30 X 100,000 = 3m records

M10: 100% CPU

 Write Only

905245

52792ms

 Read Only

52792ms

 Read/Write

1253289 ms



