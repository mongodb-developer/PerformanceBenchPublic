
Building from the command line
==============================

1) First time, run all these gradle commands:

  ./gradlew clean
  ./gradlew compileJava
  ./gradlew processResources
  ./gradlew classes
  ./gradlew jar
  ./gradle  startScripts
  ./gradlew distTar     
  ./gradlew distZip
  ./gradlew assemble
  ./gradlew build   


2) Subsequent builds (it works for me on MacOS)

  ./gradlew build


Building from NetBeans
======================

Click the "hammer + broom" icon


Running the program/binary
==========================

1) copy the file from your workspace to an oustide location, for example:

cd <temp_location>
cp /Users/danielcoupal/nobackup/repos/PerformanceBench/app/build/distributions/app.zip . ; unzip -o app.zip

2) then run with a given config file:

app/bin/app -c configSCsmall.json
