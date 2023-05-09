WordCountOutput="WordCountOutput"
WordCountOutputLocal="WordCountOutput"

cd src
rm *.class
rm *.jar
javac WordCount.java
jar -cvf ../WordCount.jar *.class
cd ..
hdfs dfs -rm -r -skipTrash $WordCountOutput
hadoop jar WordCount.jar WordCount test $WordCountOutput
echo -e "\e[1;32m"
hdfs dfs -cat $WordCountOutput/part-r-00000 > $WordCountOutputLocal
head -n 100 $WordCountOutputLocal
echo -e "\e[0m"
