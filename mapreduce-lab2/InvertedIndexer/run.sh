InvertedIndexerOutput="InvertedIndexerOutput"
InvertedIndexerOutputLocal="InvertedIndexerOutput"

cd src
rm *.class
rm *.jar
javac InvertedIndexer.java
jar -cvf ../InvertedIndexer.jar *.class

cd ..

hadoop dfsadmin -safemode leave 
hdfs dfs -rm -r -skipTrash $InvertedIndexerOutput
hadoop jar InvertedIndexer.jar InvertedIndexer test $InvertedIndexerOutput

echo -e "\e[1;32m"
hdfs dfs -cat $InvertedIndexerOutput/part-r-00000 > $InvertedIndexerOutputLocal
head -n 100 $InvertedIndexerOutputLocal
echo -e "\e[0m"
