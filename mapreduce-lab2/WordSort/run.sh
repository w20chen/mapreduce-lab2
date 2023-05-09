WordSortOutput="WordSortOutput"
WordSortOutputLocal="WordSortOutput"
WordSortInput="InvertedIndexerOutput"

cd src
rm *.class
rm *.jar
javac WordSort.java
jar -cvf ../WordSort.jar *.class

cd ..

hadoop dfsadmin -safemode leave 
hdfs dfs -rm -r -skipTrash $WordSortOutput
hadoop jar WordSort.jar WordSort $WordSortInput $WordSortOutput

echo -e "\e[1;32m"
hdfs dfs -cat $WordSortOutput/part-r-00000 > $WordSortOutputLocal
head -n 100 $WordSortOutputLocal
echo -e "\e[0m"
