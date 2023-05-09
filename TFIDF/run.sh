TFIDFOutput="TFIDFOutput"
TFIDFOutputLocal="TFIDFOutput"
intermediateDirectory="TFIDF_temp"
inputDirectory="/data/exp2"

cd src
rm *.class
rm *.jar
javac TFIDF.java
jar -cvf ../TFIDF.jar *.class
cd ..
hdfs dfs -rm -r -skipTrash $TFIDFOutput $intermediateDirectory
hadoop jar TFIDF.jar TFIDF $inputDirectory $intermediateDirectory $TFIDFOutput
echo -e "\e[1;32m"
hdfs dfs -cat $TFIDFOutput/part-r-00000 > $TFIDFOutputLocal
head -n 100 $TFIDFOutputLocal
echo -e "\e[0m"
