EXAMPLE_DIR = /user/$(USER)/final_project
INPUT_DIR   = $(EXAMPLE_DIR)/input
OUTPUT_DIR  = $(EXAMPLE_DIR)/output
OUTPUT_FILE = $(OUTPUT_DIR)/part-r-00000


CLASSPATH=$(shell hadoop classpath)

run: dsu_mapreduce.jar inputs
	-hdfs dfs -rm -f -r $(OUTPUT_DIR)
	hadoop jar dsu_mapreduce.jar dsu_mapreduce \
	       $(INPUT_DIR) $(OUTPUT_DIR)
	hdfs dfs -cat $(EXAMPLE_DIR)/output/"*"

dsu_mapreduce.jar: dsu_mapreduce.java
	mkdir -p dsu_mapreduce_classes
	javac -classpath $(CLASSPATH) -Xlint:deprecation \
 	      -d dsu_mapreduce_classes dsu_mapreduce.java
	jar -cvf dsu_mapreduce.jar -C dsu_mapreduce_classes . 


inputdir:
	hdfs dfs -test -e $(EXAMPLE_DIR) || hdfs dfs -mkdir -p $(EXAMPLE_DIR)
	hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir -p $(INPUT_DIR)

outputdir:
	hdfs dfs -test -e $(OUTPUT_DIR) || hdfs dfs -mkdir -p $(OUTPUT_DIR)

inputs: inputdir
	hdfs dfs -test -e $(INPUT_DIR)/edgeData.txt \
	  || hdfs dfs -put ./input/edgeData.txt $(INPUT_DIR)

clean:
	-rm *.jar
	-rm -r *_classes
	-hdfs dfs -rm -f -r $(INPUT_DIR)
	-hdfs dfs -rm -f -r $(OUTPUT_DIR)
	-hdfs dfs -rm -f -r $(EXAMPLE_DIR)

.PHONY: clean run directories inputs
