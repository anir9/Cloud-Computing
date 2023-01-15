EXAMPLE_DIR = /user/$(USER)/wordcount
INPUT_DIR   = $(EXAMPLE_DIR)/input
OUTPUT_DIR  = $(EXAMPLE_DIR)/output
OUTPUT_FILE = $(OUTPUT_DIR)/part-r-00000


CLASSPATH=$(shell hadoop classpath)

run: wordcount1.jar inputs
	-hdfs dfs -rm -f -r $(OUTPUT_DIR)
	hadoop jar wordcount1.jar WordCount1 \
	       $(INPUT_DIR) $(OUTPUT_DIR)
	hdfs dfs -cat $(EXAMPLE_DIR)/output/"*"

wordcount1.jar: WordCount1.java
	mkdir -p wordcount_classes
	javac -classpath $(CLASSPATH) -Xlint:deprecation \
 	      -d wordcount_classes WordCount1.java
	jar -cvf wordcount1.jar -C wordcount_classes . 


inputdir:
	hdfs dfs -test -e $(EXAMPLE_DIR) || hdfs dfs -mkdir -p $(EXAMPLE_DIR)
	hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir -p $(INPUT_DIR)

outputdir:
	hdfs dfs -test -e $(OUTPUT_DIR) || hdfs dfs -mkdir -p $(OUTPUT_DIR)

inputs: inputdir
	hdfs dfs -test -e $(INPUT_DIR)/file01 \
	  || hdfs dfs -put ../input-small/file01 $(INPUT_DIR)/file01
	hdfs dfs -test -e $(INPUT_DIR)/file02 \
	  || hdfs dfs -put ../input-small/file02 $(INPUT_DIR)/file02

clean:
	-rm *.jar
	-rm -r *_classes
	-hdfs dfs -rm -f -r $(INPUT_DIR)
	-hdfs dfs -rm -f -r $(OUTPUT_DIR)
	-hdfs dfs -rm -f -r $(EXAMPLE_DIR)

.PHONY: clean run directories inputs
