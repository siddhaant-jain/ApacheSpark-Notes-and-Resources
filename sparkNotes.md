- [officialDocumentation](spark.apache.org)
- till spark 1.x rdd were part of spark core and spark sql, dataframes and datasets were api's built over it
- from spark 2., rdd are part of kernel and spark sql, dataframes and datasets are now core mmodules of spark
- mllib, spark streamin and graphx are built over it

- datasets taken from: https://github.com/dgadiraju/data

### Setup pyspark
- install anaconda
- create new virtual environment in anaconda (did from GUI)
- install pyspark 
- download winutils from here: [WinUtils](https://github.com/cdarlint/winutils)
- set hadoop_home environment variable by opening terminal from anaconda
    ```bat
    conda env config vars set HADOOP_HOME=C:\Users\SkJain\Downloads\Compressed\winutils-master\hadoop-3.2.2
    conda activate pyspark_venv
    pyspark --conf spark.ui.port=12901
    ```

- check sc by creating rdd of orders
    ```python
    orders = sc.textFile('C:/Users/SkJain/Downloads/Compressed/data-master/retail_db/orders')
    type(orders) #<class 'pyspark.rdd.RDD'>
    ```

- when running an action, it gives error (No module named 'resource'). To resolve this downgraded pyspark version 2.3.2 instead of 2.4.0 and python version 3.7.11
------------------------------------------------

- SparkContext (sc)
    - it exposes APIs such as textFile. sequenceFile to read data from files into RDD (Ressilient Distributed Dataset) which is a distributed collection
    - spark uses lazy evaluation. so sc.textFile doesn't trigger the executors. They're triggered only when an action is called.
    - RDDs expose certain APis called Transformations and Actions
    - Transformations take one RDD as input and give another RDD as o/p
    - Actions trigger execution and get data into driver program

- using 'map' transformation on orders RDD
    ```python
    ordersMap = orders.map(lambda x: (int(x.split(',')[0]), x.split(',')[1]))
    type(ordersMap) #<class 'pyspark.rdd.PipelinedRDD'>
    ```

- using 'count' action on orders
    ```python
    orders.count()
    ```


### Spark data structures
- RDDs and Dataframes (Python doesn't have datasets)
- Difference b/w the two is that RDD doesn't have a structure while dataframe does
- rdd has partitions which is equal to number of blocks in which the data is divided
- when we call an actions, it creates task where number of tasks is equal to number of partitions.
    - each partitions is processed by one task
- after running orders.count() if we check jobs sections in localhost:12901, it will show 2/2 jobs
    ```python
    orders.getNumPartitions() # this will also output 2
    ```
- typically, number of rdd partitions depends on hdfs block size
- we can also pass arg like minPartition to control this value with APIs like textFile in sparkContext
    - we can further check by typing help(sc.textFile) in python terminal
- if we give it more than number of blocks in which data is divided, it is force to create that many partitions, but if we give a lesser number then it will stay the same since we're just defining minimum partitions and not exactly how many partitions are required

- dataframe is RDD with structure (it internally uses RDD only)
- we can use a sparkSession (created by default with name 'spark' in spark-shell) to create Dataframe
    ```python
    df = spark.read.text('C:/Users/SkJain/Downloads/Compressed/data-master/retail_db/orders')
    type(df) #<class 'pyspark.sql.dataframe.DataFrame'>
    df.printSchema() #root
                     # |-- value: string (nullable = true)
    df.count()
    ```

### word count application
- python code using RDD
    ```python
    data = sc.textFile('C:\\Users\\SkJain\\Documents\\BigDataStackWorkspace\\SparkLearn\\ApacheSpark-Notes-and-Resources\\datasets\\wordCountOnSparkDocuemntation.txt')
    wc = data. \
        flatMap(lambda line: line.split(' ')). \ 
        map(lambda word: (word,1)). \
        reduceByKey(lambda x, y: x+y)

    wc.map(lambda rec: rec[0]+','+str(rec[1])).saveAsTextFile('C:\\Users\\SkJain\\Documents\\BigDataStackWorkspace\\SparkLearn\\ApacheSpark-Notes-and-Resources\\wordCountOutput.txt')
    ```
- split will convert each line into an array of word and flatmap will convert this array of array into a single array fo all words from all lines
- map will create new rdd with same number of elements but instead of a word there will be a tuple with that word and 1
- final o/p will contain distinct words and their respective count

- python code usingn Dataframe
    ```python

    from pyspark.sql.functions import split, explode

    data = spark.read.text(filepath)
    wc = data. \
        select(explode(split(data.value, ' ')).alias('words')). \
        groupBy('words'). \
        count()

    wc.show() #first 20 recs

    wc.write.csv(filepath)
    ```
- number of files by write is equal to number of thread which run while writing (by default it is 200)
- explode has similar functionality to flatMap

## Spark Framework
- Supported execution modes for spark
    - Local (for dev)
    - Standalone (for dev and testing)
    - Mesos (for prod)
    - Yarn (for prod)
    - Kubernetes

- Deploy modes
    - cluster -- driver node can be running on any of the worker nodes
    - client (default) -- driver node runs from where we submitted the spark job

### YARN
- YARN stands for Yet Another Resource Negotiator
- Built using master-slave architecture
- Master is Resource Manager and Slaves are Node Manager
- It primarily takes care of resource management and scheduling of tasks
- for each yarn application, there's a Application Master and set of containers to process the data.For Spark these containers are executors. For MapReduce these containers are MapTask and ReduceTask
- Spark create executors to process data which is managed by Resource Manager and per job Application Master

### Execution Framework
- if we set dynamic allocation for executors to False, then by default there'll be two executors
    > pyspark --conf spark.dynamicAllocation.enabled=False --conf spark.ui.port=12901
- on localhost:12901 if we go to the executors tab, we should see 3 entries in this case, one for driver and two for executors. But if we're running on local then it'll be just driver

    ![How spark runs on clusters](https://spark.apache.org/docs/3.2.1/img/cluster-overview.png)

- spark context is created on the driver program
- executors are created on worker nodes
- each executor has certain resources allocated to it (by default 1gb and 1 core)
- executors are created as soon as spark context are created
- tasks are created only when an Action is called. **Task is a unit of work which will be sent to 1 executor**
- whenever we submit a spark application, it triggers multiple jobs. Each job is a collection of tasks and are computed parallely
- job gets divided into smaller sets of tasks called stages that depend on each other like map and reduce in MapReduce framework
- every wide transformation leads to shuffling. And whenever there's shuffling, a new stage is created. In WC example map and flatmap are narrow transformation and hence are done in stage 0 but reduceByKey is wide transformation and leads to stage 1
- Cluster manager is resource manager and application master
- we can check execution plan for a rdd by belowe code
    ```python
    dagPlan = rdd_name.toDebugString()
    for line in dagPlan.split('\n'):
        print(line)
    ```
---------------------------------

### Spark SQL
- to open spark sql shell
    > spark-sql --master yarn --conf spark.ui.port=12901 --conf spark.sql.warehouse.dir=/user/${USER}/warehouse

- to check current database
    ```sql
    SELECT current_database();
    ```

- to create new database
    ```sql
    CREATE DATABASE db_name;
    USE db_name;
    SELECT current_database();
    ```

- we can also provide the database which we wan't to use while conencting to spark-sql
    > spark-sql --master yarn --conf spark.ui.port=12901 --conf spark.sql.warehouse.dir=/user/${USER}/warehouse --database db_name

- we can use 'SET' command to see any runtime properties. For eg. spark.sql.warehouse.dir is a runtime property. Below command will give the value which we have given while starting spark-sql shell
    >SET spark.sql.warehouse.dir
- if we simply write SET without any other value, it will simply give all runtime properties along with their values. If some property is having the edfault value it might not show in the list

- we can check catalogImplementation also by saying:
    > SET spark.sql.catalogImplementation
- it will give the value of hive is spark sql is reading from hive tables

- for running any linux commad or hdfs command from spark-sql shell, we can simply write exclaimation mark followed by the query, followed by semi-colon. Eg.
    > !ls -ltr /path;
> we can add .config("spark.ui.port", "0") while creating sparkSession object same as --conf

### Spark warehouse directory
- a database in spark sql is nothing but a directory in underlying file system like hdfs with .db extension.
- spark metastore table also is a directory in underlying file system like hdfs.
- a partition of spark metastore table also is a directory in underlying file system like hdfs under table directory
- all tables in a db are also sub-directories inside db dir.
- when we load data into a table, it simply copies the file in table directory (for managed tables)
- warehouse directory is the base directory where directories related to databased and tables go by default
- reference for hive queries : https://cwiki.apache.org/confluence/display/hive/languagemanual

- CREATE DB
    ```sql
    CREATE DATABASE db_name;
    CREATE DATABASE IF NOT EXISTS db_name;
    ```

- CREATE DB at a specific location
    ```sql
    CREATE DATABASE db_name LOCATION '/foldername/folder2name/soOn/dbname.db';
    ```

- SHOW list of all db
    ```sql
    SHOW databases;
    USE db_name;
    ```

- Drop empty database
    ```sql
    DROP DATABASE db_name;
    DROP DATABASE IF EXISTS db_name;
    ```

- Drop non-empty database
    ```sql
    DROP DATABASE db_name CASCADE;
    ```

### Get Metadata of tables
- metadata is generally stored in relation databases which can be accessed by query engines like spark sql to be able to serve our queries. It has a normalized data model.
- it is used for syntax and semantic checks by query engines. Which means it validates whether table name, cloumn names etc. are correct in the query or not.
- if hive is underlying db service used in spark sql than spark metastore uses hive metastore only. 

- to get just column names and data types
    ```sql
    USE db_name;
    DESCRIBE tableName
    ```

- to get more details like db, created time, last access time, owner, type(MANAGED/EXTERNAL), location, i/p format, o/p format etc
     ```sql
    USE db_name;
    DESCRIBE EXTENDED tableName
    ```

### Spark sql major type of operations
- Selection or projection (select clause)
- filtering (where clause)
- joins
- aggregations
- sorting
- analytics functions (ranking, windowing func)

**Unable to run hive on spark sql bcz of incorrect installation**
- Link on how to install [Hadoop] (https://towardsdatascience.com/installing-hadoop-3-2-1-single-node-cluster-on-windows-10-ac258dd48aef)

- Link on how to install [Hive](https://towardsdatascience.com/installing-apache-hive-3-1-2-on-windows-10-70669ce79c79) on windows

**it started runnig without doing all this. Simply copy hadoop.dll file present in winutils/bin folder to windows/system32 folder and two below mentioned lines in python code, Delete any existing metastore_db and spark-warehouse folder after closing everything else. Then restart the system, it should start working**

```python
import os
import sys
os.environ["HADOOP_HOME"] = "C:\\Users\\SkJain\\Downloads\\Compressed\\winutils-master\\hadoop-3.2.2"
sys.path.append('C:\\Users\\SkJain\\Downloads\\Compressed\\winutils-master\\hadoop-3.2.2\\bin')
print(sys.path)
```

**createDataframe API of spark fails to run**
- solution: os.environ["PYSPARK_PYTHON"] = "python" (add this to code)

### Spark ddl and dml commands (in notebooks)


- to check run time arguments passed to code we can use sys library
- it has a argv attribute which is of type list and contains all runtime arguments
- argv[0] contains name or path of file which is running
    ```python
    import sys
    sys.argv[1]
    ```


check repartition vs coalsce

- to run in prod
    - create a zip of all python files except app.py (driver code)
    - ensure all environment variables are set corrctly
    - and all data paths are configured correctly
    - then run the command
    ```
    spark-submit --master yarn --py-files zipfilepath app.py
    ```

## Spark Application - Deploying and Monitoring
- there are three type of nodes in a hadoop/spark cluster
    - gateway nodes 
        - end users have access to only these nodes
        - this is where the application is deployed)
    - master nodes
        - they do resource management
        - compile results of worker nodes
    - worker nodes
        - they are responsible for actuall storing of data and executing tasks
- capacity of the cluster is generally considered as the capacity of each worker node
    - capacity means if CPU is x, ram is y and storage is z for each worker node
    - and there are n worker nodes then capacity is nx cpu, ny ram and nz storage
    - **check capacity of cloud services I'm using in current org, might be asked in interviews**
- to understand capacity of YARN, we can log into resource manager 
    - It is by default  configured on port 8088, but we can configure it to some other port as well
    - In Resource manager, go to about section 
    - Under cluster metric we can see capacity of yarn
    - there a table of all the worker nodes running with their capacity. 
    - Yarn capacity will be a sum of all these
- in hadoop the master component is namenode and secondary namenode and worker component is datanoed
- in yarn, master component is resource manager and worker component is node manager
- if we are running spark appications over hadoop (hdfs+yarn) architecture, then we need to configure spark history server on one of the master nodes
- we need to know the port on which spark history server can be accessed (can be 18080)
- we can know the configuration by saying spark.sparkContext.getConf().getAll()
    - where spark is a sparkSession object
- from spark history server we can see all application which ran (failed/succedded) or are currently running
- for currently running apps we can go to the applicationMaster in tracking Ui column and open SparkUI

- default --master when we run pyspark or spark-sumbit etc. is yarn
- but we can change it by modifying the properties file
- on linux systems, file is generally located in /etc/spark3/conf with name 'spark-defaults.conf'
- it has a property name spark.master which is set to yarn

- whenwe run spark application in local mode, there are no worker nodes
- everything will be running on the driver program itself (on node where the spark session is launched)
- if if spark is running in local mode, it can still access files in hdfs, bcz spark and hadoop are integrated
- when spark is integrated with hadoop (whether running in yarn mode or local mode), when we specify a file path, it will always looks for that path in hdfs location
- in this case if we want to access a file in local fs in spark we need to write like this file:///filepath

- this file:/// method only works when our spark is running in local mode, in yarn mode even this will not work
- bcz in yarn mode when we run a command like spark.read it doesn't run on the gateway node where the driver program is running, but it runs on the worker ndoes. 
- The path we mention is present on the node where the driver program is running but is irrelevant to worker node
- and since hdfs is a distributed storage we can access it from any of the nodes
- one way to access local files in by mounting the folder on nodes, but it is not a recommended way

- if by default it is configured to read from local fs and we want to read from hdfs then hdfs:///filepath

- if we mention master while running spark submit, but we have also mentioned it in the python code while creating sparkSession , then the master in python code will override the one mentioned in spark-submit

- all the CLI commands like spark-submit, spark-shell, pyspark etc. are nothing but shell scripts which we can find inside spark folder/bin

- deploy mode by default is Client (other deployment mode is cluster)
- if we run CLI commands like pyspark, spark-shell, then it can only run in client mode
- CLI cannot start in cluster mode
- cluster mode is only possible with spark-submit command
- In cluster mode deployment, the driver program is also on one of the worker nodes
- but when we run CLI commands like spark-shell, pyspark etc. it has it to open the session on the current node only (gateway node)
- also there are two types of logs generated by spark applications: executor level logs and driver level logs.
- when we run the app in client mode, we can see driver level log on the terminal of node from where we ran it
- but in cluster mode it doesn;t happen since the driver program doesn't run on the node from where ran it but on some executor node. We can also check both these logs from resource manager

- to change log setting in spark
    - we can go to the folder /etc/spark3/conf, there should be file with name log4j.properties present in it
    - inside that, there'll be a property called log4j.root.category which would be set to ERROR
    - we can change it to INFO if we want all the logs
    - if we don't want to mess the action one spark3/config, we can copy it somewhere and modify it and manually pass it with spark-submit command
    - to add a custom log4j files to spark-submit command, we need to add --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=log4jFileFullPath
    - above command was executor, similarly we need to add for driver as wll --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=log4jFileFullPath

- pass multiple properties file to spark-submit without using too many --conf
- we can do this using special variable called SPARK_CONF_DIR. By default it'll be empty
- copy the files from spark3/conf which we want to modify into a folder. Don't need to copy files which do not need modification
- then we can modify the files according to need of application in this new folder
- then write 'export SPARK_CONF_DIR=pathToFolderWithModifiedFiles'
- then we need to create a new conf file like sparkAppName.conf and add all -conf inside it
- it can either use equal to sign b/w key value or a space
- below it is space separated (sub property is separted by '='. For subproperty we can only separate using '=')
    ```
    spark.ui.port 0
    spark.executor.extraJavaOptions -Dlog4j.configuration=log4jFileFullPath
    spark.driver.extraJavaOptions -Dlog4j.configuration=log4jFileFullPath
    ```
- now we can use properties file instead of conf, the command will look like:
    > spark-submit --master yarn --deploy-mode cluster --properties-file /path/sparkAppName.conf /pythonFilePath
- property files are only to provide spark related configs. Any other things will be ignored with a warning


- above was passing spark properties using a config file
- noe we'll see how to pass application properties (inputDir, outputDir) etc using json file so we don't have to hardcode these values in code

- normal python code which is not spark code runs on driver program itself and is passed to rest of the code where ever useful
- each worker node has a cache (shown in diagram). The main code and all related files are copied to cache of each worker node. Whatever is submitted will be a part of executors

- first we create the json
    ```json
    {
        "wordcount.input.dir": "filePathOfInputDir",
        "wordcount.output.dir": "filePathOfOutputDir"
    }
    ```
- then in pyspark application we can write following code
    ```python
    import json
    props = json.load(open('jsonFilePath'))
    input_dir = props["wordcount.input.dir"]
    output_dir = props["wordcount.output.dir"]
    ```

- passing json to spark submit command using local files in client mode
    - with client mode after modifying the above code, we don't need to make any changes to spark submit command
    - bcz the driver program will be running on current node only where we have the json file.
    - and since above piece of code is non-spark code it will run on driver program only and hence python code will be able to find the json file

- passing json to spark submit command using local files in cluster mode
- this will be different bcz driver program will be running on one of the executor nodes which do not have the json file since it is a local file on the gateway node (from where we are running spark submit command)
- we need to add --files in spark submit command with an alias for json file after #
    > spark-submit --master yarn --deploy-mode cluster --properties-file /path/sparkAppName.conf --files jsonFilePath#appName.json /pythonFilePath
- when we do this, what happens in background is: this file is copied to hdfs and from there to cache of all worker nodes

- passing json to spark submit command using hdfs files in cluster mode
- copy json file and python file to hdfs folder
- now we need give hdfs path in spark instead of local file path
- for that we need to add namenode uri before the actual path
- we can find the namenode uri in /hadoop/conf/core-site.xml with name fs.defaultFS
    > spark-submit --master yarn --deploy-mode cluster --properties-file /path/sparkAppName.conf --files hdfsNamenodeUri/jsonFilePath#appName.json hdfsNamenodeUri/pythonFilePath

- passing external python libraries (in this example we're installing pyyaml library for using yaml files)
- creat a yaml file (for loading it using pyyaml library for verification)
    ```yaml
    wordcount.input.dir:
        inputFilePath
    wordcount.output.dir:
        outputFilePath
    ```

- to install dependencies we need in our application, we can run the command like this:
    > python3 -m pip install pyyaml -t folderToInstallThis
- then we need to create zip of folder which contains all the dependencies
- now for passing dependencies with pyspark or spark-submit we can mention like:
    > pyspark --py-files zipFilePath 
- now using it with spark-submit with local file and deploy mode client
    > spark-submit --master yarn --deploy-mode client --properties-file /path/sparkAppName.conf --py-files pathOfZipFolder pythonFilePath

- now using it with spark-submit with local file and deploy mode cluster
- we need to additionally as files parameter with address of yaml file which we'll read from code
- it was not required in client mode bcz the path can be mentioned inside python code only since driver program and yaml file will be on same node.
    > spark-submit --master yarn --deploy-mode cluster --properties-file /path/sparkAppName.conf --files pathToYamlFile#aliasToUseInCode --py-files pathOfZipFolder pythonFilePath

- now using it with spark-submit with hdfs file and deploy mode cluster
    > spark-submit --master yarn --deploy-mode cluster --properties-file /path/sparkAppName.conf --py-files hdfsNamenodeUri/pathOfZipFolder hdfsNamenodeUri/pythonFilePath


- movielens [dataset](https://grouplens.org/datasets/movielens/) for practice

- broadcast variable: In PySpark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs
- Note that broadcast variables are not sent to executors with sc.broadcast(variable) call instead, they will be sent to executors when they are first used
- The broadcasted data is cache in serialized format and deserialized before executing each task


### User defined functions (udf)
- to importe: from pyspark.sql.functions import udf
- create a normal python function (say abc)
- then write udfVariable = udf(lambda x: abc(x))
- df.withColumn('colName', udfVariable(col('colName')))

### Accumulators
- The PySpark Accumulator is a shared variable that is used with RDD and DataFrame to perform sum and counter operations similar to Map-reduce counters. 
- These variables are shared by all executors to update and add information through aggregation or computative operations.
- Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to update and updates from the workers get propagated automatically to the driver program. But, only the driver program is allowed to access the Accumulator variable using the value property.
- to create accumulator: sparkContext.accumulator()
- We can create Accumulators in PySpark for primitive types int and float. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.
- it has two params .add(x), .value

### Create a temp sql table on a dataframe to run sql query
- dataframeName.createOrReplaceTempView("tempTableName")
- now we can do spark.sql("SELECT * FROM tempTableName")

### Window functions in pyspark
    ```python
    from pyspark.sql.window import Window
    # get all columns for the record with highest grade in each year
    each_year = Window.partitionBy(F.col('year'))
    res = (df.withColumn('highest_grade', F.max(F.col('grade')).over(each_year))
       .where(F.col('highest_grade')==F.col('grade'))
       .drop(F.col('highest_grade'))
       )
    ```
- Window functions in detail: https://medium.com/towards-data-science/a-dive-into-pyspark-window-functions-a090aee4ff23


### File formats in Big Data
    - file formats: https://medium.com/@ghoshsiddharth25/hdfs-storage-formats-what-when-to-use-52f150d5de1b

    - CSV: CSV is a row-based file format,using plain text. CSV format is not fully standardized, and files may use separators other than commas, such as tabs or spaces. CSV files are only splittable when it is a raw, uncompressed file or when splittable compression format is used such as bzip2 or lzo 
        - Pros: human-readable and easy to edit manually, provides a simple scheme, can be processed by almost all existing applications, easy to implement and parse, CSV is compact (headers are written only once unlike json or XML).
        - Cons: allows to work with flat data. Complex data structures have to be processed separately from the format; No support for column types. No difference between text and numeric columns; no standard way to present binary data; Poor support for special characters; Lack of a universal standard.

    - JSON: presented as key-value pairs in a partially structured format.it can store data in a hierarchical format and is user readable. typically much smaller than XML therefore more commonly used in network communication (REST API), 
        - PROS: supports hierarchical structures, simplifying the storage of related data in a single document and presenting complex relationships; simplified JSON serialization libraries or built-in support for JSON serialization/deserialization in most languages; widely used file format for NoSQL databases such as MongoDB, Couchbase and Azure Cosmos DB;Built-in support in most modern tools;
        - CONS: consumes more memory due to repeatable column names; Poor support for special characters; not very splittable; lacks indexing;

    - PARQUET: Unlike CSV and JSON, parquet files are binary files that contain metadata about their contents. Therefore, without reading/parsing the contents of the file(s), Spark can simply rely on metadata to determine column names, compression/encoding, data types, and even some basic statistical characteristics. Column metadata for a Parquet file is stored at the end of the file, which allows for fast, single-pass writing. they can be highly compressed (compression algorithms work better with data with low entropy of information, which is usually contained in columns) and can be separated. Parquet is optimized for the paradigm Write Once Read Many (WORM). It writes slowly but reads incredibly quickly, especially when you only access a subset of columns. Parquet is good choice for heavy workloads when reading portions of data.
        
        - PROS: Parquet is a columnar format. Only the required columns will be retrieved/read, this reduces disk I/O. The concept is called projection pushdown;scheme travels with the data, so the data is self-describing; Parquet provides very good compression up to 75% when using even compression formats like snappy;format is the fastest for read-heavy processes compared to other file formats; suited for data storage solutions where aggregation on a particular column over a huge set of data is required; provides predicate pushdown, thus reducing the further cost of transferring data from storage to the processing engine for filtering;
        - CONS: does not always have built-in support in tools other than Spark;It does not support data modification (Parquet files are immutable) and scheme evolution. Of course, Spark knows how to combine the schema if you change it over time (you must specify a special option while reading), but you can only change something in an existing file by overwriting it.

        - PREDICATE PUSHDOWN: certain parts of queries (predicates) can be "pushed" to where the data is stored. when we give some filtering criteria, the data storage tries to filter out the records at the time of reading. advantage is that there are fewer disk i/o operations and therefore overall performance is better. Otherwise, all data will be written to memory, and then filtering will have to be performed, resulting in higher memory requirements. This concept is followed by most DBMS, as well as big data storage formats such as Parquet and ORC.

        - Projection Pushdown: When reading data from the data storage, only those columns that are required will be read, not all fields will be read. Typically, column formats such as Parquets and ORC follow this concept, resulting in better I/O performance.

    - AVRO: row-based format that has a high degree of splitting. It is also described as a data serialization system similar to Java Serialization. The schema is stored in JSON format, while the data is stored in binary format, which minimizes file size and maximizes efficiency.support for schema evolution by managing added, missing, and changed fields. This allows old software to read new data, and new software to read old data — it is a critical feature if your data can change.Since Avro is a row-based format, it is the preferred format for handling large amounts of records as it is easy to add new rows.
        - PROS: Avro stores the schema in a file header, so the data is self-describing;Easy and fast data serialization and deserialization, which can provide very good ingestion performance;As with the Sequence files, the Avro files also contain synchronization markers to separate blocks. This makes it highly splittable; schema used to read Avro files does not necessarily have to be the same as the one used to write the files. This allows new fields to be added independently of each other;Avro handles schema changes like missing fields, added fields, and changed fields. ;
        - CONS: data is not human-readable; Not integrated into every programming language.
    
    - ORC: has many advantages such as: Hive type support including DateTime, decimal, and the complex types; Concurrent reads of the same file using separate RecordReaders; ability to split files without scanning for markers; Metadata stored using Protocol Buffers, which allows the addition and removal of fields; ORC file format stores collections of rows in one file and within the collection the row data is stored in a columnar format. An ORC file contains groups of row data called stripes and auxiliary information in a file footer. At the end of the file a postscript holds compression parameters and the size of the compressed footer.The default stripe size is 250 MB. Large stripe sizes enable large, efficient reads from HDFS.

    - Avro is preferred for storing data in a data lake landing zone because from here we need to perform ETL operations for which row based format is more effecient. Also it is easy to retrieve the schema and handle schema changes.
    - Parquet is good for queries that read particular columns from a “wide” (with many columns) table since only needed columns are read, and IO is minimized.
    - PARQUET only supports schema append, whereas AVRO supports a much-featured schema evolution, i.e., adding or modifying columns.
    - ORC vs. PARQUET
        - PARQUET is more capable of storing nested data.
        - ORC is more capable of Predicate Pushdown.
        - ORC supports ACID properties.
        - ORC is more compression efficient.


    ![filetype comparison](https://miro.medium.com/max/700/1*0Frhzu__LfgEX99Ergb-Hg.png)


- spark execution plan: https://medium.com/@omarlaraqui/a-beginners-guide-to-spark-execution-plan-b11f441005c9

- partitioning and bucketing: https://selectfrom.dev/apache-spark-partitioning-bucketing-3fd350816911

- createTempView, createGlobalTempView