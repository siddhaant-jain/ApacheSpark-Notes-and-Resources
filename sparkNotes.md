- [officialDocumentation](spark.apache.org)
- till spark 1.x rdd were part of spark core and spark sql, dataframes and datasets were api's built over it
- from spark 2., rdd are part of kernel and spark sql, dataframes and datasets are now core mmodules of spark
- mllib, spark streamin and graphx are built over it


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