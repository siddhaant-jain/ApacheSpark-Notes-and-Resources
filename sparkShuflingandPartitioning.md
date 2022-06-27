**Task**: A Task is unit of work that happens on a partition. If any partition is way too big or too small than the other partitions which belong to same stage, then Entire stage may get delayed, out of memory errors could occur because of that one / few staggering tasks that has to deal with huge data in partitions.


### Shuffling
- **need for shuffling**
    - reference for this: https://medium.com/swlh/revealing-apache-spark-shuffling-magic-b2c304306142
    - data re-distribution: Shuffling is executed whenever there is a need to re-distribute an existing distributed data collection (rdd, dataframes, datasets). Need for re-distribution:
        - Increase or Decrease the number of data partitions: Since a data partition represents the quantum of data to be processed together by a single Spark Task, there could be situations:

            - Where existing number of data partitions are not sufficient enough in order to maximize the usage of available resources
            - Where existing number of data partitions are too heavy to be computed reliably without memory overruns.
            - Where existing number of data partitions are too high in number such that task scheduling overhead becomes the bottleneck in the overall processing time. 
        - Perform Aggregation/Join on a data collection(s): For that, all data records belonging to a join key should be in a single data partition. 
    - Partitioner and Number of Shuffle Partitions:  two widely used implementations of Partitioner, viz., Hash and Range partitioner. 'Number of Shuffle' Partitions decided in how many parts the data will be divided while 'Partitioner' provides the logic to decide in which partition a particular record will go. Hash Partitioner decides the output partition based on hash code computed for key object specified for the data record, while Range Partitioner decides the output partition based on the comparison of key value against the range of key values estimated for each of the shuffled partition. one can define their own custom partitioner and use the same for shuffling in limited RDD APIs. However, there is no such provision of custom partitioner in any of the Dataframe/Dataset APIs.

    Provision of number of shuffle partitions varies between RDD and Dataset/Dataframe APIs. In case of RDD, number of shuffle partitions are either implicitly assumed to be same as before shuffling, or number of partitions has to be explicitly provided in the APIs as an argument. In case of Dataset/Dataframe, a key configurable property ‘spark.sql.shuffle.partitions’ decides the number of shuffle partitions for most of the APIs requiring shuffling. The default value for this property is set to 200. However, in few other Dataframe/Dataset APIs requiring shuffling, user can explicitly mention the number of shuffle partitions as an argument.

    - Shuffle Block (didn;t understand completely): A shuffle block uniquely identifies a block of data which belongs to a single shuffled partition and is produced from executing shuffle write operation (by ShuffleMap task) on a single input partition during a shuffle write stage in a Spark application.

    - Shuffle Read/Write:  shuffle operation introduces a pair of stage in a Spark application. Shuffle write happens in one of the stage while Shuffle read happens in subsequent stage. Further, Shuffle write operation is executed independently for each of the input partition which needs to be shuffled, and similarly, Shuffle read operation is executed independently for each of the shuffled partition.

    - Shuffle Spill: During shuffle write operation, before writing to a final index and data file, a buffer is used to store the data records (while iterating over the input partition) in order to sort the records on the basis of targeted shuffled partitions. However, if the memory limits of the aforesaid buffer is breached, the contents are first sorted and then spilled to disk in a temporary shuffle file. This process is called as shuffle spilling. If the breach happens multiple times, multiple spill files could be created during the iteration process. After the iteration process is over, these spilled files are again read and merged to produce the final shuffle index and data file.

### Importance of partitions and dynamic coalescing
- reference article: https://towardsdatascience.com/coalescing-vs-dynamic-coalescing-in-apache-spark-1d7824c8dbcb
- Right set of partitions is pivotal for optimum execution of a Spark application. It achieves optimum efficiency when each of its constituent stages execute optimally. This means that each stage should run on optimum number of partitions which could differ from stage to stage because both, the amount of input data and the nature of computation generally differs from stage to stage.
- Too many small partitions increases Spark overhead of bookkeeping and scheduling, while large partitions lead to loss of desired parallelism. heavy and memory intensive computations prefer small sized partitions while large sized partitions are natural fit for light computations. 
- Most stages in a Spark Job comes into existence due to the shuffle exchange operators being inserted by the Spark Execution engine.

- Repartition and Coalesce : ‘Repartition’ transformation allows user to repartition a dataset into higher or lower number of partitions as compared to the original number of partitions in the dataset. This is usually done by specifying a repartitioning key being derived from one or more attributes of a record. If ‘repartition’ is used before groupby transformation, the repartitioning key is same as grouping key while if ‘repartition’ is used before Join transformation, the repartitioning key is same as join key. 
- enabling Dynamic coalescing: set 'spark.sql.adaptive.enabled=True' and 'spark.sql.adaptive.coalescePartitions.enabled=True' 

### Managing Partitions with Spark
- reference article: https://medium.com/@irem.ertuerk/managing-partitions-with-spark-41d0b6d2ea52
- The main idea of partitioning is to optimize job performance. The job performance can be obtained by ensuring each workload is distributed equally to each Spark executor. That means there is no executor without work, and non of the executors become a bottleneck due to unbalanced work assignments.
    - if we have fewer partitions than our Spark executors, the remaining executors will stay idle and we are not able to advantage fully of the existing resources.
    - if we have lot of small partitions, then more network communication to access each small file sitting on the data lake and computation may require lots of shuffling data on disk space.
- Spark is designed to advantage from in-memory calculations, but if our computational needs require to access other partitions the calculations happen by on-disk.
- We should partition our data to reduce shuffling.
    - Do not partition by columns with high cardinality
    - Partition by specific columns that are mostly used during filter and groupBy operations.
    - Even though there is no best number, it is recommended to keep each partition file size between 256MB to 1GB.
- Partition methods:
    - repartition(numsPartition, cols) By numsPartition argument, the number of partition files can be specified. On the other hand, the cols argument ensures that only one partition is created for the combination of column values.
        - repartition creates fresh new partitions and transfers data from existing partitions equally to newly created partitions. It involves a full reshuffle of data across the cluster. Slow process due to high network I/O during shuffling. All new partitions will have similar sizes but lot of shuffling is required to create it.
        - if we give a column name as parameter to repartition then a separate partition is created for each distinct value of the column. Eg if we repartition by months then separate partition will be created for each month(jan, feb ... ,dec). For such cases we should always choose column which have nearly equal number of records for each distinct value and number of distinct values are not too few or too many. If above conditions are not met, then there can be huge difference b/w partition sizes.
    - coelesce(numPartitions) is optimized for reducing the partition numbers (by not shuffling data but not ensuring exact equality in distribution), therefore whenever a partition number is needed to be reduced, coalesce should be used. 
        - if we use coalesce, the new partitions can have big difference in sizes bcz it combines nearby partitions to do minimum shuffling and network transfer. But this can lead to skewness in patitions. Leading to one of th executors taking more time than other
    - partitionBy(cols) is used to define the folder structure of data, however, there is no specific control over how many partitions are going to be created.
        - partitionBy effects the folder structure and does not have a direct effect on the number of partition files that are going to be created nor the partition sizes.
        - ensures that the folder structure created and data is split respectively based on specified column combinations. if we partition by two columns year and month. Then year folders will be created inside each year folder there will be months folders which will contain the actual file. Number of files will depend on partitions which we can change using coelesce or repartition.
        - if we specify same column for repartition and partitionBy then each folder will contain only 1 file. if we specify a column for partitionBy and number of partitions for repartition, then repartition ensures that each folder contains a maximum of a specified number of partitions.


## Optimization in spark
- reference: https://medium.com/@msdilli1997/spark-optimization-techniques-681cb85d01f9 and https://medium.com/@msdilli1997/spark-optimization-part-2-1c09c28a2625
- Use reduceByKey instead of groupByKey beacuse reduceByKey internally reduces the same key value and then shuffles the data but groupByKey shuffles data and then they try reducing it.
- Avoid UDF: UDF’s are a black box to Spark hence it can’t apply optimization and we will lose all the optimization that Spark does on Dataframe/Dataset (Tungsten and catalyst optimizer). Whenever possible we should use Spark SQL built-in functions as these functions designed to provide optimization
-  Using Broadcasting and accumulators variables concept if there is requirement: Accumulators and Broadcast variables (a read only variable that is cached on all the executors to avoid shuffling of data between executors.) both are Spark-shared variables. Accumulators is a write /update shared variable whereas Broadcast is a read shared variable. 
- Spills: Spill happens whenever there is Shuffle and the data has to be moved around and the executor is not able to hold the data in its memory. So it has to use the storage to save the data in disk for a certain time. When we don’t set right size partitions, we get spills. Always avoid Spills. In Simple terms, Spills is moving the data from in-memory to disk, we know that reading a data from disk incurs Disk I/O time. Default number of shuffle partitions in spark is 200. Take each partition holds 100 MB of data (recommended would be 100 or 200 MB of data for each partition)
- Formula to calculate a optimal shuffle partition number: partition count= Total input file data size / each partition size; if (core count ≥ partition count) then set no of shuffle partitions should be partition count; else no of shuffle partitions should be a factor of the core count. Else we would be not utilizing the cores in the last run.
- **Bucketing**: Similar to partitionBy, but uses hash function to find the bucket. or example if we have a table columns like id, state and few other columns read in a data frame, partition by can be applied on state column because we can group each state records into each partition. What if we have all unique data like id, in that case we need to use bucketing, where data’s are grouped based on the hash function. Finally, when we have a distinct column data in that case need to use bucketing and when we have duplicate data then in that case partition by helps. Partition By may create uneven partitions but bucketing creates even distribution of data. Bucketing can be used in Spark 3 to resolve the data shuffling problem to join multiple dataframe (explained in detail in second link).

### Spark Jobs: Stage, Shuffle, Tasks, Slots
- Spark creates one job for each action.
- This job may contain a series of multiple transformations.
- The Spark engine will optimize those transformations and creates a logical plan for the job.
- Then spark will break the logical plan at the end of every wide dependency and create two or more stages.
- If you do not have a wide dependency, your plan will be a single-stage plan.
- But if you have N wide-dependencies, your plan should have N+1 stages.
- Data from one stage to another stage is shared using the shuffle/sort operation.
- Now each stage may be executed as one or more parallel tasks.
- The number of tasks in the stage is equal to the number of input partitions.
- let’s assume I have 4 CPU cores to each executor. So, my Executor JVM can create four parallel threads and that’s the slot capacity of my executor. So, each executor can have four parallel threads, and we call them executor slots. The driver knows how many slots we have at each executor and it is going to assign tasks to fit in the executor slots.

### Execution Plan
- reference article: https://medium.com/data-engineering-on-cloud/advance-spark-concepts-for-job-interview-part-1-b7c2cadffc42
- better explaining article (all below points are from first article): https://medium.com/@harun.raseed093/spark-logical-and-physical-plans-e111de6cc22e
- we may have Dataframe APIs, or you may have SQL both will go to the Spark SQL engine.For Spark, they are nothing but a Spark Job represented as a logical plan. The Spark SQL Engine will process your logical plan in four stages.
![Catalyst Optimization](https://miro.medium.com/max/1400/0*0JBcGShY2b_05wrK.png)

- The Analysis stage will parse your code for errors and incorrect names. The Analysis phase will parse your code and create a fully resolved logical plan. Your code is valid if it passes the Analysis phase.
- The Logical Optimization phase applies standard rule-based optimizations to the logic plan.
- Spark SQL takes a logical plan and generates one or more in the Physical Planning phase. Physical planning phase considers cost based optimization. So the engine will create multiple plans to calculate each plan’s cost and select the one with the low cost. At this stage the engine use different join algorithms to generate more than one physical plan.
- The last stage is Code Generation. So, your best physical plan goes into code generation, the engine will generate Java byte code for the RDD operations, and that’s why Spark is also said to act as a compiler as it uses state of the art compiler technology for code generation to accelerate execution.

