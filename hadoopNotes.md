- important configuaration files to study for hadoop, yarn, hive and spark:
    - /etc/hadoop/core-site.xml
    - /etc/hadoop/hdfs-site.xml
    - /etc/hadoop/hadoop-env.sh
    - /dfs
    - start-dfs.sh 
    - all other config files in /etc/hadoop
    - /etc/hadoop/yarn-site.xml
    - /etc/hadoop/mapred-site.xml
    - /conf/hive-site.xml
    - conf/spark-env.sh
    - conf/spark-defaults.conf

- setting hadoop_home

- study REPL (Read, evaluate, print, loop)

## hadoop commands
> similar to linux commands but use hadoop fs or hdfs dfs at the starting

- hadoop fs -help 
    - give details of all th commands

### hadoop ls command
- hadoop fs -ls -help
    - this will give all options availible with ls (similarly 'help' can be used with other commands also)
    - ls by default is sorted by name in alphabetic order
    - we can use -r to reverse the current order
    - we can use -t to sort by time (most recent first, so -t -r will sort by time with oldest first)
    - we can use -S to sort by size (largest files first, so -S -r will sort by time with smallest first)
    - we can use -h to see size of file in readable format

### managing directories
- sudo -u hdfs hdfs dfs -rm -R -skipTrash /user/username
    - this command is used to delete a user
    - first hdfs is a superuser, and second is the actual command which we use while running
    - we can run this with username also instead of hdfs since it will also have access to it's own folder
    - but it's the wrong way, since user will be logged out after deleting itself

- sudo -u hdfs hdfs dfs -mkdir /user/username
    - we can use -p to create nested subfolders which can be given as path
    - any part of path which is already present will be ignored
- sudo -u hdfs hdfs dfs -chown -R username:something /user/username
    - we use -R to recorsively give same permission to subfolders and files also

- hdfs dfs -rm -R
    - this can be used to delete a non-empty folder
- hdfs dfs -rmdir
    - this can be used to delete a empty folder
    - we can pass multiple folders also for deleting

### Copying files between local and hdfs
- we can use -copyFromLocal or -put command
- we cannot update a single record in any file in hdfs. We need to copy it to local, fix it and then put it back to hdfs
- files which we copy to hdfs will be divided into blocks and stored in different data nodes in distributed fashion

- put and copyFromLocal are identical, all options are applicable and both of themm can be used alternatively
-  syntax: -put [-f] [-p] [-l] filePathInLocalSystem hdfsPathToCopyIn
    - we use -f overwrite the destination if file already exists
    - if we don't use -f, then it will give warning for all the files which already exists but it won't fail, it'll still copy all the non-existing files even if they come after an existing file
    - we use -p to preserve access, modfication time, ownership and mode etc.

- hdfs dfs -put parentFolderLocal/mainFolderLocal/* parentFolderHadoop/mainFolderHadoop
    - it means to copy all files and folder inside mainFolderLocal to mainFolderHadoop
    - if we don't mention /* then it will simply create something like mainFolderHadoop/mainFolderLocal which is copy entire localFolder in the hadoop as subfolder
    - alternatively we can also do: hdfs dfs -put parentFolderLocal/mainFolderLocal/* parentFolderHadoop

- we can use -copyToLocal or -get to copy from hdfs to local file system
- files remain in hdfs as well
- we can mention multiple files on hdfs side which we want to copy but in that case on local file system side we need to mention a folder. We cannot copy multiple files into a single file

### getting file's metadata
- fsck command is used for that
- it a command under hdfs similar to dfs, so we directly write hdfs fsck and not hdfs dfs fsck
- get details of files, locations and blocks
- each block in hadoop is of size 128mb except last block which will lesser size

### previewing data in hdfs
- we can use -tail and -cat command for that
- recommended to use only to read text file
- -tail print only last 1kb of file
- we use -f with tail  which will keep adding data to display if content is getting added to the file. Sort of like streaming
- cat print the entire content and can take a while for even medium sized file. So should be used carefully.

### HDFS block size
- hdfs stands for hadoop distributed file system
- all the data is divided into block which are put on different worked node
- if replication factor of say 2 is there, then this same block will be present in 3 different worker nodes
- each block will have a unique id and can be of max size 128mb
- block size property(dfs.blocksize) is defined in hdfs-site.xml

### HDFS replication factor
- making multiple copies to make it fault-tolerant
- default replication factor is 3
- we configure rack awareness in production to further improve fault tolerance
- replication factor property(dfs.replication) is defined in hdfs-site.xml

### HDFS storage usage
- hdfs dfs -df
    - get current capacity and usage of hdfs
    - use -h with this to get in readable format (gb/tb instead of bytes)
- hdfs dfs -du
    - get size occupied by a file or folder
    - use -h with this to get in readable format (gb/tb instead of bytes)
    - use -s to summarize at folder level instead of at file or subfolder level

### hdfs stat command
    - hdfs dfs -stat path/of/folder
        - simply running this will give when was folder last updated
        - adding %b will give size of file
        - %F will give whether a file or directory
        - %o gives the block size
        - %r gives the replication factor

### hdfs file permissions
- owner will have read(r), write(e) and execute(x) permission
- others will have only r and x
- permission can be given using chmod command similar to linux
- to give write permissions to other users
    > hdfs dfs -chmod -R o+w filePath
    - o stands for others and w for write
    - if we replace o with g it will give it to the user group
    - o is for others, g is for group and u is for owner

    > hdfs dfs -chmod -R -w filePath
    - this command will remove the write permissions
    - since we have not mentioned u,g or o it will remove for everyone

- we can do same using octal form also
- each of r,w,x are represented by 0/1
- 0 means not permitted and 1 means permitted
- if a user as read and execute permission then it'll be 101, if they have no permissions, then it will be 000 and f they have all it'll be 111
- if we convert this to decimal, minimum is 0 (000) and maximum is 7(111). So 0 to 7 are all possible permission a user can have
- since for a particular file,we give different permissions to owner, group and others we will have to provide three decimal number with chmod
- eg. if we give permissions 747 to file, it'll imply 111-100-111, which would mean rwx,rx,rwx i.e. owner and others have all permissions but user group has only read permissions
    > hdfs dfs -chmod -R 757 filePath

### Overriding properties
- we can change any property not defined as final in core-site.xml and hdfs-site.xml
- we can change block size and replication factor while copying the files. We can also change after copying
- we can pass individual properties using -D or multiple properties in xml file using --conf
    > hdfs dfs -Ddfs.blocksize=64M -Ddfs.replication=3 -put localFilePath hadoopFilePath


https://towardsdatascience.com/complete-guide-to-spark-and-pyspark-setup-for-data-science-374ecd8d1eea
https://github.com/dgadiraju/retail_db
https://github.com/siddhaant-jain/MongoDB-Notes-and-Resources

https://www.sarthaksarbahi.com/interview-amex
file:///C:/Users/SkJain/Downloads/resume---sarthak-sarbahi-Aq2QX4X19Vsqe8xj.pdf
