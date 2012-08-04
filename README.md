# PyHDFS

## Description

**PyHDFS** is python interface to Apache Hadoop Distributed File System.  
It uses WebHDFS REST API of Hadoop.
 
## Prerequisites

WebHDFS must to enabled for PyHDFS to work. 
To enable WebHDFS set **dfs.webhdfs.enabled** property in **hdfs-site.xml**

	<property>
		<name>dfs.webhdfs.enabled</name>
		<value>true</value>
	</property>

## Current Status

PyHDFS is currently in very early stage of developement and should be used very
cautiously.

## Bugs & Known Issues
PyHDFS currently works well if no security is enabled on HDFS.  
It is not tested on HDFS instances with Kerboros Security.  
  
Please report any bugs and suggestions at https://github.com/nkoilada/pyhdfs/issues 

## License

PyHDFS comes with GNU LGPL v2.1. For complete terms see "LICENSE.txt"

## Author

Name: Nagendra Koilada  
Email: nkoilada@uci.edu
