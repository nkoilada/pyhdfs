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

## Examples

	$HADOOP_HOME/bin/hadoop namenode -format &
	$HADOOP_HOME/bin/start-all.sh

	>>> from pyhdfs import hdfs
	>>> 
	>>> # Print Current Config
	... print hdfs.getConfig()
	{'namenode_port': '50070', 'hostname': 'localhost', 'user': 'webuser'}
	>>> 
	>>> # Set username is config
	... hdfs.setConfig(username = "nkoilada")
	>>> print hdfs.getConfig()
	{'namenode_port': '50070', 'hostname': 'localhost', 'user': 'nkoilada'}
	>>> 
	>>> # List files in root directory
	... for f in hdfs.listDirectory("/"):
	...	    print f.fileType, f.permission, f.owner, f.path
	... 
	DIRECTORY 755 nkoilada /test2
	DIRECTORY 755 nkoilada /tmp
	>>> # Create a "test" directory in root directory
	... hdfs.makeDirectory("/test")
	True
	>>> for f in hdfs.listDirectory("/"):
	...     print f.fileType, f.permission, f.owner, f.path
	... 
	DIRECTORY 755 nkoilada /test
	DIRECTORY 755 nkoilada /test2
	DIRECTORY 755 nkoilada /tmp
	>>> # Print home direcoty
	... print hdfs.getHomeDirectory()
	/user/nkoilada
	>>> 
	>>> # Move "test" to "test2"
	... hdfs.move("/test", "/test2")
	True
	>>> for f in hdfs.listDirectory("/"):
	...     print f.fileType, f.permission, f.owner, f.path
	... 
	DIRECTORY 755 nkoilada /test2
	DIRECTORY 755 nkoilada /tmp
	>>> 
	
## Bugs & Known Issues
PyHDFS currently works well if no security is enabled on HDFS.  
It is not tested on HDFS instances with Kerboros Security.  
  
Please report any bugs and suggestions at https://github.com/nkoilada/pyhdfs/issues 

## License

PyHDFS comes with GNU LGPL v2.1. For complete terms see "LICENSE.txt"

## Author

Name: Nagendra Koilada  
Email: nkoilada@uci.edu
