from pyhdfs import hdfs

# Print Current Config
print hdfs.getConfig()

# Set username is config
hdfs.setConfig(username = "nkoilada")
print hdfs.getConfig()

# List files in root directory
for f in hdfs.listDirectory("/"):
	print f.fileType, f.permission, f.owner, f.path

# Create a "test" directory in root directory
hdfs.makeDirectory("/test")
for f in hdfs.listDirectory("/"):
    print f.fileType, f.permission, f.owner, f.path

# Print home direcoty
print hdfs.getHomeDirectory()

# Move "test" to "test2"
hdfs.move("/test", "/test2")
for f in hdfs.listDirectory("/"):
	print f.fileType, f.permission, f.owner, f.path


