# PyHDFS: A python interface to Apache Hadoop Distributed File System
#
# Copyright (C) 2012  Nagendra Koilada
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

import urllib
from hdfsclasses import *

NO_SECURITY = 1
KERBOROS_SPNEGO = 2
DELEGATION_TOKEN = 3

hdfs_config = {"hostname" : "localhost",
			   "namenode_port" : "50070",
			   "user" : "webuser"}

auth_params = {"user.name" : hdfs_config["user"]}

conn = HDFSConnection()

def paramsToURL(path, request_params):
	url = "http://" + hdfs_config["hostname"] + \
		  ":" + hdfs_config["namenode_port"] + \
		  "/webhdfs/v1" + path + "?"

	for k,v in auth_params.items() + request_params.items():
		if v is not None:
			url = url + k + "=" + v + "&"
	return url.rstrip("&")

def HDFSConfig(hostname, port, username):
	global hdfs_config
	hdfs_config["hostname"] = hostname
	hdfs_config["namenode_port"] = port	
	hdfs_config["user"] = username
	global auth_params
	auth_params["user.name"] = username

def getAuthorization(auth_type):
	global auth_params
	auth_params = {}
	if auth_type is KERBOROS_SPNEGO:
		pass
	elif auth_type is DELEGATION_TOKEN:
		auth_params["delegation"] = value
	else:
		auth_params["user.name"] = hdfs_config["user"]

def makeDirectory(path, perms=None):
	request_params = {"op" : "MKDIRS",
					  "permission" : perms}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "PUT")
	return resp

def move(src, dest):
	request_params = {"op" : "RENAME",
					  "destination" : dest}
	url = paramsToURL(src, request_params)
	resp = conn.request(url, "PUT")
	return resp

def remove(path, recursive=False):
	request_params = {"op" : "DELETE",
					  "recursive" : str(recursive).lower()}
 	url = paramsToURL(path, request_params)
	resp = conn.request(url, "DELETE")
	return resp
	
def getFileChecksum(path):
	request_params = {"op" : "GETFILECHECKSUM"}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "GET")
	return resp

def setOwner(path, owner=None, group=None):
	request_params = {"op" : "SETOWNER",
					  "owner" : owner,
					  "group" : group}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "PUT")
	return resp

def getFileStatus(path):
	request_params = {"op" : "GETFILESTATUS"}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "GET")
	return resp

def listDirectory(path):
	request_params = {"op" : "LISTSTATUS"}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "GET")
	for i in resp:
		i.path = path + "/" + i.path
	return resp

def getContentSummary(path):
	request_params = {"op" : "GETCONTENTSUMMARY"}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "GET")
	return resp

def setPermissions(path, perms):
	request_params = {"op" : "SETPERMISSION",
					  "permission" : perms}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "PUT")
	return resp

def setReplicationFactor(path, rf):
	request_params = {"op" : "SETREPLICATION",
					  "replication" : rf}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "PUT")
	return resp

def setAccessTime(path, access_time, modification_time):
	request_params = {"op" : "SETTIMES",
					  "accesstime" : access_time,
					  "modificationtime" : modification_time}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "PUT")
	return resp

def getHomeDirectory():
	request_params = {"op" : "GETHOMEDIRECTORY"}
	url = paramsToURL("", request_params)
	resp = conn.request(url, "GET")
	return resp

def getDelegationToken():
	request_params = {"op" : "GETDELEGATIONTOKEN",
					  "renewer" : hdfs_config["user"]}
	url = paramsToURL("", request_params)
	resp = conn.request(url, "GET")
	return resp

def renewDelegationToken(token):
	request_params = {"op" : "RENEWDELEGATIONTOKEN",
					  "token" : token}
	url = paramsToURL("", request_params)
	resp = conn.request(url, "PUT")
	return resp

def cancelDelegationToken():
	request_params = {"op" : "CANCELDELEGATIONTOKEN",
					  "token" : token}
	url = paramsToURL("", request_params)
	resp = conn.request(url, "PUT")
	return resp

def readFile(path, offset=None, length=None, buffersize=None):
	request_params = {"op" : "OPEN",
					  "offset" : offset,
					  "length" : length,
					  "buffersize" : buffersize}
	url = paramsToURL(path, request_params)
	resp = conn.request(url, "GET")
	return resp

def putFile(srcfile, destpath , overwrite=False, blocksize=None, 
		    replication=None, perms=None, buffersize=None):
	request_params = {"op" : "CREATE",
					  "overwrite" : str(overwrite).lower(),
					  "blocksize" : blocksize,
					  "replication" : replication,
					  "permission" : perms,
					  "buffersize" : buffersize}
	url = paramsToURL(destpath, request_params)
	datanode_loc = conn.request(url, "PUT")
	resp = conn.request(datanode_loc, "PUT", open(srcfile, "rb"))
	
#def fileAppend(path, data, buffersize=None):
#	request_params = {"op" : "APPEND",
#					  "buffersize" : buffersize}
#	url = paramsToURL(path, request_params)
#	datanode_loc = conn.request(url, "POST")
#	resp = conn.request(datanode_loc, "POST", data)
