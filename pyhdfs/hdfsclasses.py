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

import httplib2
import json

class HDFSContentSummary():
	pass
	def __init__(self, obj):
		for k,v in obj.iteritems():
			setattr(self, k, v)

class HDFSFileChecksum():
	pass
	def __init__(self, obj):
		for k,v in obj.iteritems():
			setattr(self, k, v)

class HDFSFileStatus():
	pass
	def __init__(self, obj):
		for k,v in obj.iteritems():
			if k == "type":
				setattr(self, "fileType", v)
			else:
				setattr(self, k, v)

class HDFSRemoteException(Exception):
	def __init__(self, obj):
		Exception.__init__(self, obj["RemoteException"]["message"])
		self.exceptionType = obj["RemoteException"]["exception"]
		self.javaClassName = obj["RemoteException"]["javaClassName"]

class HDFSConnection:
	httpconn = httplib2.Http()
	def request(self, url, method="GET", body=None, headers=None):
		header, resp = self.httpconn.request(url, method, body, headers)
		
		if header["status"] == "307":
			return header["location"]
		elif header["status"] == "201":
			return True
		elif header["status"] ==  "200":
			if header["content-type"] == "application/octet-stream":
				if header["content-length"] == "0":
					return True
				else:	
					return resp
			elif header["content-type"] == "application/json":
				response = json.loads(resp)
				if response.get("boolean") is not None:
					return response["boolean"]
				elif response.get("Token") is not None:
					print response
					return str(response["Token"]["urlString"])
				elif response.get("long") is not None:
					return long(response["long"])
				elif response.get("Path") is not None:
					return str(response.get("Path"))
				elif response.get("ContentSummary") is not None:
					return HDFSContentSummary(response["ContentSummary"])
				elif response.get("FileStatus") is not None:
					return HDFSFileStatus(response["FileStatus"])
				elif response.get("FileStatuses") is not None:
					objs = []
					for i in response["FileStatuses"]["FileStatus"]:
						objs.append(HDFSFileStatus(i))
					return objs
				else:
					return response
			else:
				pass
		elif header["status"] == "400" or \
			 header["status"] == "401" or \
			 header["status"] == "403" or \
			 header["status"] == "404" or \
			 header["status"] == "500":
			raise HDFSRemoteException(json.loads(resp))
		else:
			print header, resp
			raise HDFSRemoteException()
