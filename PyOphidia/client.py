#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2012-2016 CMCC Foundation
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import sys
import os

sys.path.append(os.path.dirname(__file__))

import ophsubmit as _ophsubmit
import json

del sys
del os

from inspect import currentframe


def get_linenumber():
	cf = currentframe()
	return __file__, cf.f_back.f_lineno


class Client():
	"""Client(username,password,server,port='11732') -> obj

	Attributes:
		username: Ophidia username
		password: Ophidia password
		server: Ophidia server address
		port: Ophidia server port (default is 11732)
		session: ID of the current session
		cwd: Current Working Directory
		cube: Last produced cube PID
		exec_mode: Execution mode, 'sync' for synchronous mode (default),'async' for asynchronous mode
		ncores: Number of cores for each operation (default is 1)
		last_request: Last submitted query
		last_response: Last response received from the server (JSON string)
		last_jobid: Job ID associated to the last request

	Methods:
		submit(query) -> self : Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server according to all login parameters of the Client and its state.
		deserialize_response() -> dict : Return the last_response JSON string attribute as a Python dictionary.
		resume_session() -> self : Resume the last session the user was connected to.
		resume_cwd() -> self : Resume the last cwd (current working directory) the user was located into.
		resume_cube() -> self : Resume the last cube produced by the user.
		wsubmit(workflow,*params) -> self : Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.
		wisvalid(workflow) -> bool : Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False.
	"""

	def __init__(self, username, password, server, port='11732'):
		"""Client(username,password,server,port='11732') -> obj

		:param username: Ophidia username
		:type username: str
		:param password: Ophidia password
		:type password: str
		:param server: Ophidia server address
		:type server: str
		:param port: Ophidia server port (default is 11732)
		:type port: str
		:returns: None
		:rtype: None
		:raises: RuntimeError
		"""

		self.username = username
		self.password = password
		self.server = server
		self.port = port
		self.session = ''
		self.cwd = '/'
		self.cube = ''
		self.exec_mode = 'sync'
		self.ncores = 1
		self.last_request = ''
		self.last_response = ''
		self.last_jobid = ''
		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		try:
			self.resume_session().resume_cwd().resume_cube()
		except Exception as e:
			print(get_linenumber(),"Something went wrong in resuming last session/cwd/cube:", e)
		else:
			if self.session:
				print("Current session is " + self.session)
			if self.cwd:
				print("Current cwd is " + self.cwd)
			if self.cube:
				print("The last produced cube is " + self.cube)
		finally:
			pass

	def __del__(self):
		del self.username
		del self.password
		del self.server
		del self.port
		del self.session
		del self.cwd
		del self.cube
		del self.exec_mode
		del self.ncores
		del self.last_request
		del self.last_response
		del self.last_jobid

	def submit(self, query):
		"""submit(query) -> self : Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server according to all login parameters of the Client and its state.

		:param query: query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;'
		:type query: str
		:returns: self or None
		:rtype: Client or None
		:raises: RuntimeError
		"""

		if query is None:
			raise RuntimeError('query is not present')
		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		# Check if the query contains only the oph operator
		r = query.split()
		if len(r) != 1:
			if not query.endswith(';'):
				query = query.rstrip()
				query += ';'
		else:
			query += ' '
		if self.session and 'sessionid' not in query:
			query += 'sessionid=' + self.session + ';'
		if self.cwd and 'cwd' not in query:
			query += 'cwd=' + self.cwd + ';'
		if self.cube and 'cube' not in query:
			query += 'cube=' + self.cube + ';'
		if self.exec_mode and 'exec_mode' not in query:
			query += 'exec_mode=' + self.exec_mode + ';'
		if self.ncores and 'ncores' not in query:
			query += 'ncores=' + str(self.ncores) + ';'
		self.last_request = query
		try:
			self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
			if return_value:
				raise RuntimeError(error)
			if newsession is not None:
				if len(newsession) == 0:
					self.session = None
				else:
					self.session = newsession
					self.cwd = '/'
			response = self.deserialize_response()
			if response is not None:
				for response_i in response['response']:
					if response_i['objclass'] == 'text':
						if response_i['objcontent'][0]['title'] == 'Output Cube':
							self.cube = response_i['objcontent'][0]['message']
							break
				for response_i in response['response']:
					if response_i['objclass'] == 'text':
						if response_i['objcontent'][0]['title'] == 'Current Working Directory':
							self.cwd = response_i['objcontent'][0]['message']
							break
		except Exception as e:
			print(get_linenumber(),"Something went wrong in submitting the request:", e)
			return None
		return self

	def deserialize_response(self):
		"""deserialize_response() -> dict : Return the last_response JSON string attribute as a Python dictionary.

		:returns: deserialized response or None
		:rtype: dict or None
		"""

		if self.last_response is None:
			return None
		return json.loads(self.last_response)

	def resume_session(self):
		"""resume_session() -> self : Resume the last session the user was connected to.

		:returns: self or None
		:rtype: Client or None
		:raises: RuntimeError
		"""

		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		query = 'operator=oph_get_config;key=OPH_SESSION_ID;'
		self.last_request = query
		try:
			self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
			if return_value:
				raise RuntimeError(error)
			response = self.deserialize_response()
			if response is not None:
				for response_i in response['response']:
					if response_i['objkey'] == 'get_config':
						self.session = response_i['objcontent'][0]['rowvalues'][0][1]
						break
		except Exception as e:
			print(get_linenumber(),"Something went wrong in resuming last session:", e)
			return None
		return self

	def resume_cwd(self):
		"""resume_cwd() -> self : Resume the last cwd (current working directory) the user was located into.

		:returns: self or None
		:rtype: Client or None
		:raises: RuntimeError
		"""

		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		query = 'operator=oph_get_config;key=OPH_CWD;'
		self.last_request = query
		try:
			self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
			if return_value:
				raise RuntimeError(error)
			response = self.deserialize_response()
			if response is not None:
				for response_i in response['response']:
					if response_i['objkey'] == 'get_config':
						self.cwd = response_i['objcontent'][0]['rowvalues'][0][1]
						break
		except Exception as e:
			print(get_linenumber(),"Something went wrong in resuming last cwd:", e)
			return None
		return self

	def resume_cube(self):
		"""resume_cube() -> self : Resume the last cube produced by the user.

		:returns: self or None
		:rtype: Client or None
		:raises: RuntimeError
		"""

		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		query = 'operator=oph_get_config;key=OPH_DATACUBE;'
		self.last_request = query
		try:
			self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
			if return_value:
				raise RuntimeError(error)
			response = self.deserialize_response()
			if response is not None:
				for response_i in response['response']:
					if response_i['objkey'] == 'get_config':
						self.cube = response_i['objcontent'][0]['rowvalues'][0][1]
						break
		except Exception as e:
			print(get_linenumber(),"Something went wrong in resuming last cube:", e)
			return None
		return self

	def wsubmit(self, workflow, *params):
		"""wsubmit(workflow,*params) -> self : Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.

		:param workflow: JSON string or path of a JSON file containing an Ophidia workflow
		:type workflow: str
		:param params: list of positional parameters that will replace $1, $2 etc. in the workflow
		:type params: str
		:returns: self or None
		:rtype: Client or None
		:raises: RuntimeError
		"""

		if workflow is None:
			raise RuntimeError('workflow is not present')
		if self.username is None or self.password is None or self.server is None or self.port is None:
			raise RuntimeError('one or more login parameters are None')
		request = None
		import os.path
		import re

		if os.path.isfile(workflow):
			try:
				file = open(workflow, 'r')
				buffer = file.read()
				file.close()
				for index, param in enumerate(params, start=1):
					buffer = buffer.replace('${' + str(index) + '}', str(param))
					buffer = re.sub('(\$' + str(index) + ')([^0-9]|$)', str(param) + '\g<2>', buffer)
				request = json.loads(buffer)
			except Exception as e:
				print(get_linenumber(),"Something went wrong in reading and/or parsing the file:", e)
				return None
		else:
			try:
				buffer = workflow
				for index, param in enumerate(params, start=1):
					buffer = buffer.replace('${' + str(index) + '}', str(param))
					buffer = re.sub('(\$' + str(index) + ')([^0-9]|$)', str(param) + '\g<2>', buffer)
				request = json.loads(buffer)
			except Exception as e:
				print(get_linenumber(),"Something went wrong in parsing the string:", e)
				return None
		del os.path
		del re
		if self.session and 'sessionid' not in request:
			request['sessionid'] = self.session
		if self.cwd and 'cwd' not in request:
			request['cwd'] = self.cwd
		if self.cube and 'cube' not in request:
			request['cube'] = self.cube
		if self.exec_mode and 'exec_mode' not in request:
			request['exec_mode'] = self.exec_mode
		if self.ncores and 'ncores' not in request:
			request['ncores'] = str(self.ncores)
		self.last_request = json.dumps(request)
		try:
			if not self.wisvalid(self.last_request):
				print("The workflow is not valid")
				return None
			self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, self.last_request)
			if return_value:
				raise RuntimeError(error)
			if newsession is not None:
				if len(newsession) == 0:
					self.session = None
				else:
					self.session = newsession
					self.cwd = '/'
			response = self.deserialize_response()
			if response is not None:
				for response_i in response['response']:
					if response_i['objclass'] == 'text':
						if response_i['objcontent'][0]['title'] == 'Output Cube':
							self.cube = response_i['objcontent'][0]['message']
							break
				for response_i in response['response']:
					if response_i['objclass'] == 'text':
						if response_i['objcontent'][0]['title'] == 'Current Working Directory':
							self.cwd = response_i['objcontent'][0]['message']
							break
		except Exception as e:
			print(get_linenumber(),"Something went wrong in submitting the request:", e)
			return None
		return self

	def wisvalid(self, workflow):
		"""wisvalid(workflow) -> bool : Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False.

		:param workflow: a JSON string or a Python dict containing an Ophidia workflow
		:type workflow: str or dict
		:returns: True or False
		:rtype: bool
		"""

		if workflow is None:
			return False
		w = None
		if isinstance(workflow, str):
			try:
				w = json.loads(workflow)
			except:
				return False
		elif isinstance(workflow, dict):
			w = workflow
		else:
			return False
		if 'name' not in w or not w['name']:
			return False
		if 'author' not in w or not w['author']:
			return False
		if 'abstract' not in w or not w['abstract']:
			return False
		if 'on_error' in w:
			try:
				if w['on_error'] != 'skip' and w['on_error'] != 'continue' and w['on_error'] != 'break' and (w['on_error'][:7] != 'repeat ' or not w['on_error'][7:].isdigit() or int(w['on_error'][7:]) < 0):
					return False
			except:
				return False
		if 'ncores' in w and not w['ncores'].isdigit():
			return False
		if 'exec_mode' in w and w['exec_mode'] != 'sync' and w['exec_mode'] != 'async':
			return False
		if 'tasks' not in w or not w['tasks']:
			return False
		import re

		pattern = re.compile('^[A-Za-z0-9_]+=')
		for task in w['tasks']:
			if 'name' not in task or not task['name']:
				return False
			if 'operator' not in task or not task['operator']:
				return False
			if 'arguments' in task and task['arguments']:
				for argument in task['arguments']:
					if not pattern.match(argument):
						return False
			if 'dependencies' in task and task['dependencies']:
				for dependency in task['dependencies']:
					if 'task' not in dependency or not dependency['task']:
						return False
					if 'type' in dependency:
						if dependency['type'] != 'all' and dependency['type'] != 'single' and dependency['type'] != 'embedded':
							return False
			if 'on_error' in task:
				try:
					if task['on_error'] != 'skip' and task['on_error'] != 'continue' and task['on_error'] != 'break' and (task['on_error'][:7] != 'repeat ' or not task['on_error'][7:].isdigit() or int(task['on_error'][7:]) < 0):
						return False
				except:
					return False
		del re

		for index, task in enumerate(w['tasks']):
			if 'dependencies' in task and task['dependencies']:
				for dependency in task['dependencies']:
					if dependency['task'] == task['name']:
						return False
					for index2, task2 in enumerate(w['tasks']):
						if dependency['task'] == task2['name']:
							dependency['task_index'] = index2
							if 'dependents_indexes' not in task2 or not task2['dependents_indexes']:
								task2['dependents_indexes'] = []
							task2['dependents_indexes'].append(index)
							break
					else:
						return False

		class WorkflowNode():
			def __init__(self):
				self.out_edges = []
				self.out_edges_num = 0
				self.in_edges = []
				self.in_edges_num = 0
				self.index = 0

		graph = []
		for index, task in enumerate(w['tasks']):
			node = WorkflowNode()
			if 'dependencies' in task and task['dependencies']:
				node.in_edges_num = len(task['dependencies'])
				for dependency in task['dependencies']:
					node.in_edges.append(dependency['task_index'])
			if 'dependents_indexes' in task and task['dependents_indexes']:
				node.out_edges_num = len(task['dependents_indexes'])
				for dependent in task['dependents_indexes']:
					node.out_edges.append(dependent)
			node.index = index
			graph.append(node)

		# Test for DAG through Topological Sort
		#
		#	S <- Set of all nodes with no incoming edges
		#	while S is non-empty do
		#	    remove a node n from S
		#	    for each node m with an edge e from n to m do
		#	        remove edge e from the graph
		#	        if m has no other incoming edges then
		#	            insert m into S
		#	if graph has edges then
		#	    return error (graph has at least one cycle)
		#	else
		#	    return success (graph has no cycles)

		#	S <- Set of all nodes with no incoming edges
		S = []
		for node in graph:
			if node.in_edges_num == 0:
				S.append(node)

		#	while S is non-empty do
		while len(S) != 0:
			#	remove a node n from S
			n = S.pop()
			assert isinstance(n, WorkflowNode)
			#	for each node m with an edge e from n to m do
			for i in range(len(n.out_edges)):
				if n.out_edges[i] != -1:
					#	remove edge e from the graph
					index = n.out_edges[i]
					n.out_edges[i] = -1
					n.out_edges_num -= 1
					for j in range(len(graph[index].in_edges)):
						if graph[index].in_edges[j] == n.index:
							graph[index].in_edges[j] = -1
							graph[index].in_edges_num -= 1
							#	if m has no other incoming edges then
							if graph[index].in_edges_num == 0:
								#	insert m into S
								S.append(graph[index])
							break

		for node in graph:
			#	if graph has edges then
			if node.in_edges_num != 0 or node.out_edges_num != 0:
				#	return error (graph has at least one cycle)
				return False

		#	else return success (graph has no cycles)
		return True
