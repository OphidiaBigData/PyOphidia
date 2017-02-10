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

import client as _client

del sys
del os

from inspect import currentframe


def get_linenumber():
	cf = currentframe()
	return __file__, cf.f_back.f_lineno


class Cube():
	"""Cube(container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-', imp_concept_level='c', import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-') -> obj or Cube(pid=None) -> obj

	Attributes:
		pid: cube PID
		creation_date: creation date of the cube
		measure: name of the variable imported into the cube
		measure_type: measure data type
		level: number of operations between the original imported cube and the actual cube
		nfragments: total number of fragments
		source_file: parent of the actual cube
		hostxcube: number of hosts associated with the cube
		dbmsxhost: number of DBMS instances on each host
		dbxdbms: number of databases for each DBMS
		fragxdb: number of fragments for each database
		rowsxfrag: number of rows for each fragment
		elementsxrow: number of elements for each row
		compressed: 'yes' for a compressed cube, 'no' otherwise
		size: size of the cube
		nelements: total number of elements
		dim_info: list of dict with information on each cube dimension

	Class Attributes:
		client: instance of class Client through which it is possible to submit all requests

	Methods:
		info() -> None : call OPH_CUBESIZE, OPH_CUBEELEMENTS and OPH_CUBESCHEMA to fill all Cube attributes
		exportnc(export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_EXPORTNC
		aggregate(operation=None, container=None, grid='-', group_size='all', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_AGGREGATE
		aggregate2(dim=None, operation=None, concept_level='A', container=None, grid='-', midnight='24', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_AGGREGATE2
		apply(query=None, check_type='yes', compressed='auto', container=None, dim_query='null', dim_type='manual', measure='null', measure_type='manual', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_APPLY
		concatnc(src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_CONCATNC
		provenance(branch='all', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_CUBEIO
		delete(ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_DELETE
		drilldown(ndim=1, container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DRILLDOWN
		duplicate(container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DUPLICATE
		explore(level=1, limit_filter=10, output_path='default', output_name='default', show_id='no', show_index='no', show_time='no', subset_dims=None, subset_filter=None, exec_mode='sync', ncores=1) -> dict or None : wrapper of the operator OPH_EXPLORECUBE
		intercube(cube2=None, operation=None, output_measure=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_INTERCUBE
		merge(nmerge=0, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_MERGE
		metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_METADATA
		permute(dim_pos=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_PERMUTE
		reduce(operation=None, container=None, exec_mode='sync', grid='-', group_size='all', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE
		reduce2(dim=None, operation=None, concept_level='A', container=None, exec_mode='sync', grid='-', midnight='24', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE2
		rollup(ndim=1, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_ROLLUP
		split(nsplit=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_SPLIT
		subset(subset_dims=None, subset_filter=None, container=None, exec_mode='sync', grid='-', ncores=1) -> Cube or None : wrapper of the operator OPH_SUBSET
		subset2(subset_dims=None, subset_filter=None, grid='-', container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_SUBSET2

	Class Methods:
		setclient(username, password, server, port='11732') -> None : Instantiate the Client, common for all Cube objects, for submitting requests
		createcontainer(container=None, cwd=None, dim=None, dim_type=None, base_time='1900 - 01 - 01 00:00:00', calendar='standard', compressed='no', hierarchy='oph_base',leap_month=2,leap_year=0, month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', ncores=1, units='d', vocabulary='-', exec_mode='sync') -> dict or None : wrapper of the operator OPH_CREATECONTAINER
		deletecontainer(container=None, cwd=None, delete_type='physical', hidden='no', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_DELETECONTAINER
		folder(command=None, cwd=None, path=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_FOLDER
		list(level=1, path=None, container_filter=None, cube=None, host_filter=None, dbms_filter=None, db_filter=None, measure_filter=None, ntransform=None, src_filter=None, recursive='no', hidden='no', cwd=None, ncores=1,exec_mode='sync') -> dict or None : wrapper of the operator OPH_LIST
		man(function=None, function_type='operator', function_version='latest', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_MAN
		movecontainer(container=None, cwd=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_MOVECONTAINER
		operators(operator_filter=None, limit_filter=0, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_OPERATORS_LIST
		primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_PRIMITIVES_LIST
		restorecontainer(container=None, cwd=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_RESTORECONTAINER
		script(script=None, args=None, stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_SCRIPT
		mergecubes(cubes=None, container=None, exec_mode='sync', ncores=1) -> Cube : wrapper of the operator OPH_MERGECUBES
	"""

	client = None

	@classmethod
	def setclient(cls, username, password, server, port='11732'):
		"""setclient(username, password, server, port='11732') -> None : Instantiate the Client, common for all Cube objects, for submitting requests

		:param username: Ophidia user
		:type username: str
		:param password: Ophidia password
		:type password: str
		:param server: Ophidia server address
		:type server: str
		:param port: Ophidia server port
		:type port: str
		:returns: None
		:rtype: None
		"""

		try:
			cls.client = _client.Client(username, password, server, port)
		except Exception as e:
			print(get_linenumber(), "Something went wrong in setting the client:", e)
		finally:
			pass

	@classmethod
	def createcontainer(cls, container=None, cwd=None, dim=None, dim_type=None, base_time='1900-01-01 00:00:00', calendar='standard', compressed='no', hierarchy='oph_base',leap_month=2,leap_year=0, month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', ncores=1, units='d', vocabulary='-', exec_mode='sync'):
		"""createcontainer(container=None, cwd=None, dim=None, dim_type=None, base_time='1900 - 01 - 01 00:00:00', calendar='standard', compressed='no', hierarchy='oph_base',leap_month=2,leap_year=0, month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', ncores=1, units='d', vocabulary='-', exec_mode='sync') -> dict or None : wrapper of the operator OPH_CREATECONTAINER

		:param container: container name
		:type container: str
		:param cwd: current working directory
		:type cwd: str
		:param dim: pipe (|) separated list of dimension names
		:type dim: str
		:param dim_type: pipe (|) separated list of dimension types (int|float|long|double)
		:type dim_type: str
		:param base_time: reference time
		:type base_time: str
		:param calendar: calendar used
		:type calendar: str
		:param compressed: yes or no
		:type compressed: str
		:param hierarchy: pipe (|) separated list of dimension hierarchies (oph_base|oph_time)
		:type hierarchy: str
		:param leap_month: leap month
		:type leap_month: int
		:param leap_year: leap year
		:type leap_year: int
		:param month_lengths: comma-separated list of month lengths
		:type month_lengths: str
		:param ncores: number of cores to use
		:type ncores: int
		:param units: unit of time
		:type units: str
		:param vocabulary: metadata vocabulary
		:type vocabulary: str
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if container is None or dim is None or dim_type is None:
				raise RuntimeError('one or more required parameters are None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_createcontainer '

			if container is not None:
				query += 'container=' + str(container) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if dim is not None:
				query += 'dim=' + str(dim) + ';'
			if dim_type is not None:
				query += 'dim_type=' + str(dim_type) + ';'
			if base_time is not None:
				query += 'base_time=' + str(base_time) + ';'
			if calendar is not None:
				query += 'calendar=' + str(calendar) + ';'
			if compressed is not None:
				query += 'compressed=' + str(compressed) + ';'
			if hierarchy is not None:
				query += 'hierarchy=' + str(hierarchy) + ';'
			if leap_month is not None:
				query += 'leap_month=' + str(leap_month) + ';'
			if leap_year is not None:
				query += 'leap_year=' + str(leap_year) + ';'
			if month_lengths is not None:
				query += 'month_lengths=' + str(month_lengths) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if units is not None:
				query += 'units=' + str(units) + ';'
			if vocabulary is not None:
				query += 'vocabulary=' + str(vocabulary) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def deletecontainer(cls, container=None, cwd=None, delete_type='physical', hidden='no', ncores=1, exec_mode='sync'):
		"""deletecontainer(container=None, cwd=None, delete_type='physical', hidden='no', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_DELETECONTAINER

		:param container: container name
		:type container: str
		:param cwd: current working directory
		:type cwd: str
		:param delete_type: logical or physical
		:type delete_type: str
		:param hidden: yes or no
		:type hidden: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if container is None:
				raise RuntimeError('container name is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_deletecontainer '

			if container is not None:
				query += 'container=' + str(container) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if delete_type is not None:
				query += 'delete_type=' + str(delete_type) + ';'
			if hidden is not None:
				query += 'hidden=' + str(hidden) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def folder(cls, command=None, cwd=None, path=None, ncores=1, exec_mode='sync'):
		"""folder(command=None, cwd=None, path=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_FOLDER

		:param command: cd|mkdir|mv|rm
		:type command: str
		:param cwd: current working directory
		:type cwd: str
		:param path: absolute or relative path
		:type path: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if command is None:
				raise RuntimeError('command is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_folder '

			if command is not None:
				query += 'command=' + str(command) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if path is not None:
				query += 'path=' + str(path) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def list(cls, level=1, path=None, container_filter=None, cube=None, host_filter=None, dbms_filter=None, db_filter=None, measure_filter=None, ntransform=None, src_filter=None, recursive='no', hidden='no', cwd=None, ncores=1,exec_mode='sync'):
		"""list(level=1, path=None, container_filter=None, cube=None, host_filter=None, dbms_filter=None, db_filter=None, measure_filter=None, ntransform=None, src_filter=None, recursive='no', hidden='no', cwd=None, ncores=1,exec_mode='sync') -> dict or None : wrapper of the operator OPH_LIST

		:param level: 0|1|2|3|4|5|6|7|8
		:type level: int
		:param path: absolute or relative path
		:type path: str
		:param container_filter: filter on container name
		:type container_filter: str
		:param cube: filter on cube
		:type cube: str
		:param host_filter: filter on host
		:type host_filter: str
		:param dbms_filter: filter on DBMS
		:type dbms_filter: str
		:param db_filter: filter on db
		:type db_filter: str
		:param measure_filter: filter on measure
		:type measure_filter: str
		:param ntransform: filter on cube level
		:type ntransform: int
		:param src_filter: filter on source file
		:type src_filter: str
		:param recursive: yes|no
		:type recursive: str
		:param hidden: yes|no
		:type hidden: str
		:param cwd: current working directory
		:type cwd: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_list '

			if level is not None:
				query += 'level=' + str(level) + ';'
			if path is not None:
				query += 'path=' + str(path) + ';'
			if container_filter is not None:
				query += 'container_filter=' + str(container_filter) + ';'
			if cube is not None:
				query += 'cube=' + str(cube) + ';'
			if host_filter is not None:
				query += 'host_filter=' + str(host_filter) + ';'
			if dbms_filter is not None:
				query += 'dbms_filter=' + str(dbms_filter) + ';'
			if db_filter is not None:
				query += 'db_filter=' + str(db_filter) + ';'
			if measure_filter is not None:
				query += 'measure_filter=' + str(measure_filter) + ';'
			if ntransform is not None:
				query += 'ntransform=' + str(ntransform) + ';'
			if src_filter is not None:
				query += 'src_filter=' + str(src_filter) + ';'
			if recursive is not None:
				query += 'recursive=' + str(recursive) + ';'
			if hidden is not None:
				query += 'hidden=' + str(hidden) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def man(cls, function=None, function_type='operator', function_version='latest', ncores=1, exec_mode='sync'):
		"""man(function=None, function_type='operator', function_version='latest', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_MAN

		:param function: operator or primitive name
		:type function: str
		:param function_type: operator|primitive
		:type function_type: str
		:param function_version: operator or primitive version
		:type function_version: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if function is None:
				raise RuntimeError('function is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_man '

			if function is not None:
				query += 'function=' + str(function) + ';'
			if function_type is not None:
				query += 'function_type=' + str(function_type) + ';'
			if function_version is not None:
				query += 'function_version=' + str(function_version) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def movecontainer(cls, container=None, cwd=None, ncores=1, exec_mode='sync'):
		"""movecontainer(container=None, cwd=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_MOVECONTAINER

		:param container: container name
		:type container: str
		:param cwd: current working directory
		:type cwd: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if container is None:
				raise RuntimeError('container is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_movecontainer '

			if container is not None:
				query += 'container=' + str(container) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def operators(cls, operator_filter=None, limit_filter=0, ncores=1, exec_mode='sync'):
		"""operators(operator_filter=None, limit_filter=0, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_OPERATORS_LIST

		:param operator_filter: filter on operator name
		:type operator_filter: str
		:param limit_filter: max number of lines
		:type limit_filter: int
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_operators_list '

			if operator_filter is not None:
				query += 'operator_filter=' + str(operator_filter) + ';'
			if limit_filter is not None:
				query += 'limit_filter=' + str(limit_filter) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def primitives(cls, dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, ncores=1, exec_mode='sync'):
		"""primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_PRIMITIVES_LIST

		:param dbms_filter: filter on DBMS
		:type dbms_filter: str
		:param level: 1|2|3|4|5
		:type level: int
		:param limit_filter: max number of lines
		:type limit_filter: int
		:param primitive_filter: filter on primitive name
		:type primitive_filter: str
		:param primitive_type: all|simple|aggregate
		:type primitive_type: str
		:param return_type: all|array|number
		:type return_type: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_primitives_list '

			if dbms_filter is not None:
				query += 'dbms_filter=' + str(dbms_filter) + ';'
			if level is not None:
				query += 'level=' + str(level) + ';'
			if limit_filter is not None:
				query += 'limit_filter=' + str(limit_filter) + ';'
			if primitive_filter is not None:
				query += 'primitive_filter=' + str(primitive_filter) + ';'
			if primitive_type is not None:
				query += 'primitive_type=' + str(primitive_type) + ';'
			if return_type is not None:
				query += 'return_type=' + str(return_type) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def restorecontainer(cls, container=None, cwd=None, ncores=1, exec_mode='sync'):
		"""restorecontainer(container=None, cwd=None, ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_RESTORECONTAINER

		:param container: container name
		:type container: str
		:param cwd: current working directory
		:type cwd: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if container is None:
				raise RuntimeError('container is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_restorecontainer '

			if container is not None:
				query += 'container=' + str(container) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def script(cls, script=None, args=None, stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync'):
		"""script(script=None, args=None, stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_SCRIPT

		:param script: script/executable filename
		:type script: str
		:param args: pipe (|) separated list of arguments for the script
		:type args: str
		:param stdout: file/stream where stdout is redirected
		:type stdout: str
		:param stderr: file/stream where stderr is redirected
		:type stderr: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		response = None
		try:
			if script is None:
				raise RuntimeError('script is None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_script '

			if script is not None:
				query += 'script=' + str(script) + ';'
			if args is not None:
				query += 'args=' + str(args) + ';'
			if stdout is not None:
				query += 'stdout=' + str(stdout) + ';'
			if stderr is not None:
				query += 'stderr=' + str(stderr) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'

			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	@classmethod
	def mergecubes(cls, cubes=None, container=None, exec_mode='sync', ncores=1):
		"""mergecubes(cubes=None, container=None, exec_mode='sync', ncores=1) -> Cube : wrapper of the operator OPH_MERGECUBES

		:param cubes: pipe (|) separated list of cubes
		:type cubes: str
		:param container: optional container name
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or cubes is None:
			raise RuntimeError('Cube.client is None or cubes is None')
		newcube = None

		query = 'oph_mergecubes '

		if cubes is not None:
			query += 'cubes=' + str(cubes) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def __init__(self, container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-', imp_concept_level='c', import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-', pid=None):
		"""Cube(container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-', imp_concept_level='c', import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-') -> obj or Cube(pid=None) -> obj

		:param container: container name
		:type container: str
		:param cwd: current working directory
		:type cwd: str
		:param exp_dim: pipe (|) separated list of explicit dimension names
		:type exp_dim: str
		:param host_partition: host partition name
		:type host_partition: str
		:param imp_dim: pipe (|) separated list of implicit dimension names
		:type imp_dim: str
		:param measure: measure to be imported
		:type measure: str
		:param src_path: path of file to be imported
		:type src_path: str
		:param compressed: yes|no
		:type compressed: str
		:param exp_concept_level: pipe (|) separated list of explicit dimensions hierarchy levels
		:type exp_concept_level: str
		:param filesystem: local|global
		:type filesystem: str
		:param grid: optionally group dimensions in a grid
		:type grid: str
		:param imp_concept_level: pipe (|) separated list of implicit dimensions hierarchy levels
		:type imp_concept_level: str
		:param import_metadata: yes|no
		:type import_metadata: str
		:param check_compliance: yes|no
		:type check_compliance: str
		:param ioserver: mysql_table|tcpip_memory
		:type ioserver: str
		:param ndb: number of db/dbms to use
		:type ndb: int
		:param ndbms: number of dbms/host to use
		:type ndbms: int
		:param nfrag: number of fragments/db to use
		:type nfrag: int
		:param nhost: number of hosts to use
		:type nhost: int
		:param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
		:type subset_dims: str
		:param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
		:type subset_filter: str
		:param subset_type: index|coord
		:type subset_type: str
		:param base_time: reference time
		:type base_time: str
		:param calendar: calendar used
		:type calendar: str
		:param hierarchy: pipe (|) separated list of dimension hierarchies (oph_base|oph_time)
		:type hierarchy: str
		:param leap_month: leap month
		:type leap_month: int
		:param leap_year: leap year
		:type leap_year: int
		:param month_lengths: comma-separated list of month lengths
		:type month_lengths: str
		:param run: yes|no
		:type run: str
		:param units: unit of time
		:type units: str
		:param vocabulary: metadata vocabulary
		:type vocabulary: str
		:param pid: PID of an existing cube (if used all other parameters are ignored)
		:type pid: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: None
		:rtype: None
		:raises: RuntimeError
		"""

		self.pid = None
		self.creation_date = None
		self.measure = None
		self.measure_type = None
		self.level = None
		self.nfragments = None
		self.source_file = None
		self.hostxcube = None
		self.dbmsxhost = None
		self.dbxdbms = None
		self.fragxdb = None
		self.rowsxfrag = None
		self.elementsxrow = None
		self.compressed = None
		self.size = None
		self.nelements = None
		self.dim_info = None

		if pid is not None:
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')
			self.pid = pid
			try:
				self.info()
			except Exception as e:
				print(get_linenumber(),"Something went wrong in instantiating the cube", e)
			finally:
				pass
		else:
			if exp_dim is None or imp_dim is None or measure is None or src_path is None:
				raise RuntimeError('one or more required parameters are None')
			if Cube.client is None:
				raise RuntimeError('Cube.client is None')

			query = 'oph_importnc '

			if container is not None:
				query += 'container=' + str(container) + ';'
			if cwd is not None:
				query += 'cwd=' + str(cwd) + ';'
			if exp_dim is not None:
				query += 'exp_dim=' + str(exp_dim) + ';'
			if host_partition is not None:
				query += 'host_partition=' + str(host_partition) + ';'
			if imp_dim is not None:
				query += 'imp_dim=' + str(imp_dim) + ';'
			if measure is not None:
				query += 'measure=' + str(measure) + ';'
			if src_path is not None:
				query += 'src_path=' + str(src_path) + ';'
			if compressed is not None:
				query += 'compressed=' + str(compressed) + ';'
			if exp_concept_level is not None:
				query += 'exp_concept_level=' + str(exp_concept_level) + ';'
			if filesystem is not None:
				query += 'filesystem=' + str(filesystem) + ';'
			if grid is not None:
				query += 'grid=' + str(grid) + ';'
			if imp_concept_level is not None:
				query += 'imp_concept_level=' + str(imp_concept_level) + ';'
			if import_metadata is not None:
				query += 'import_metadata=' + str(import_metadata) + ';'
			if check_compliance is not None:
				query += 'check_compliance=' + str(check_compliance) + ';'
			if ioserver is not None:
				query += 'ioserver=' + str(ioserver) + ';'
			if ncores is not None:
				query += 'ncores=' + str(ncores) + ';'
			if ndb is not None:
				query += 'ndb=' + str(ndb) + ';'
			if ndbms is not None:
				query += 'ndbms=' + str(ndbms) + ';'
			if nfrag is not None:
				query += 'nfrag=' + str(nfrag) + ';'
			if nhost is not None:
				query += 'nhost=' + str(nhost) + ';'
			if subset_dims is not None:
				query += 'subset_dims=' + str(subset_dims) + ';'
			if subset_filter is not None:
				query += 'subset_filter=' + str(subset_filter) + ';'
			if subset_type is not None:
				query += 'subset_type=' + str(subset_type) + ';'
			if exec_mode is not None:
				query += 'exec_mode=' + str(exec_mode) + ';'
			if base_time is not None:
				query += 'base_time=' + str(base_time) + ';'
			if calendar is not None:
				query += 'calendar=' + str(calendar) + ';'
			if hierarchy is not None:
				query += 'hierarchy=' + str(hierarchy) + ';'
			if leap_month is not None:
				query += 'leap_month=' + str(leap_month) + ';'
			if leap_year is not None:
				query += 'leap_year=' + str(leap_year) + ';'
			if month_lengths is not None:
				query += 'month_lengths=' + str(month_lengths) + ';'
			if run is not None:
				query += 'run=' + str(run) + ';'
			if units is not None:
				query += 'units=' + str(units) + ';'
			if vocabulary is not None:
				query += 'vocabulary=' + str(vocabulary) + ';'

			try:
				if Cube.client.submit(query) is None:
					raise RuntimeError()

				if Cube.client.last_response is not None:
					if Cube.client.cube:
						self.pid = Cube.client.cube
						self.info()
			except Exception as e:
				print(get_linenumber(), "Something went wrong in instantiating the cube", e)
				raise RuntimeError()
			else:
				if self.pid:
					print("New cube is " + self.pid)

	def __del__(self):
		del self.pid
		del self.creation_date
		del self.measure
		del self.measure_type
		del self.level
		del self.nfragments
		del self.source_file
		del self.hostxcube
		del self.dbmsxhost
		del self.dbxdbms
		del self.fragxdb
		del self.rowsxfrag
		del self.elementsxrow
		del self.compressed
		del self.size
		del self.nelements
		del self.dim_info

	def info(self):
		"""info() -> None : call OPH_CUBESIZE, OPH_CUBEELEMENTS and OPH_CUBESCHEMA to fill all Cube attributes

		:returns: None
		:rtype: None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		query = 'oph_cubesize exec_mode=sync;cube=' + str(self.pid) + ';'
		if Cube.client.submit(query) is None:
			raise RuntimeError()
		query = 'oph_cubeelements exec_mode=sync;cube=' + str(self.pid) + ';'
		if Cube.client.submit(query) is None:
			raise RuntimeError()
		query = 'oph_cubeschema exec_mode=sync;cube=' + str(self.pid) + ';'
		if Cube.client.submit(query) is None:
			raise RuntimeError()
		res = Cube.client.deserialize_response()
		if res is not None:
			for res_i in res['response']:
				if res_i['objkey'] == 'cubeschema_cubeinfo':
					self.pid = res_i['objcontent'][0]['rowvalues'][0][0]
					self.creation_date = res_i['objcontent'][0]['rowvalues'][0][1]
					self.measure = res_i['objcontent'][0]['rowvalues'][0][2]
					self.measure_type = res_i['objcontent'][0]['rowvalues'][0][3]
					self.level = res_i['objcontent'][0]['rowvalues'][0][4]
					self.nfragments = res_i['objcontent'][0]['rowvalues'][0][5]
					self.source_file = res_i['objcontent'][0]['rowvalues'][0][6]
				elif res_i['objkey'] == 'cubeschema_morecubeinfo':
					self.hostxcube = res_i['objcontent'][0]['rowvalues'][0][1]
					self.dbmsxhost = res_i['objcontent'][0]['rowvalues'][0][2]
					self.dbxdbms = res_i['objcontent'][0]['rowvalues'][0][3]
					self.fragxdb = res_i['objcontent'][0]['rowvalues'][0][4]
					self.rowsxfrag = res_i['objcontent'][0]['rowvalues'][0][5]
					self.elementsxrow = res_i['objcontent'][0]['rowvalues'][0][6]
					self.compressed = res_i['objcontent'][0]['rowvalues'][0][7]
					self.size = res_i['objcontent'][0]['rowvalues'][0][8] + ' ' + res_i['objcontent'][0]['rowvalues'][0][9]
					self.nelements = res_i['objcontent'][0]['rowvalues'][0][10]
				elif res_i['objkey'] == 'cubeschema_diminfo':
					self.dim_info = list()
					for row_i in res_i['objcontent'][0]['rowvalues']:
						element = dict()
						element['name'] = row_i[0]
						element['type'] = row_i[1]
						element['size'] = row_i[2]
						element['hierarchy'] = row_i[3]
						element['concept_level'] = row_i[4]
						element['array'] = row_i[5]
						element['level'] = row_i[6]
						element['lattice_name'] = row_i[7]
						self.dim_info.append(element)

	def exportnc(self, export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync'):
		"""exportnc(export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_EXPORTNC

		:param export_metadata: yes|no
		:type export_metadata: str
		:param force: yes|no
		:type force: str
		:param output_path: directory of the output file
		:type output_path: str
		:param output_name: name of the output file
		:type output_name: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: None
		:rtype: None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')

		query = 'oph_exportnc '

		if export_metadata is not None:
			query += 'export_metadata=' + str(export_metadata) + ';'
		if force is not None:
			query += 'force=' + str(force) + ';'
		if output_path is not None:
			query += 'output_path=' + str(output_path) + ';'
		if output_name is not None:
			query += 'output_name=' + str(output_name) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()

	def aggregate(self, operation=None, container=None, grid='-', group_size='all', ncores=1, exec_mode='sync'):
		"""aggregate(operation=None, container=None, grid='-', group_size='all', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_AGGREGATE

		:param operation: max|min|avg|sum
		:type operation: str
		:param container: optional container name
		:type container: str
		:param grid: optionally group dimensions in a grid
		:type grid: str
		:param group_size: number of tuples per group to consider in the aggregation function
		:type group_size: int or str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or operation is None:
			raise RuntimeError('Cube.client is None or pid is None or operation is None')
		newcube = None

		query = 'oph_aggregate '

		if operation is not None:
			query += 'operation=' + str(operation) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if group_size is not None:
			query += 'group_size=' + str(group_size) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def aggregate2(self, dim=None, operation=None, concept_level='A', container=None, grid='-', midnight='24', ncores=1, exec_mode='sync'):
		"""aggregate2(dim=None, operation=None, concept_level='A', container=None, grid='-', midnight='24', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_AGGREGATE2

		:param dim: name of dimension on which the operation will be applied
		:type dim: str
		:param operation: max|min|avg|sum
		:type operation: str
		:param concept_level: concept level inside the hierarchy used for the operation
		:type concept_level: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param midnight: 00|24
		:type midnight: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or dim is None or operation is None:
			raise RuntimeError('Cube.client is None or pid is None or dim is None or operation is None')
		newcube = None

		query = 'oph_aggregate2 '

		if dim is not None:
			query += 'dim=' + str(dim) + ';'
		if operation is not None:
			query += 'operation=' + str(operation) + ';'
		if concept_level is not None:
			query += 'concept_level=' + str(concept_level) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if midnight is not None:
			query += 'midnight=' + str(midnight) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def apply(self, query=None, check_type='yes', compressed='auto', container=None, dim_query='null', dim_type='manual', measure='null', measure_type='manual', ncores=1, exec_mode='sync'):
		"""apply(query=None, check_type='yes', compressed='auto', container=None, dim_query='null', dim_type='manual', measure='null', measure_type='manual', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_APPLY

		:param query: query to be submitted
		:type query: str
		:param check_type: yes|no
		:type check_type: str
		:param compressed: yes|no|auto
		:type compressed: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param dim_query: optional query on dimension values
		:type dim_query: str
		:param dim_type: auto|manual
		:type dim_type: str
		:param measure: name of the new measure resulting from the specified operation
		:type measure: str
		:param measure_type: auto|manual
		:type measure_type: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or query is None:
			raise RuntimeError('Cube.client is None or pid is None or query is None')
		newcube = None

		internal_query = 'oph_apply '

		if query is not None:
			internal_query += 'query=' + str(query) + ';'
		if check_type is not None:
			internal_query += 'check_type=' + str(check_type) + ';'
		if compressed is not None:
			internal_query += 'compressed=' + str(compressed) + ';'
		if container is not None:
			internal_query += 'container=' + str(container) + ';'
		if dim_query is not None:
			internal_query += 'dim_query=' + str(dim_query) + ';'
		if dim_type is not None:
			internal_query += 'dim_type=' + str(dim_type) + ';'
		if measure is not None:
			internal_query += 'measure=' + str(measure) + ';'
		if measure_type is not None:
			internal_query += 'measure_type=' + str(measure_type) + ';'
		if ncores is not None:
			internal_query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			internal_query += 'exec_mode=' + str(exec_mode) + ';'

		internal_query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(internal_query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def concatnc(self, src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync'):
		"""concatnc(src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_CONCATNC

		:param src_path: file to be concatenated
		:type src_path: str
		:param check_exp_dim: yes|no
		:type check_exp_dim: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param import_metadata: yes|no
		:type import_metadata: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or src_path is None:
			raise RuntimeError('Cube.client is None or pid is None or src_path is None')
		newcube = None

		query = 'oph_concatnc '

		if src_path is not None:
			query += 'src_path=' + str(src_path) + ';'
		if check_exp_dim is not None:
			query += 'check_exp_dim=' + str(check_exp_dim) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if import_metadata is not None:
			query += 'import_metadata=' + str(import_metadata) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def provenance(self, branch='all', ncores=1, exec_mode='sync'):
		"""provenance(branch='all', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_CUBEIO

		:param branch: parent|children|all
		:type branch: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		response = None

		query = 'oph_cubeio '

		if branch is not None:
			query += 'branch=' + str(branch) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	def delete(self, ncores=1, exec_mode='sync'):
		"""delete(ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_DELETE

		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: None
		:rtype: None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')

		query = 'oph_delete '

		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()

	def drilldown(self, ndim=1, container=None, ncores=1, exec_mode='sync'):
		"""drilldown(ndim=1, container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DRILLDOWN

		:param ndim: number of implicit dimensions that will be transformed in explicit dimensions
		:type ndim: int
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_drilldown '

		if ndim is not None:
			query += 'ndim=' + str(ndim) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def duplicate(self, container=None, ncores=1, exec_mode='sync'):
		"""duplicate(container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DUPLICATE

		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_duplicate '

		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def explore(self, level=1, limit_filter=10, output_path='default', output_name='default', show_id='no', show_index='no', show_time='no', subset_dims=None, subset_filter=None, exec_mode='sync', ncores=1):
		"""explore(level=1, limit_filter=10, output_path='default', output_name='default', show_id='no', show_index='no', show_time='no', subset_dims=None, subset_filter=None, exec_mode='sync', ncores=1) -> dict or None : wrapper of the operator OPH_EXPLORECUBE

		:param level: 1|2
		:type level: int
		:param limit_filter: max number of rows
		:type limit_filter: int
		:param output_path: absolute path of the JSON Response
		:type output_path: str
		:param output_name: filename of the JSON Response
		:type output_name: str
		:param show_id: yes|no
		:type show_id: str
		:param show_index: yes|no
		:type show_index: str
		:param show_time: yes|no
		:type show_time: str
		:param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
		:type subset_dims: str
		:param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
		:type subset_filter: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		response = None

		query = 'oph_explorecube '

		if level is not None:
			query += 'level=' + str(level) + ';'
		if limit_filter is not None:
			query += 'limit_filter=' + str(limit_filter) + ';'
		if output_path is not None:
			query += 'output_path=' + str(output_path) + ';'
		if output_name is not None:
			query += 'output_name=' + str(output_name) + ';'
		if show_id is not None:
			query += 'show_id=' + str(show_id) + ';'
		if show_index is not None:
			query += 'show_index=' + str(show_index) + ';'
		if show_time is not None:
			query += 'show_time=' + str(show_time) + ';'
		if subset_dims is not None:
			query += 'subset_dims=' + str(subset_dims) + ';'
		if subset_filter is not None:
			query += 'subset_filter=' + str(subset_filter) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	def intercube(self, cube2=None, operation=None, output_measure=None, container=None, exec_mode='sync', ncores=1):
		"""intercube(cube2=None, operation=None, output_measure=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_INTERCUBE

		:param cube2: PID of the second cube
		:type cube2: str
		:param operation: sum|sub|mul|div|abs|arg|corr|mask
		:type operation: str
		:param output_measure: new measure name
		:type output_measure: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or cube2 is None or operation is None or output_measure is None:
			raise RuntimeError('Cube.client is None or pid is None or cube2 is None or operation is None or output_measure is None')
		newcube = None

		query = 'oph_intercube '

		if cube2 is not None:
			query += 'cube2=' + str(cube2) + ';'
		if operation is not None:
			query += 'operation=' + str(operation) + ';'
		if output_measure is not None:
			query += 'output_measure=' + str(output_measure) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def merge(self, nmerge=0, container=None, exec_mode='sync', ncores=1):
		"""merge(nmerge=0, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_MERGE

		:param nmerge: number of input fragments to merge in an output fragment, 0 for all
		:type nmerge: int
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_merge '

		if nmerge is not None:
			query += 'nmerge=' + str(nmerge) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def metadata(self, mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', ncores=1, exec_mode='sync'):
		"""metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_METADATA

		:param mode: insert|read|update|delete
		:type mode: str
		:param metadata_id: id of the particular metadata instance to interact with
		:type metadata_id: int
		:param metadata_key: name of the key (or the enumeration of keys) identifying requested metadata
		:type metadata_key: str
		:param variable: name of the variable to which we can associate a new metadata key
		:type variable: str
		:param metadata_type: text|image|video|audio|url
		:type metadata_type: str
		:param metadata_value: string value to be assigned to specified metadata
		:type metadata_value: str
		:param metadata_type_filter: filter on metadata type
		:type metadata_type_filter: str
		:param metadata_value_filter: filter on metadata value
		:type metadata_value_filter: str
		:param force: force update or deletion of functional metadata associated to a vocabulary, default is no
		:type force: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: response or None
		:rtype: dict or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		response = None

		query = 'oph_metadata '

		if mode is not None:
			query += 'mode=' + str(mode) + ';'
		if metadata_id is not None:
			query += 'metadata_id=' + str(metadata_id) + ';'
		if metadata_key is not None:
			query += 'metadata_key=' + str(metadata_key) + ';'
		if variable is not None:
			query += 'variable=' + str(variable) + ';'
		if metadata_type is not None:
			query += 'metadata_type=' + str(metadata_type) + ';'
		if metadata_value is not None:
			query += 'metadata_value=' + str(metadata_value) + ';'
		if metadata_type_filter is not None:
			query += 'metadata_type_filter=' + str(metadata_type_filter) + ';'
		if metadata_value_filter is not None:
			query += 'metadata_value_filter=' + str(metadata_value_filter) + ';'
		if force is not None:
			query += 'force=' + str(force) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				response = Cube.client.deserialize_response()
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return response

	def permute(self, dim_pos=None, container=None, exec_mode='sync', ncores=1):
		"""permute(dim_pos=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_PERMUTE

		:param dim_pos: permutation of implicit dimensions as a comma-separated list of dimension levels
		:type dim_pos: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or dim_pos is None:
			raise RuntimeError('Cube.client is None or pid is None or dim_pos is None')
		newcube = None

		query = 'oph_permute '

		if dim_pos is not None:
			query += 'dim_pos=' + str(dim_pos) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def reduce(self, operation=None, container=None, exec_mode='sync', grid='-', group_size='all', ncores=1):
		"""reduce(operation=None, container=None, exec_mode='sync', grid='-', group_size='all', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE

		:param operation: max|min|avg|sum
		:type operation: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param group_size: size of the aggregation set, all for the entire array
		:type group_size: int or str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or operation is None:
			raise RuntimeError('Cube.client is None or pid is None or operation is None')
		newcube = None

		query = 'oph_reduce '

		if operation is not None:
			query += 'operation=' + str(operation) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if group_size is not None:
			query += 'group_size=' + str(group_size) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def reduce2(self, dim=None, operation=None, concept_level='A', container=None, exec_mode='sync', grid='-', midnight='24', ncores=1):
		"""reduce2(dim=None, operation=None, concept_level='A', container=None, exec_mode='sync', grid='-', midnight='24', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE2

		:param dim: name of dimension on which the operation will be applied
		:type dim: str
		:param operation: max|min|avg|sum
		:type operation: str
		:param concept_level: concept level inside the hierarchy used for the operation
		:type concept_level: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param midnight: 00|24
		:type midnight: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or dim is None or operation is None:
			raise RuntimeError('Cube.client is None or pid is None or dim is None or operation is None')
		newcube = None

		query = 'oph_reduce2 '

		if dim is not None:
			query += 'dim=' + str(dim) + ';'
		if operation is not None:
			query += 'operation=' + str(operation) + ';'
		if concept_level is not None:
			query += 'concept_level=' + str(concept_level) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if midnight is not None:
			query += 'midnight=' + str(midnight) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def rollup(self, ndim=1, container=None, exec_mode='sync', ncores=1):
		"""rollup(ndim=1, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_ROLLUP

		:param ndim: number of explicit dimensions that will be transformed in implicit dimensions
		:type ndim: int
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_rollup '

		if ndim is not None:
			query += 'ndim=' + str(ndim) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def split(self, nsplit=None, container=None, exec_mode='sync', ncores=1):
		"""split(nsplit=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_SPLIT

		:param nsplit: number of output fragments per input fragment
		:type nsplit: int
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None or nsplit is None:
			raise RuntimeError('Cube.client is None or pid is None or nsplit is None')
		newcube = None

		query = 'oph_split '

		if nsplit is not None:
			query += 'nsplit=' + str(nsplit) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def subset(self, subset_dims=None, subset_filter=None, container=None, exec_mode='sync', grid='-', ncores=1):
		"""subset(subset_dims=None, subset_filter=None, container=None, exec_mode='sync', grid='-', ncores=1) -> Cube or None : wrapper of the operator OPH_SUBSET

		:param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
		:type subset_dims: str
		:param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters on dimension indexes (e.g. 1,5,10:2:50)
		:type subset_filter: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_subset '

		if subset_dims is not None:
			query += 'subset_dims=' + str(subset_dims) + ';'
		if subset_filter is not None:
			query += 'subset_filter=' + str(subset_filter) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def subset2(self, subset_dims=None, subset_filter=None, grid='-', container=None, ncores=1, exec_mode='sync'):
		"""subset2(subset_dims=None, subset_filter=None, grid='-', container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_SUBSET2

		:param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
		:type subset_dims: str
		:param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters on dimension values (e.g. 30,5,10:50)
		:type subset_filter: str
		:param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
		:type grid: str
		:param container: name of the container to be used to store the output cube, by default it is the input container
		:type container: str
		:param ncores: number of cores to use
		:type ncores: int
		:param exec_mode: async or sync
		:type exec_mode: str
		:returns: new cube or None
		:rtype: Cube or None
		:raises: RuntimeError
		"""

		if Cube.client is None or self.pid is None:
			raise RuntimeError('Cube.client is None or pid is None')
		newcube = None

		query = 'oph_subset2 '

		if subset_dims is not None:
			query += 'subset_dims=' + str(subset_dims) + ';'
		if subset_filter is not None:
			query += 'subset_filter=' + str(subset_filter) + ';'
		if grid is not None:
			query += 'grid=' + str(grid) + ';'
		if container is not None:
			query += 'container=' + str(container) + ';'
		if ncores is not None:
			query += 'ncores=' + str(ncores) + ';'
		if exec_mode is not None:
			query += 'exec_mode=' + str(exec_mode) + ';'

		query += 'cube=' + str(self.pid) + ';'

		try:
			if Cube.client.submit(query) is None:
				raise RuntimeError()

			if Cube.client.last_response is not None:
				if Cube.client.cube:
					newcube = Cube(pid=Cube.client.cube)
		except Exception as e:
			print(get_linenumber(), "Something went wrong:", e)
			raise RuntimeError()
		else:
			return newcube

	def __str__(self):
		buf = "-" * 30 + "\n"
		buf += "%30s: %s" % ("Cube", self.pid) + "\n"
		buf += "-" * 30 + "\n"
		buf += "%30s: %s" % ("Creation Date", self.creation_date) + "\n"
		buf += "%30s: %s (%s)" % ("Measure (type)", self.measure, self.measure_type) + "\n"
		buf += "%30s: %s" % ("Source file", self.source_file) + "\n"
		buf += "%30s: %s" % ("Level", self.level) + "\n"
		if self.compressed == 'yes':
			buf += "%30s: %s (%s)" % ("Size", self.size, "compressed") + "\n"
		else:
			buf += "%30s: %s (%s)" % ("Size", self.size, "not compressed") + "\n"
		buf += "%30s: %s" % ("Num. of elements", self.nelements) + "\n"
		buf += "%30s: %s" % ("Num. of fragments", self.nfragments) + "\n"
		buf += "-" * 30 + "\n"
		buf += "%30s: %s" % ("Num. of hosts", self.hostxcube) + "\n"
		buf += "%30s: %s (%s)" % ("Num. of DBMSs/host (total)", self.dbmsxhost, int(self.dbmsxhost) * int(self.hostxcube)) + "\n"
		buf += "%30s: %s (%s)" % ("Num. of DBs/DBMS (total)", self.dbxdbms, int(self.dbxdbms) * int(self.dbmsxhost) * int(self.hostxcube)) + "\n"
		buf += "%30s: %s (%s)" % ("Num. of fragments/DB (total)", self.fragxdb, int(self.fragxdb) * int(self.dbxdbms) * int(self.dbmsxhost) * int(self.hostxcube)) + "\n"
		buf += "%30s: %s (%s)" % ("Num. of rows/fragment (total)", self.rowsxfrag, int(self.rowsxfrag) * int(self.fragxdb) * int(self.dbxdbms) * int(self.dbmsxhost) * int(self.hostxcube)) + "\n"
		buf += "%30s: %s (%s)" % ("Num. of elements/row (total)", self.elementsxrow, int(self.elementsxrow) * int(self.rowsxfrag) * int(self.fragxdb) * int(self.dbxdbms) * int(self.dbmsxhost) * int(self.hostxcube)) + "\n"
		buf += "-" * 127 + "\n"
		buf += "%15s %15s %15s %15s %15s %15s %15s %15s" % ("Dimension", "Data type", "Size", "Hierarchy", "Concept level", "Array", "Level", "Lattice name") + "\n"
		buf += "-" * 127 + "\n"
		for dim in self.dim_info:
			buf += "%15s %15s %15s %15s %15s %15s %15s %15s" % (dim['name'], dim['type'], dim['size'], dim['hierarchy'], dim['concept_level'], dim['array'], dim['level'], dim['lattice_name']) + "\n"
		buf += "-" * 127 + "\n"
		return buf

