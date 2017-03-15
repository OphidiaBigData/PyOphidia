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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
import client as _client
from inspect import currentframe
sys.path.append(os.path.dirname(__file__))


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


class Cube():
    """Cube(container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-', imp_concept_level='c',
       import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all', subset_type='index', exec_mode='sync',
       base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-')
       -> obj or Cube(pid=None) -> obj

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
        apply(query=None, check_type='yes', compressed='auto', container=None, dim_query='null', dim_type='manual', measure='null', measure_type='manual', ncores=1, exec_mode='sync')
        -> Cube or None : wrapper of the operator OPH_APPLY
        concatnc(src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_CONCATNC
        provenance(branch='all', exec_mode='sync') -> dict or None : wrapper of the operator OPH_CUBEIO
        delete(ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_DELETE
        Cubeschema(exec_mode='sync') -> None : wrapper of the operator OPH_CUBESCHEMA
        drilldown(ndim=1, container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DRILLDOWN
        duplicate(container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_DUPLICATE
        explore(level=1, limit_filter=10, output_path='default', output_name='default', show_id='no', show_index='no', show_time='no', subset_dims=None, subset_filter=None, exec_mode='sync', ncores=1)
        -> dict or None : wrapper of the operator OPH_EXPLORECUBE
        intercube(cube2=None, operation=None, output_measure=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_INTERCUBE
        merge(nmerge=0, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_MERGE
        publish  ( ncores=1, container=None,exec_mode='sync', content=None, show_id= 'no' ,show_index='no') -> None : wrapper of the operator OPH_PUBLISH
        unpublish ( ncores=1, container=None,exec_mode='sync', content=None) -> None : wrapper of the operator OPH_UNPUBLISH
        exportnc2 (export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync') -> None : wrapper of the operator OPH_EXPORTNC2
        metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', exec_mode='sync')
        -> dict or None : wrapper of the operator OPH_METADATA
        permute(dim_pos=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_PERMUTE
        reduce(operation=None, container=None, exec_mode='sync', grid='-', group_size='all', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE
        reduce2(dim=None, operation=None, concept_level='A', container=None, exec_mode='sync', grid='-', midnight='24', ncores=1) -> Cube or None : wrapper of the operator OPH_REDUCE2
        rollup(ndim=1, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_ROLLUP
        split(nsplit=None, container=None, exec_mode='sync', ncores=1) -> Cube or None : wrapper of the operator OPH_SPLIT
        subset(subset_dims=None, subset_filter=None, container=None, exec_mode='sync', grid='-', ncores=1) -> Cube or None : wrapper of the operator OPH_SUBSET
        subset2(subset_dims=None, subset_filter=None, grid='-', container=None, ncores=1, exec_mode='sync') -> Cube or None : wrapper of the operator OPH_SUBSET2



    Class Methods:
        setclient(username, password, server, port='11732') -> None : Instantiate the Client, common for all Cube objects, for submitting requests
        createcontainer(container=None, cwd=None, dim=None, dim_type=None, base_time='1900-01-01 00:00:00', calendar='standard', compressed='no', hierarchy='oph_base',leap_month=2,leap_year=0,
                        month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', units='d', vocabulary='-', exec_mode='sync') -> dict or None : wrapper of the operator OPH_CREATECONTAINER
        deletecontainer(container=None, cwd=None, delete_type='physical', hidden='no', exec_mode='sync') -> dict or None : wrapper of the operator OPH_DELETECONTAINER
        folder(command=None, cwd=None, path=None, exec_mode='sync') -> dict or None : wrapper of the operator OPH_FOLDER
        list(level=1, path=None, container_filter=None, cube=None, host_filter=None, dbms_filter=None, db_filter=None, measure_filter=None, ntransform=None, src_filter=None, recursive='no', hidden='no',
             cwd=None, exec_mode='sync') -> dict or None : wrapper of the operator OPH_LIST
        man(function=None, function_type='operator', function_version='latest', exec_mode='sync') -> dict or None : wrapper of the operator OPH_MAN
        movecontainer(container=None, cwd=None, exec_mode='sync') -> dict or None : wrapper of the operator OPH_MOVECONTAINER
        operators(operator_filter=None, limit_filter=0, exec_mode='sync') -> dict or None : wrapper of the operator OPH_OPERATORS_LIST
        primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, exec_mode='sync') -> dict or None : wrapper of the operator OPH_PRIMITIVES_LIST
        restorecontainer(container=None, cwd=None, exec_mode='sync') -> dict or None : wrapper of the operator OPH_RESTORECONTAINER
        script(script=None, args=None, stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync') -> dict or None : wrapper of the operator OPH_SCRIPT
        mergecubes(cubes=None, container=None, exec_mode='sync', ncores=1) -> Cube : wrapper of the operator OPH_MERGECUBES
        mergecubes2(cubes=None, container='-', dim='-', description='-', objkey_filter='all', schedule=0, exec_mode='sync', ncores=1, display=False) -> Cube : wrapper of the operator OPH_MERGECUBES2
        explorenc (force='no',measure= None, ncores=1, src_path= None, exp_dim= None, imp_dim= None , exec_mode='sync') -> None : wrapper of the operator OPH_EXPLORENC
        randcube (path=None, container=None, nhost=None ,ndbms=None, ndb=None, nfrag=None, ntuple=None, measure=None, measure_type='manual', exp_ndim=None,dim=None, dim_size=None , grid=None,compressed= 'auto' ,
                  concept_level=None, host_partition= None, filesystem='local',hidden='no', cwd=None, ncores=1,exec_mode='sync') -> dict or None : wrapper of the operator OPH_RANDCUBE
        intercomparison (cubes=None, container=None, exec_mode='sync', ncores=1) -> Cube : wrapper of the operator OPH_INTERCOMPARISON
        importnc3 (path=None, container=None, measure=None, src_path=None, imp_concept_level=None,import_metadata=None , concept_level=None, cwd=None, ncores=1,exec_mode='sync')
                   -> dict or None : wrapper of the operator OPH_IMPORTNC3
        Cancel( cores=1, id=None, type=None,  display=True) -> None : wrapper of the operator OPH_CANCEL.
        Hierarchy.(hierarchy='no', hierarchy_version='latest', exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_HIERARCHY.
        Resume_operator:( id=0, id_type='workflow', document_type='response', level=1, save='no', session='this', objkey_filter='all', user='', display=True) -> None : wrapper of the operator OPH_RESUME
        service: Request or set the service status.(cls, status='', objkey_filter='all', display=False) -> dict or None : wrapper of the operator OPH_SERVICE
        get_config: Request the configuration parameters. (cls, key='all', objkey_filter='all', display=False) -> dict or None : wrapper of the operator OPH_GET_CONFIG
        manage_session: Request or set session data: session list, session creation date, authorized users, etc.  (cls, key='all', objkey_filter='all', display=False) ->
                        dict or None : wrapper of the operator OPH_MANAGE_SESSION
        instances: It shows information about host partitions, hosts and dbms instances. (cls, level=1, host_filter='all', host_partition='all', filesystem_filter='all', ioserver='all',
                   host_status='all', dbms_status='all', exec_mode='async', objkey_filter='all', display=True) -> dict or None : wrapper of the operator OPH_INSTANCES
        log_info: It reads the last lines from the server log or from a specific container log.(cls, log_type='server', container_id=0, ioserver='mysql', nlines=10,
                  objkey_filter='all', exec_mode='sync', display=False) -> dict or None : wrapper of the operator OPH_LOG_INFO
        loggingbk: It shows info about jobs and sessions (logging bookkeeping). Any filter can use SQL wildcards (%, _, etc.).(cls, session_level=0, job_level=0,
                   mask=000, session_filter='all', session_label_filter='all', session_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', workflow_filter='all',
                   markerid_filter='all', parent_job_filter='all', job_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_status_filter='all', submission_string_filter='all',
                   job_start_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_end_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',  nlines=100, objkey_filter='all',
                   exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_LOGGINGBK.
        tasks ( cores=1, container=None,exec_mode='sync') -> None : wrapper of the operator OPH_TASKS
        showgrid ( cores=1, container=None,exec_mode='sync') -> None : wrapper of the operator OPH_SHOWGRID
        search ( mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', exec_mode='sync')
        -> dict or None : wrapper of the operator OPH_SEARCH
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
    def createcontainer(cls, exec_mode='sync', container=None, cwd=None, dim=None, dim_type=None, hierarchy='oph_base', base_time='1900-01-01 00:00:00', 
                         units='d', calendar='standard', month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', leap_year=0, leap_month=2, vocabulary='-', 
                         compressed='no', objkey_filter='all', display=True):
        """createcontainer(container=None, cwd=None, dim=None, dim_type=None, base_time='1900 - 01 - 01 00:00:00', calendar='standard', compressed='no', hierarchy='oph_base',leap_month=2,leap_year=0,
           month_lengths='31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31', units='d', vocabulary='-', exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_CREATECONTAINER

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
        :param units: unit of time
        :type units: str
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
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

            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'    
            if container is not None:
                query += 'container=' + str(container) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if dim is not None:
                query += 'dim=' + str(dim) + ';'
            if dim_type is not None:
                query += 'dim_type=' + str(dim_type) + ';'
            if hierarchy is not None:
                query += 'hierarchy=' + str(hierarchy) + ';'    
            if base_time is not None:
                query += 'base_time=' + str(base_time) + ';'
            if units is not None:
                query += 'units=' + str(units) + ';'    
            if calendar is not None:
                query += 'calendar=' + str(calendar) + ';'
            if month_lengths is not None:
                query += 'month_lengths=' + str(month_lengths) + ';'
            if leap_year is not None:
                query += 'leap_year=' + str(leap_year) + ';'
            if leap_month is not None:
                query += 'leap_month=' + str(leap_month) + ';'
            if vocabulary is not None:
                query += 'vocabulary=' + str(vocabulary) + ';'
            if compressed is not None:
                query += 'compressed=' + str(compressed) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def deletecontainer(cls, container=None, delete_type='physical', hidden='no', cwd=None, exec_mode='sync', objkey_filter='all', display=False):
        """deletecontainer(container=None, cwd=None, delete_type='physical', hidden='no', exec_mode='sync', display=False) -> dict or None : wrapper of the operator OPH_DELETECONTAINER

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param delete_type: logical or physical
        :type delete_type: str
        :param hidden: yes or no
        :type hidden: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
            if delete_type is not None:
                query += 'delete_type=' + str(delete_type) + ';'
            if hidden is not None:
                query += 'hidden=' + str(hidden) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def cancel(cls, id='', type='kill', objkey_filter='all', display=False):
        """cancel(command=None, cwd=None, path=None, exec_mode='sync', display=False) -> dict or None : wrapper of the operator OPH_CANCEL

        :param type: type of method
        :type type: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client or id is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_cancel '

            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def service(cls, status='', level=1, objkey_filter='all', display=False):
        """service: Request or set the service status.(cls, status='', objkey_filter='all', display=False) -> dict or None : wrapper of the operator OPH_SERVICE

        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_service '

            if status is not None:
                query += 'status=' + str(status) + ';'
            if level is not None:
                query += 'level=' + str(level) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def get_config(cls, key='all', objkey_filter='all', display=True):
        """get_config: Request the configuration parameters. (cls, key='all', objkey_filter='all', display=True) -> dict or None : wrapper of the operator OPH_GET_CONFIG

        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_get_config '

            if key is not None:
                query += 'key=' + str(key) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def manage_session(cls, action='', session='this', key='user', value='null', objkey_filter='all', display=False):
        """manage_session: Request or set session data: session list, session creation date, authorized users, etc.  (cls, key='all', objkey_filter='all', display=False) ->
           dict or None : wrapper of the operator OPH_MANAGE_SESSION

        :param action:request action
        :type action:str
        :param session: current session
        type: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_manage_session '

            if action is not None:
                query += 'action=' + str(action) + ';'
            if session is not None:
                query += 'session=' + str(session) + ';'
            if key is not None:
                query += 'key=' + str(key) + ';'
            if value is not None:
                query += 'value=' + str(value) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def instances(cls, level=1, host_filter='all', host_partition='all', filesystem_filter='all', ioserver='all', host_status='all',
                   dbms_status='all',exec_mode='sync', objkey_filter='all', display=True):
        """instances: It shows information about host partitions, hosts and dbms instances. (cls, level=1, host_filter='all', host_partition='all', filesystem_filter='all', ioserver='all',
           host_status='all', dbms_status='all', exec_mode='sync', objkey_filter='all', display=True) -> dict or None : wrapper of the operator OPH_INSTANCES

        :param action:request action
        :type action:str
        :param session: current session
        :type: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_instances '

            if level is not None:
                query += 'level=' + str(level) + ';'
            if host_filter is not None:
                query += 'host_filter=' + str(host_filter) + ';'
            if host_partition is not None:
                query += 'host_partition=' + str(host_partition) + ';'
            if filesystem_filter is not None:
                query += 'filesystem_filter=' + str(filesystem_filter) + ';'
            if ioserver is not None:
                query += 'ioserver=' + str(ioserver) + ';'
            if host_status is not None:
                query += 'host_status=' + str(host_status) + ';'
            if dbms_status is not None:
                query += 'dbms_status=' + str(dbms_status) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def log_info(cls, log_type='server', container_id=0, ioserver='mysql', nlines=10, exec_mode='sync', objkey_filter='all', display=True):
        """log_info: It reads the last lines from the server log or from a specific container log.(cls, log_type='server', container_id=0, ioserver='mysql', nlines=10,
           objkey_filter='all', exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_LOG_INFO

        :param type: type of method
        :type type: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client or id is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_log_info '

            if log_type is not None:
                query += 'log_type=' + str(log_type) + ';'
            if container_id is not None:
                query += 'container_id=' + str(container_id) + ';'
            if ioserver is not None:
                query += 'ioserver=' + str(ioserver) + ';'
            if nlines is not None:
                query += 'nlines=' + str(nlines) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def loggingbk(cls, session_level=0, job_level=0, mask=000, session_filter='all', session_label_filter='all', session_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',
                  workflow_filter='all', markerid_filter='all', parent_job_filter='all', job_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_status_filter='all',
                  submission_string_filter='all', job_start_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_end_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',
                  nlines=100, objkey_filter='all', exec_mode='sync', display=True):
        """loggingbk: It shows info about jobs and sessions (logging bookkeeping). Any filter can use SQL wildcards (%, _, etc.).(cls, session_level=0, job_level=0,
           mask=000, session_filter='all', session_label_filter='all', session_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', workflow_filter='all',
           markerid_filter='all', parent_job_filter='all', job_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_status_filter='all', submission_string_filter='all',
           job_start_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_end_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',  nlines=100, objkey_filter='all',
           exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_LOGGINGBK

        :param path: absolute or relative path
        :type path: str
        :param container: container name
        :type container: str
        :param cube: filter on cube
        :type cube: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')
            query = 'oph_loggingbk '

            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if session_level is not None:
                query += 'session_level=' + str(session_level) + ';'
            if job_level is not None:
                query += 'job_level=' + str(job_level) + ';'
            if mask is not None:
                query += 'mask=' + str(mask) + ';'
            if nlines is not None:
                query += 'nlines=' + str(nlines) + ';'
            if session_filter is not None:
                query += 'session_filter=' + str(session_filter) + ';'
            if session_label_filter is not None:
                query += 'session_label_filter=' + str(session_label_filter) + ';'
            if session_creation_filter is not None:
                query += 'session_creation_filter=' + str(session_creation_filter) + ';'
            if workflow_filter is not None:
                query += 'workflow_filter=' + str(workflow_filter) + ';'
            if markerid_filter is not None:
                query += 'markerid_filter=' + str(markerid_filter) + ';'
            if parent_job_filter is not None:
                query += 'parent_job_filter=' + str(parent_job_filter) + ';'
            if job_creation_filter is not None:
                query += 'job_creation_filter=' + str(job_creation_filter) + ';'
            if job_status_filter is not None:
                query += 'job_status_filter=' + str(job_status_filter) + ';'
            if submission_string_filter is not None:
                query += 'submission_string_filter=' + str(submission_string_filter) + ';'
            if job_start_filter is not None:
                query += 'job_start_filter=' + str(job_start_filter) + ';'
            if job_end_filter is not None:
                query += 'job_end_filter=' + str(job_end_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def folder(cls, command=None, path='-', cwd=None, exec_mode='sync', objkey_filter='all', display=False):
        """folder(command=None, cwd=None, path=None, exec_mode='sync', display=False) -> dict or None : wrapper of the operator OPH_FOLDER

        :param command: cd|mkdir|mv|rm
        :type command: str
        :param cwd: current working directory
        :type cwd: str
        :param path: absolute or relative path
        :type path: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
            if path is not None:
                query += 'path=' + str(path) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def tasks(cls, cube_filter='all', operator_filter='all', path='-', cwd=None, container='all', exec_mode='sync', objkey_filter='all', display=True):
        """tasks(cls, cube_filter='all', path='-', operator_filter='all', cwd=None, container='all', objkey_filter='all', exec_mode='sync', display=True) -> dict or None
           : wrapper of the operator OPH_tasks

        :param path: absolute or relative path
        :type path: str
        :param container: container name
        :type container: str
        :param cube: filter on cube
        :type cube: str
        :param hidden: yes|no
        :type hidden: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_tasks '

            
            if cube_filter is not None:
                query += 'cube_filter=' + str(cube_filter) + ';'
            if operator_filter is not None:
                query += 'operator_filter=' + str(operator_filter) + ';'
            if path is not None:
                query += 'path=' + str(path) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'    
            if container is not None:
                query += 'container=' + str(container) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def showgrid(cls, container=None, grid='all', dim='all', show_index='no', cwd=None, exec_mode='sync', objkey_filter='all', display=True):
        """ Show the grid 'grid1' related to container 'container1':( cores=1, container=None,exec_mode='sync', grid='all', dim='all', show_index='no', cwd=None,
            objkey_filter='all', display=True) -> None : wrapper of the operator OPH_SHOWGRID

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_showgrid '

            if container is not None:
                query += 'container=' + str(container) + ';'
            if grid is not None:
                query += 'grid=' + str(grid) + ';'
            if dim is not None:
                query += 'dim=' + str(dim) + ';'    
            if show_index is not None:
                query += 'show_index=' + str(show_index) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def search(cls, container_filter='all', metadata_key_filter='all', metadata_value_filter='all', path='-', cwd=None, exec_mode='sync', objkey_filter='all', display=True):
        """search(path='-',  metadata_value_filter='all', exec_mode='sync', metadata_key_filter='all', container_filter='all', objkey_filter='all', cwd=None, display=True) ->
           dict or None : wrapper of the operator OPH_SEARCH
        :param mode: insert|read|update|delete
        :type mode: str
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
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_search '

            if container_filter is not None:
                query += 'container_filter=' + str(container_filter) + ';'
            if metadata_key_filter is not None:
                query += 'metadata_key_filter=' + str(metadata_key_filter) + ';'
            if metadata_value_filter is not None:
                query += 'metadata_value_filter=' + str(metadata_value_filter) + ';'
            if path is not None:
                query += 'path=' + str(path) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def hierarchy(cls, hierarchy='all', hierarchy_version='latest', exec_mode='sync', display=True):
        """It shows the list of the hierarchies or the description of a specified hierarchy.(hierarchy='no', hierarchy_version='latest', exec_mode='sync', display=True) ->
           dict or None : wrapper of the operator OPH_HIERARCHY.

        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_hierarchy '

            if hierarchy is not None:
                query += 'hierarchy=' + str(hierarchy) + ';'
            if hierarchy_version is not None:
                query += 'hierarchy_version=' + str(hierarchy_version) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def list(cls, level=1, exec_mode='sync', path='-', cwd=None, container_filter='all', cube='all', host_filter='all', dbms_filter='all',  
             measure_filter='all', ntransform='all', src_filter='all', db_filter='all', recursive='no', hidden='no', objkey_filter='all', display=True):
        """list(level=1, path='-', container_filter='all', cube='all', host_filter='all', dbms_filter='all', db_filter='all', measure_filter='all', ntransform='all', src_filter='all', recursive='no',
           hidden='no', cwd=None, exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_LIST

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
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
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
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'

            if path is not None:
                query += 'path=' + str(path) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if container_filter is not None:
                query += 'container_filter=' + str(container_filter) + ';'
            if cube is not None:
                query += 'cube=' + str(cube) + ';'
            if host_filter is not None:
                query += 'host_filter=' + str(host_filter) + ';'
            if dbms_filter is not None:
                query += 'dbms_filter=' + str(dbms_filter) + ';'
            if measure_filter is not None:
                query += 'measure_filter=' + str(measure_filter) + ';'
            if ntransform is not None:
                query += 'ntransform=' + str(ntransform) + ';'
            if src_filter is not None:
                query += 'src_filter=' + str(src_filter) + ';'
            if db_filter is not None:
                query += 'db_filter=' + str(db_filter) + ';'
            if recursive is not None:
                query += 'recursive=' + str(recursive) + ';'
            if hidden is not None:
                query += 'hidden=' + str(hidden) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def randcube(cls, ncores=1, exec_mode='sync', container=None, cwd=None, host_partition='auto', filesystem='auto', ioserver='mysql_table', schedule=0, nhost=0, ndbms=1, ndb=1, run='yes', nfrag=1,
                  ntuple=1, measure=None, measure_type=None, exp_ndim=None, dim=None, concept_level='c', dim_size=None, compressed='yes', grid='-', description='-', objkey_filter='all', display=False):
        """Generate a random compressed data cube:( container=None, nhost=0, ndbms=1, ndb=1, nfrag=1, ntuple=1, measure=None, measure_type='manual', exp_ndim=None, dim=None, dim_size=None , grid='-',
           compressed= 'auto' , concept_level='c', host_partition='auto', filesystem='auto',hidden='no', cwd=None, ncores=1, exec_mode='sync', ioserver='mysql_table',schedule=0, objkey_filter='all',
           display=False) -> dict or None : wrapper of the operator OPH_RANDCUBE

        :param container: container name
        :type container: str
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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or container is None or measure in None or measure_type is None or exp_ndim is None or dim is None or dim_size is None:
            raise RuntimeError('Cube.client is None or container is None or measure in None or measure_type is None or exp_ndim is None or dim is None or dim_size is None')
        newcube = None

        query = 'oph_randcube '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if cwd is not None:
            query += 'cwd=' + str(cwd) + ';'
        if host_partition is not None:
            query += 'host_partition=' + str(host_partition) + ';'
        if filesystem is not None:
            query += 'filesystem=' + str(filesystem) + ';'
        if ioserver is not None:
            query += 'ioserver=' + str(ioserver) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if nhost is not None:
            query += 'nhost=' + str(nhost) + ';'
        if ndbms is not None:
            query += 'ndbms=' + str(ndbms) + ';'
        if ndb is not None:
            query += 'ndb=' + str(ndb) + ';'
        if run is not None:
            query += 'run=' + str(run) + ';'
        if nfrag is not None:
            query += 'nfrag=' + str(nfrag) + ';'
        if ntuple is not None:
            query += 'ntuple=' + str(ntuple) + ';'
        if measure is not None:
            query += 'measure=' + str(measure) + ';'
        if measure_type is not None:
            query += 'measure_type=' + str(measure_type) + ';'
        if exp_ndim is not None:
            query += 'exp_ndim=' + str(exp_ndim) + ';'
        if dim is not None:
            query += 'dim=' + str(dim) + ';'
        if concept_level is not None:
            query += 'concept_level=' + str(concept_level) + ';'
        if dim_size is not None:
            query += 'dim_size=' + str(dim_size) + ';'
        if compressed is not None:
            query += 'compressed=' + str(compressed) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'


        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def explorenc(cls, ncores=1, exec_mode='sync', schedule=0, measure=None, src_path=None, exp_dim=None, imp_dim=None, subset_dims='none', subset_type='index', subset_filter='all', limit_filter=100, show_index='no',
                   show_id='no', show_time='no', show_stats='no', show_fit='no', level=1, imp_num_point=0, offset=50, operation='avg', wavelet='no', wavelet_ratio=0, wavelet_coeff='no', objkey_filter='all', display=False):
        """Read the NetCDF file(force='no',measure= None, src_path= None, exp_dim= None, imp_dim= None , ncores=1, exec_mode='sync', schedule=0, subset_dims='none', subset_filter='all', subset_type='index',
        limit_filter=0, show_id='no', show_index='no', show_time='no', show_stats='no', show_fit='no', level=1, imp_num_point=0, wavelet='no', display=False) -> None : wrapper of the operator OPH_EXPLORENC

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_explorenc '
        
            if ncores is not None:
                query += 'ncores=' + str(ncores) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if schedule is not None:
                query += 'schedule=' + str(schedule) + ';'
            if measure is not None:
                query += 'measure=' + str(measure) + ';'
            if src_path is not None:
                query += 'src_path=' + str(src_path) + ';'
            if exp_dim is not None:
                query += 'exp_dim=' + str(exp_dim) + ';'
            if imp_dim is not None:
                query += 'imp_dim=' + str(imp_dim) + ';'
            if subset_dims is not None:
                query += 'subset_dims=' + str(subset_dims) + ';'
            if subset_type is not None:
                query += 'subset_type=' + str(subset_type) + ';'
            if subset_filter is not None:
                query += 'subset_filter=' + str(subset_filter) + ';'
            if limit_filter is not None:
                query += 'limit_filter=' + str(limit_filter) + ';'
            if show_index is not None:
                query += 'show_index=' + str(show_index) + ';'
            if show_id is not None:
                query += 'show_id=' + str(show_id) + ';'
            if show_time is not None:
                query += 'show_time=' + str(show_time) + ';'
            if show_stats is not None:
                query += 'show_stats=' + str(show_stats) + ';'
            if show_fit is not None:
                query += 'show_fit=' + str(show_fit) + ';'
            if level is not None:
                query += 'level=' + str(level) + ';'
            if imp_num_point is not None:
                query += 'imp_num_point=' + str(imp_num_point) + ';'
            if offset is not None:
                query += 'offset=' + str(offset) + ';'
            if operation is not None:
                query += 'operation=' + str(operation) + ';'
            if wavelet is not None:
                query += 'wavelet=' + str(wavelet) + ';'
            if wavelet_ratio is not None:
                query += 'wavelet_ratio=' + str(wavelet_ratio) + ';'
            if wavelet_coeff is not None:
                query += 'wavelet_coeff=' + str(wavelet_coeff) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def importnc3(cls, ncores=1, exec_mode='sync', container=None, cwd=None, host_partition='auto', filesystem='auto', ioserver='mysql_table', import_metadata=None,
                   check_compliance='no', schedule=0, nhost=0, ndbms=0, ndb=1, nfrag=0, run='yes', measure=None, src_path=None, exp_dim=None, imp_dim=None, subset_dims=None,
                   subset_type='index', subset_filter='all', time_filter='enabled', offset='O', exp_concept_level='c', imp_concept_level='c', compressed='no', grid=None, hierarchy='oph_base',  
                   vocabulary='-', base_time='1900-01-01 00:00:00', units='d', calendar='standard', month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', leap_year=0, leap_month=2,  
                   description='-', objkey_filter='all', display=False):
        """Import a NetCDF file excluding metadata into the session directory:(path='-', container=None, measure=None, src_path=None, imp_concept_level=None,import_metadata=None , concept_level=None,
           cwd=None, ncores=1,exec_mode='sync', imp_dim=None, exp_dim=None, subset_dims=None, grid=None, host_partition='auto', filesystem='auto', ioserver='mysql_table', check_compliance='no', schedule=0,
           nhost=0, ndbms=0, ndb=1, nfrag=0, subset_filter='all', time_filter='enabled', offset='', exp_concept_level='c', compressed='no', subset_type='index', base_time='1900-01-01 00:00:00',
           calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-', description='-', display=False) ->
           dict or None : wrapper of the operator OPH_IMPORTNC3

        :param path: absolute or relative path
        :type container_filter: str
        :param cube: filter on cube
        :type cube: str
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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or measure is None or src_path is None:
            raise RuntimeError('Cube.client is None or measure is None or src_path is None')
        newcube = None

        query = 'oph_importnc3 '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if cwd is not None:
            query += 'cwd=' + str(cwd) + ';'
        if host_partition is not None:
            query += 'host_partition=' + str(host_partition) + ';'
        if filesystem is not None:
            query += 'filesystem=' + str(filesystem) + ';'
        if ioserver is not None:
            query += 'ioserver=' + str(ioserver) + ';'
        if import_metadata is not None:
            query += 'import_metadata=' + str(import_metadata) + ';'
        if check_compliance is not None:
            query += 'check_compliance=' + str(check_compliance) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if nhost is not None:
            query += 'nhost=' + str(nhost) + ';'
        if ndbms is not None:
            query += 'ndbms=' + str(ndbms) + ';'
        if ndb is not None:
            query += 'ndb=' + str(ndb) + ';'
        if nfrag is not None:
            query += 'nfrag=' + str(nfrag) + ';'
        if run is not None:
            query += 'run=' + str(run) + ';'
        if measure is not None:
            query += 'measure=' + str(measure) + ';'
        if src_path is not None:
            query += 'src_path=' + str(src_path) + ';'                              
        if exp_dim is not None:
            query += 'exp_dim=' + str(exp_dim) + ';'
        if imp_dim is not None:
            query += 'imp_dim=' + str(imp_dim) + ';'
        if subset_dims is not None:
            query += 'subset_dims=' + str(subset_dims) + ';'
        if subset_type is not None:
            query += 'subset_type=' + str(subset_type) + ';'
        if subset_filter is not None:
            query += 'subset_filter=' + str(subset_filter) + ';'
        if offset is not None:
            query += 'offset=' + str(offset) + ';'            
        if exp_concept_level is not None:
            query += 'exp_concept_level=' + str(exp_concept_level) + ';'
        if imp_concept_level is not None:
            query += 'imp_concept_level=' + str(imp_concept_level) + ';'
        if compressed is not None:
            query += 'compressed=' + str(compressed) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if hierarchy is not None:
            query += 'hierarchy=' + str(hierarchy) + ';'
        if vocabulary is not None:
            query += 'vocabulary=' + str(vocabulary) + ';'
        if base_time is not None:
            query += 'base_time=' + str(base_time) + ';'
        if units is not None:
            query += 'units=' + str(units) + ';'
        if calendar is not None:
            query += 'calendar=' + str(calendar) + ';'
        if month_lengths is not None:
            query += 'month_lengths=' + str(month_lengths) + ';'
        if leap_year is not None:
            query += 'leap_year=' + str(leap_year) + ';'
        if leap_month is not None:
            query += 'leap_month=' + str(leap_month) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def man(cls, function=None, function_version='latest', function_type='operator', exec_mode='sync', objkey_filter='all', display=True):
        """man(function=None, function_type='operator', function_version='latest', exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_MAN

        :param function: operator or primitive name
        :type function: str
        :param function_type: operator|primitive
        :type function_type: str
        :param function_version: operator or primitive version
        :type function_version: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
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
            if function_version is not None:
                query += 'function_version=' + str(function_version) + ';'
            if function_type is not None:
                query += 'function_type=' + str(function_type) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def movecontainer(cls, container=None, cwd=None, exec_mode='sync', objkey_filter='all', display=False):
        """movecontainer(container=None, cwd=None, exec_mode='sync', display=False) -> dict or None : wrapper of the operator OPH_MOVECONTAINER

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def operators(cls, operator_filter=None, limit_filter=0, exec_mode='sync', objkey_filter='all', display=True):
        """operators(operator_filter=None, limit_filter=0, exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_OPERATORS_LIST

        :param operator_filter: filter on operator name
        :type operator_filter: str
        :param limit_filter: max number of lines
        :type limit_filter: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
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
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def primitives(cls, level=1, dbms_filter='-', return_type='all', primitive_type='all', primitive_filter='', limit_filter=0, exec_mode='sync', objkey_filter='all', display=True):
        """primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, exec_mode='sync', objkey_filter='all', display=True) ->
           dict or None : wrapper of the operator OPH_PRIMITIVES_LIST

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
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_primitives_list '

            if level is not None:
                query += 'level=' + str(level) + ';'
            if dbms_filter is not None:
                query += 'dbms_filter=' + str(dbms_filter) + ';'
            if return_type is not None:
                query += 'return_type=' + str(return_type) + ';'
            if primitive_type is not None:
                query += 'primitive_type=' + str(primitive_type) + ';'
            if primitive_filter is not None:
                query += 'primitive_filter=' + str(primitive_filter) + ';'
            if limit_filter is not None:
                query += 'limit_filter=' + str(limit_filter) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def restorecontainer(cls, exec_mode='sync', container=None, cwd=None,  objkey_filter='all', display=False):
        """restorecontainer(container=None, cwd=None, exec_mode='sync', objkey_filter='all', display=False) -> dict or None : wrapper of the operator OPH_RESTORECONTAINER

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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

            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if container is not None:
                query += 'container=' + str(container) + ';'
            if cwd is not None:
                query += 'cwd=' + str(cwd) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def script(cls, script=':', args=' ', stdout='stdout', stderr='stderr', list='no', exec_mode='sync', ncores=1, objkey_filter='all', display=False):
        """script(script=':', args=' ', stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync', list='no', objkey_filter='all', display=False) ->
           dict or None : wrapper of the operator OPH_SCRIPT

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
            if list is not None:
                query += 'list=' + str(list) + ';'
            if exec_mode is not None:
                query += 'exec_mode=' + str(exec_mode) + ';'
            if ncores is not None:
                query += 'ncores=' + str(ncores) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def resume(cls, session='this', id=0, id_type='workflow', document_type='response', level=1, user='', status_filter='11111111', save='no', objkey_filter='all', display=True):
        """ Get the list of the commands submitted within the working session:( id=0, id_type='workflow', document_type='response', level=1, save='no', session='this',
            objkey_filter='all', user='', display=True) -> None : wrapper of the operator OPH_RESUME

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_resume '

            if session is not None:
                query += 'session=' + str(session) + ';'
            if id is not None:
                query += 'id=' + str(id) + ';'
            if id_type is not None:
                query += 'id_type=' + str(id_type) + ';'
            if document_type is not None:
                query += 'document_type=' + str(document_type) + ';'
            if level is not None:
                query += 'level=' + str(level) + ';'
            if user is not None:
                query += 'user=' + str(user) + ';'
            if status_filter is not None:
                query += 'status_filter=' + str(status_filter) + ';'
            if save is not None:
                query += 'save=' + str(save) + ';'
            if objkey_filter is not None:
                query += 'objkey_filter=' + str(objkey_filter) + ';'

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def intercomparison(cls, ncores=1, exec_mode='sync', cube=None, cube2=None, missingvalue='NAN', schedule=0, container=None, discription='-', objkey_filter='all', display=False):
        """intercomparison(cubes=None, container=None, schedule=0, exec_mode='sync', ncores=1, display=False) -> Cube : wrapper of the operator OPH_INTERCOMPARISON

        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param container: optional container name
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cube is None or cube2 is None:
            raise RuntimeError('Cube.client is None or cubes is None')
        newcube = None

        query = 'oph_intercomparison '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if cube is not None:
            query += 'cube=' + str(cube) + ';'
        if cube2 is not None:
            query += 'cube2=' + str(cube2) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if discription is not None:
            query += 'discription=' + str(discription) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def mergecubes(cls, ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', description='-', objkey_filter='all', display=False):
        """mergecubes(cubes=None, container='-', dim='-', description='-', objkey_filter='all', schedule=0, exec_mode='sync', ncores=1, display=False) -> Cube : wrapper of the operator OPH_MERGECUBES

        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param container: optional container name
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cubes is None:
            raise RuntimeError('Cube.client is None or cubes is None')
        newcube = None

        query = 'oph_mergecubes '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if cubes is not None:
            query += 'cubes=' + str(cubes) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def mergecubes2(cls, ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', description='-', dim='-', objkey_filter='all', display=False):
        """mergecubes2(cubes=None, container='-', dim='-', description='-', objkey_filter='all', schedule=0, exec_mode='sync', ncores=1, display=False) -> Cube : wrapper of the operator OPH_MERGECUBES2

        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param container: optional container name
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cubes is None:
            raise RuntimeError('Cube.client is None or cubes is None')
        newcube = None

        query = 'oph_mergecubes2 '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if cubes is not None:
            query += 'cubes=' + str(cubes) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if dim is not None:
            query += 'dim=' + str(dim) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def __init__(self, container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-',
                 imp_concept_level='c', import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all',
                 subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
                 month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-', pid=None, display=False):
        """Cube(container=None, cwd=None, exp_dim=None, host_partition=None, imp_dim=None, measure=None, src_path=None, compressed='no', exp_concept_level='c', filesystem='local', grid='-',
           imp_concept_level='c', import_metadata='no', check_compliance='no', ioserver='mysql_table', ncores=1, ndb=1, ndbms=1, nfrag=1, nhost=1, subset_dims='none', subset_filter='all',
           subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31',
           run='yes', units='d', vocabulary='-', display=False) -> obj or Cube(pid=None) -> obj

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
                self.info(display)
            except Exception as e:
                print(get_linenumber(), "Something went wrong in instantiating the cube", e)
            finally:
                pass
        else:
            if exp_dim is None or imp_dim is None or measure is None or src_path is None:
                raise RuntimeError('one or more required parameters are None')
            if Cube.client is None:
                raise RuntimeError('Cube.client is None')

            query = 'oph_importnc3 '

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
                if Cube.client.submit(query, display) is None:
                    raise RuntimeError()

                if Cube.client.last_response is not None:
                    if Cube.client.cube:
                        self.pid = Cube.client.cube
                        self.info(display)
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

    def info(self, display=True):
        """info(display=True) -> None : call OPH_CUBESIZE, OPH_CUBEELEMENTS and OPH_CUBESCHEMA to fill all Cube attributes

        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        query = 'oph_cubesize exec_mode=sync;cube=' + str(self.pid) + ';'
        if Cube.client.submit(query, display) is None:
            raise RuntimeError()
        query = 'oph_cubeelements exec_mode=sync;cube=' + str(self.pid) + ';'
        if Cube.client.submit(query, display) is None:
            raise RuntimeError()
        query = 'oph_cubeschema exec_mode=sync;cube=' + str(self.pid) + ';'
        if Cube.client.submit(query, display) is None:
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

    def exportnc(self, misc='no', output_path='default', output_name='default', force='no', export_metadata='no', schedule=0, exec_mode='sync', ncores=1, objkey_filter='all', display=False):
        """exportnc(export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync', display=False) -> None : wrapper of the operator OPH_EXPORTNC

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')

        query = 'oph_exportnc '

        if misc is not None:
            query += 'misc=' + str(misc) + ';'
        if output_path is not None:
            query += 'output_path=' + str(output_path) + ';'
        if output_name is not None:
            query += 'output_name=' + str(output_name) + ';'
        if force is not None:
            query += 'force=' + str(force) + ';'
        if export_metadata is not None:
            query += 'export_metadata=' + str(export_metadata) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def exportnc2(self, misc='no', output_path='default', output_name='default', force='no', export_metadata='no', schedule=0, exec_mode='sync', ncores=1, objkey_filter='all', display=False):
        """exportnc(export_metadata='no', force='no', output_path='default', output_name='default', ncores=1, exec_mode='sync', schedule=0, misc='no', display=False) ->
           None : wrapper of the operator OPH_EXPORTNC2

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')

        query = 'oph_exportnc2 '

        if misc is not None:
            query += 'misc=' + str(misc) + ';'
        if output_path is not None:
            query += 'output_path=' + str(output_path) + ';'
        if output_name is not None:
            query += 'output_name=' + str(output_name) + ';'
        if force is not None:
            query += 'force=' + str(force) + ';'
        if export_metadata is not None:
            query += 'export_metadata=' + str(export_metadata) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def aggregate(self, ncores=1, exec_mode='sync', schedule=0, group_size='all', operation=None, missingvalue='NAN', grid='-', container=None, discription='-', objkey_filter='all', display=False):
        """aggregate( schedule=0, operation=None, container=None, grid='-', group_size='all', ncores=1, exec_mode='sync', display=False) -> Cube or None : wrapper of the operator OPH_AGGREGATE

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or operation is None:
            raise RuntimeError('Cube.client is None or pid is None or operation is None')
        newcube = None

        query = 'oph_aggregate '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if group_size is not None:
            query += 'group_size=' + str(group_size) + ';'            
        if operation is not None:
            query += 'operation=' + str(operation) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'    
        if container is not None:
            query += 'container=' + str(container) + ';'
        if discription is not None:
            query += 'discription=' + str(discription) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'    
        
        

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def aggregate2(self, ncores=1, exec_mode='sync', schedule=0, dim=None, concept_level='A', midnight='24', operation=None, grid='-', missingvalue='NAN', container=None, description= '-', display=False):
        """aggregate2(dim=None, schedule=0, operation=None, concept_level='A', container=None, grid='-', midnight='24', ncores=1, exec_mode='sync', display=False) ->
           Cube or None : wrapper of the operator OPH_AGGREGATE2

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or dim is None or operation is None:
            raise RuntimeError('Cube.client is None or pid is None or dim is None or operation is None')
        newcube = None

        query = 'oph_aggregate2 '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if dim is not None:
            query += 'dim=' + str(dim) + ';'
        if concept_level is not None:
            query += 'concept_level=' + str(concept_level) + ';'
        if midnight is not None:
            query += 'midnight=' + str(midnight) + ';'
        if operation is not None:
            query += 'operation=' + str(operation) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'    
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def apply(self, ncores=1, exec_mode='sync', query=None, dim_query='null', measure='null', measure_type='manual', dim_type='manual', check_type='yes', compressed='auto', schedule='O', container=None,
              description='-', objkey_filter='all', display=False):
        """apply(query=None, check_type='yes', compressed='auto', container=None, dim_query='null', dim_type='manual', measure='null', measure_type='manual', ncores=1, exec_mode='sync', description='-',
           display=False) -> Cube or None : wrapper of the operator OPH_APPLY

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or query is None:
            raise RuntimeError('Cube.client is None or pid is None or query is None')
        newcube = None

        internal_query = 'oph_apply '

        if ncores is not None:
            internal_query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            internal_query += 'exec_mode=' + str(exec_mode) + ';'
        if query is not None:
            internal_query += 'query=' + str(query) + ';'
        if dim_query is not None:
            internal_query += 'dim_query=' + str(dim_query) + ';'
        if measure is not None:
            internal_query += 'measure=' + str(measure) + ';'
        if measure_type is not None:
            internal_query += 'measure_type=' + str(measure_type) + ';'
        if dim_type is not None:
            internal_query += 'dim_type=' + str(dim_type) + ';'
        if check_type is not None:
            internal_query += 'check_type=' + str(check_type) + ';'
        if compressed is not None:
            internal_query += 'compressed=' + str(compressed) + ';'
        if schedule is not None:
            internal_query += 'schedule=' + str(schedule) + ';'
        if container is not None:
            internal_query += 'container=' + str(container) + ';'
        if description is not None:
            internal_query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            internal_query += 'objkey_filter=' + str(objkey_filter) + ';'

        internal_query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(internal_query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    # def concatnc(self, src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync', display=False):
        """concatnc(src_path=None, check_exp_dim='yes', grid='-', import_metadata='no', ncores=1, exec_mode='sync', display=False) -> Cube or None : wrapper of the operator OPH_CONCATNC

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        # if Cube.client is None or self.pid is None or src_path is None:
        #     raise RuntimeError('Cube.client is None or pid is None or src_path is None')
        # newcube = None

        # query = 'oph_concatnc '

        # if src_path is not None:
        #     query += 'src_path=' + str(src_path) + ';'
        # if check_exp_dim is not None:
        #     query += 'check_exp_dim=' + str(check_exp_dim) + ';'
        # if grid is not None:
        #     query += 'grid=' + str(grid) + ';'
        # if import_metadata is not None:
        #     query += 'import_metadata=' + str(import_metadata) + ';'
        # if ncores is not None:
        #     query += 'ncores=' + str(ncores) + ';'
        # if exec_mode is not None:
        #     query += 'exec_mode=' + str(exec_mode) + ';'

        # query += 'cube=' + str(self.pid) + ';'

        # try:
        #     if Cube.client.submit(query, display) is None:
        #         raise RuntimeError()

        #     if Cube.client.last_response is not None:
        #         if Cube.client.cube:
        #             newcube = Cube(pid=Cube.client.cube)
        # except Exception as e:
        #     print(get_linenumber(), "Something went wrong:", e)
        #     raise RuntimeError()
        # else:
        #     return newcube

    def provenance(self, branch='all', exec_mode='sync', objkey_filter='all', display=True):
        """provenance(branch='all', exec_mode='sync', display=True) -> dict or None : wrapper of the operator OPH_CUBEIO

        :param branch: parent|children|all
        :type branch: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
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
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def delete(self, ncores=1, exec_mode='sync', schedule=0, objkey_filter='all', display=False):
        """delete(ncores=1, exec_mode='sync', display=False) -> None : wrapper of the operator OPH_DELETE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'    

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def drilldown(self, ncores=1, exec_mode='sync', schedule=0, ndim=1, container=None, description='-', objkey_filter='all', display=False):
        """drilldown(ndim=1, container=None, ncores=1, exec_mode='sync', description='-' display=False) -> Cube or None : wrapper of the operator OPH_DRILLDOWN

        :param ndim: number of implicit dimensions that will be transformed in explicit dimensions
        :type ndim: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        newcube = None

        query = 'oph_drilldown '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if ndim is not None:
            query += 'ndim=' + str(ndim) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def duplicate(self, ncores=1, exec_mode='sync', schedule=0, container=None, description='-', objkey_filter='all', display=False):
        """duplicate(container=None, ncores=1, exec_mode='sync', description='-', display=False) -> Cube or None : wrapper of the operator OPH_DUPLICATE

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        newcube = None

        query = 'oph_duplicate '

        
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def explore(self, schedule=0, limit_filter=0, subset_dims=None, subset_filter=None, time_filter='yes', show_index='no', show_id='no', show_time='no', level=1, output_path='default', output_name='default', base64='no', ncores=1, 
                exec_mode='sync', objkey_filter='all', display=True):
        """explore(level=1, output_path='default', output_name='default', show_id='no', show_index='no', show_time='no', subset_dims=None, subset_filter=None, exec_mode='sync', ncores=1, display=True) ->
           dict or None : wrapper of the operator OPH_EXPLORECUBE

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_explorecube '

        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if limit_filter is not None:
            query += 'limit_filter=' + str(limit_filter) + ';'
        if subset_dims is not None:
            query += 'subset_dims=' + str(subset_dims) + ';'
        if subset_filter is not None:
            query += 'subset_filter=' + str(subset_filter) + ';'
        if time_filter is not None:
            query += 'time_filter=' + str(time_filter) + ';'
        if show_index is not None:
            query += 'show_index=' + str(show_index) + ';'
        if show_id is not None:
            query += 'show_id=' + str(show_id) + ';'
        if show_time is not None:
            query += 'show_time=' + str(show_time) + ';'
        if level is not None:
            query += 'level=' + str(level) + ';'
        if output_path is not None:
            query += 'output_path=' + str(output_path) + ';'
        if output_name is not None:
            query += 'output_name=' + str(output_name) + ';'
        if base64 is not None:
            query += 'base64=' + str(base64) + ';'
        
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def publish(self, content='all', schedule=0, show_index='no', show_id='no', show_time='no', ncores=1,  exec_mode='sync', objkey_filter='all', display=True):
        """ Publish data from the datacube "URL/1/1":( ncores=1, content='all',exec_mode='sync', show_id= 'no' ,show_index='no', schedule=0, objkey_filter='all', show_time='no', display=True) ->
            None : wrapper of the operator OPH_PUBLISH

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_publish '

        if content is not None:
            query += 'content=' + str(content) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if show_index is not None:
            query += 'show_index=' + str(show_index) + ';'
        if show_id is not None:
            query += 'show_id=' + str(show_id) + ';'
        if show_time is not None:
            query += 'show_time=' + str(show_time) + ';'
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def unpublish(self, ncores=1, exec_mode='sync', objkey_filter='all', display=False):
        """ unpublish data of the datacube "URL/1/1":( ncores=1, exec_mode='sync', objkey_filter='all', display=False) -> None : wrapper of the operator OPH_UNPUBLISH

        :param exec_mode: async or sync
        :type exec_mode: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_unpublish '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'
        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def cubeschema(self, level=0, dim=None, show_index='no', show_time='no', base64='no', container=None, exec_mode='sync', objkey_filter='all', display=True):
        """ Show metadata information about the datacube identified by the PID "URL/1/1":( cores=1, container=None, exec_mode='sync', level=0, dim=None, show_index='no', show_time='no',
            base64='no', display=True) -> None : wrapper of the operator OPH_CUBESCHEMA

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_cubeschema '

        if level is not None:
            query += 'level=' + str(level) + ';'
        if dim is not None:
            query += 'dim=' + str(dim) + ';'
        if show_index is not None:
            query += 'show_index=' + str(show_index) + ';'
        if show_time is not None:
            query += 'show_time=' + str(show_time) + ';'
        if base64 is not None:
            query += 'base64=' + str(base64) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'    
        

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def cubesize(self, schedule=0, exec_mode='sync', byte_unit='MB', ncores=1, objkey_filter='all', display=True):
        """ Compute the size of the datacube "URL/1/1":( cores=1, container=None,exec_mode='sync', display=True) -> None : wrapper of the operator OPH_CUBESIZE

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_cubesize '

        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if byte_unit is not None:
            query += 'byte_unit=' + str(byte_unit) + ';'
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def cubeelements(self, schedule=0, exec_mode='sync', algorithm=None, ncores=1, objkey_filter='all', display=True):
        """ Compute the number of elements in the datacube "URL/1/1":( schedule=0, algorithm=None, cores=1, container=None,exec_mode='sync', display=True) -> None : wrapper of the operator OPH_CUBEELEMENTS

        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        response = None

        query = 'oph_cubeelements '

        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if algorithm is not None:
            query += 'algorithm=' + str(algorithm) + ';'
        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def intercube(self, ncores=1, exec_mode='sync', cube2=None, operation=None, missingvalue='NAN', output_measure=None, measure='null', schedule=0, container=None, description='-', objkey_filter='all', display=False):
        """intercube(cube2=None, operation=None, output_measure=None, container=None, exec_mode='sync', ncores=1, description='-', display=False) -> Cube or None : wrapper of the operator OPH_INTERCUBE

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or cube2 is None or operation is None or output_measure is None:
            raise RuntimeError('Cube.client is None or pid is None or cube2 is None or operation is None or output_measure is None')
        newcube = None

        query = 'oph_intercube '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if cube2 is not None:
            query += 'cube2=' + str(cube2) + ';'
        if operation is not None:
            query += 'operation=' + str(operation) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if measure is not None:
            query += 'measure=' + str(measure) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'    
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def merge(self, ncores=1, exec_mode='sync', cube='-', schedule=0, nmerge=0, container=None, description='-', objkey_filter='all', display=False):
        """merge(nmerge=0, schedule=0, description='-', objkey_filter='all', container=None, exec_mode='sync', ncores=1, display=False) -> Cube or None : wrapper of the operator OPH_MERGE

        :param nmerge: number of input fragments to merge in an output fragment, 0 for all
        :type nmerge: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or cube is None:
            raise RuntimeError('Cube.client is None or pid is None or cube is None')
        newcube = None

        query = 'oph_merge '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if cube is not None:
            query += 'cube=' + str(cube) + ';'    
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if nmerge is not None:
            query += 'nmerge=' + str(nmerge) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def metadata(self, mode='read', metadata_key='all', variable='global', metadata_id=0,  metadata_type='text', metadata_value='-', metadata_type_filter='all', metadata_value_filter='all', force='no',
                 exec_mode='sync', objkey_filter='all', display=True):
        """metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, metadata_type_filter=None, metadata_value_filter=None, force='no', 
           exec_mode='sync', objkey_filter='all', display=True) -> dict or None : wrapper of the operator OPH_METADATA

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
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is Ture)
        :type display: bool
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
        if metadata_key is not None:
            query += 'metadata_key=' + str(metadata_key) + ';'
        if variable is not None:
            query += 'variable=' + str(variable) + ';'
        if metadata_id is not None:
            query += 'metadata_id=' + str(metadata_id) + ';'
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
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def permute(self, ncores=1, exec_mode='sync', schedule=0, dim_pos=None, container=None, description='-', objkey_filter='all', display=False):
        """permute(dim_pos=None, container=None, exec_mode='sync', ncores=1, schedule=0, description='-', objkey_filter='all', display=False) -> Cube or None : wrapper of the operator OPH_PERMUTE

        :param dim_pos: permutation of implicit dimensions as a comma-separated list of dimension levels
        :type dim_pos: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or dim_pos is None:
            raise RuntimeError('Cube.client is None or pid is None or dim_pos is None')
        newcube = None

        query = 'oph_permute '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if dim_pos is not None:
            query += 'dim_pos=' + str(dim_pos) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def reduce(self, ncores=1, exec_mode='sync', schedule=0, group_size='all', operation=None, order=2, missingvalue='NAN', grid='-', container=None, description='-', objkey_filter='all', display=False):
        """reduce(operation=None, container=None, exec_mode='sync', grid='-', group_size='all', ncores=1, schedule=0, order=2, description='-', objkey_filter='all', display=False) ->
           Cube or None : wrapper of the operator OPH_REDUCE

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or operation is None:
            raise RuntimeError('Cube.client is None or pid is None or operation is None')
        newcube = None

        query = 'oph_reduce '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if group_size is not None:
            query += 'group_size=' + str(group_size) + ';'
        if operation is not None:
            query += 'operation=' + str(operation) + ';'
        if order is not None:
            query += 'order=' + str(order) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def reduce2(self, ncores=1, exec_mode='sync', schedule=0, dim=None, concept_level='A', midnight='24', operation=None, order=2, missingvalue='NAN', grid='-', container=None, description='-',
                objkey_filter='all', display=False):
        """reduce2(dim=None, operation=None, concept_level='A', container=None, exec_mode='sync', grid='-', midnight='24', order=2, description='-',
           objkey_filter='all', schedule=0, ncores=1, display=False) -> Cube or None : wrapper of the operator OPH_REDUCE2

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or dim is None or operation is None:
            raise RuntimeError('Cube.client is None or pid is None or dim is None or operation is None')
        newcube = None

        query = 'oph_reduce2 '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if dim is not None:
            query += 'dim=' + str(dim) + ';'
        if concept_level is not None:
            query += 'concept_level=' + str(concept_level) + ';'
        if midnight is not None:
            query += 'midnight=' + str(midnight) + ';'
        if operation is not None:
            query += 'operation=' + str(operation) + ';'
        if order is not None:
            query += 'order=' + str(order) + ';'
        if missingvalue is not None:
            query += 'missingvalue=' + str(missingvalue) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'
        

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def rollup(self, ncores=1, exec_mode='sync', schedule=0, ndim=1, container=None, description='-', objkey_filter='all', display=False):
        """rollup(ndim=1, container=None, exec_mode='sync', ncores=1, schedule=0, description='-', objkey_filter='all', display=False) -> Cube or None : wrapper of the operator OPH_ROLLUP

        :param ndim: number of explicit dimensions that will be transformed in implicit dimensions
        :type ndim: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        newcube = None

        query = 'oph_rollup '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'    
        if ndim is not None:
            query += 'ndim=' + str(ndim) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'        
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def split(self, ncores=1, exec_mode='sync', schedule=0, nsplit=2, container=None, description='-', objkey_filter='all', display=False):
        """split(nsplit=2, container=None, exec_mode='sync', ncores=1, schedule=0, description='-', objkey_filter='all', display=False) -> Cube or None : wrapper of the operator OPH_SPLIT

        :param nsplit: number of output fragments per input fragment
        :type nsplit: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or nsplit is None:
            raise RuntimeError('Cube.client is None or pid is None or nsplit is None')
        newcube = None

        query = 'oph_split '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if nsplit is not None:
            query += 'nsplit=' + str(nsplit) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def subset(self, ncores=1, exec_mode='sync', schedule=0, subset_dims='none', subset_filter='all', grid='-', container=None, description='-', objkey_filter='all', display=False):
        """subset(subset_dims='none', subset_filter='all', container=None, exec_mode='sync', grid='-', ncores=1, schedule=0, description='-', objkey_filter='all', display=False) ->
           Cube or None : wrapper of the operator OPH_SUBSET

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        newcube = None

        query = 'oph_subset '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'    
        if subset_dims is not None:
            query += 'subset_dims=' + str(subset_dims) + ';'
        if subset_filter is not None:
            query += 'subset_filter=' + str(subset_filter) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def subset2(self, ncores=1, exec_mode='sync', schedule=0, subset_dims='none', subset_filter='all', time_filter='yes', offset=0, grid='-', container=None, description='-',
                objkey_filter='all', display=False):
        """subset2(subset_dims='none', subset_filter='all', grid='-', container=None, ncores=1, exec_mode='sync', schedule=0, time_filter='yes', offset=0, description='-',
           objkey_filter='all', display=False) -> Cube or None : wrapper of the operator OPH_SUBSET2

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
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError('Cube.client is None or pid is None')
        newcube = None

        query = 'oph_subset2 '

        if ncores is not None:
            query += 'ncores=' + str(ncores) + ';'
        if exec_mode is not None:
            query += 'exec_mode=' + str(exec_mode) + ';'
        if schedule is not None:
            query += 'schedule=' + str(schedule) + ';'
        if subset_dims is not None:
            query += 'subset_dims=' + str(subset_dims) + ';'
        if subset_filter is not None:
            query += 'subset_filter=' + str(subset_filter) + ';'
        if time_filter is not None:
            query += 'time_filter=' + str(time_filter) + ';'
        if offset is not None:
            query += 'offset=' + str(offset) + ';'
        if grid is not None:
            query += 'grid=' + str(grid) + ';'
        if container is not None:
            query += 'container=' + str(container) + ';'
        if description is not None:
            query += 'description=' + str(description) + ';'
        if objkey_filter is not None:
            query += 'objkey_filter=' + str(objkey_filter) + ';'

        query += 'cube=' + str(self.pid) + ';'

        try:
            if Cube.client.submit(query, display) is None:
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
