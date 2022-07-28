#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2022 CMCC Foundation
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
import base64
import struct
import PyOphidia.client as _client
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))


def _get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


def _dependency_check(dependency):
    if dependency == "pandas":
        try:
            import pandas
        except ModuleNotFoundError:
            raise RuntimeError("pandas is not installed")
    elif dependency == "xarray":
        try:
            import xarray
        except ModuleNotFoundError:
            raise RuntimeError("xarray is not installed")
    elif dependency == "numpy":
        try:
            import numpy
        except ModuleNotFoundError:
            raise RuntimeError("numpy is not installed")
    else:
        raise AttributeError("Dependency must be xarray, numpy or pandas")


def _time_dimension_finder(cube):
    for c in cube.dim_info:
        if c["hierarchy"].lower() == "oph_time" or c["name"].lower() == "time":
            return c["name"]
    return None


def _get_unpack_format(element_num, output_type):
    if output_type == "float":
        format = str(element_num) + "f"
    elif output_type == "double":
        format = str(element_num) + "d"
    elif output_type == "int":
        format = str(element_num) + "i"
    elif output_type == "long":
        format = str(element_num) + "l"
    elif output_type == "short":
        format = str(element_num) + "h"
    elif output_type == "char":
        format = str(element_num) + "c"
    else:
        raise RuntimeError("The value type is not valid")
    return format


def _calculate_decoded_length(decoded_string, output_type):
    if output_type == "float" or output_type == "int":
        num = int(float(len(decoded_string)) / float(4))
    elif output_type == "double" or output_type == "long":
        num = int(float(len(decoded_string)) / float(8))
    elif output_type == "short":
        num = int(float(len(decoded_string)) / float(2))
    elif output_type == "char":
        num = int(float(len(decoded_string)) / float(1))
    else:
        raise RuntimeError("The value type is not valid")
    return num


class Cube:
    """Cube(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
            cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='no',
            check_compliance='no', offset=0, ioserver='mysql_table', ncores=1, nfrag=0, nhost=0, subset_dims='none',
            subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00',
            calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
            month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-', policy='rr',
            description='-', schedule=0, pid=None, check_grid='no', save='yes', display=False) -> obj
         or Cube(pid=None) -> obj

    Attributes:
        pid: cube PID
        creation_date: creation date of the cube
        measure: name of the variable imported into the cube
        measure_type: measure data type
        level: number of operations between the original imported cube and the actual cube
        nfragments: total number of fragments
        source_file: parent of the actual cube
        hostxcube: number of hosts associated with the cube
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
        aggregate(ncores=1, nthreads=1, exec_mode='sync', schedule=0, group_size='all', operation=None, missingvalue='-',
                  grid='-', container='-', description='-', check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_AGGREGATE
        aggregate2(ncores=1, nthreads=1, exec_mode='sync', schedule=0, dim='-', concept_level='A', midnight='24', operation=None,
                   grid='-', missingvalue='-', container='-', description='-', check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_AGGREGATE2
        apply(ncores=1, nthreads=1, exec_mode='sync', query='measure', dim_query='null', measure='null', measure_type='manual',
              dim_type='manual', check_type='yes', on_reduce='skip', compressed='auto', schedule=0,container='-', description='-',
              save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_APPLY
        concatnc(src_path=None, cdd=None, grid='-', check_exp_dim='yes', dim_offset='-', dim_continue='no', offset=0,
                 description='-', subset_dims='none', subset_filter='all', subset_type='index', time_filter='yes', ncores=1,
                 exec_mode='sync', schedule=0, save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_CONCATNC
        concatnc2(src_path=None, cdd=None, grid='-', check_exp_dim='yes', dim_offset='-', dim_continue='no', offset=0,
                  description='-', subset_dims='none', subset_filter='all', subset_type='index', time_filter='yes', ncores=1,
                  nthreads=1, exec_mode='sync', schedule=0, save='yes', display=False)
           -> Cube or None : wrapper of the operator OPH_CONCATNC2
        cubeelements( schedule=0, algorithm='dim_product', ncores=1, exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_CUBEELEMENTS
        cubeschema(objkey_filter='all', exec_mode='sync', level=0, dim=None, show_index='no', show_time='no', base64='no',
                   action='read', concept_level='c', dim_level=1, dim_array='yes', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_CUBESCHEMA
        cubesize(schedule=0, ncores=1, byte_unit='MB', algorithm='euristic', objkey_filter='all', exec_mode='sync',
                 save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_CUBESIZE
        delete(ncores=1, nthreads=1, exec_mode='sync', schedule=0, save='yes', display=False)
          -> None : wrapper of the operator OPH_DELETE
        drilldown(ndim=1, container='-', ncores=1, exec_mode='sync', schedule=0, description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_DRILLDOWN
        duplicate(container='-', ncores=1, nthreads=1, exec_mode='sync', description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_DUPLICATE
        explore(schedule=0, limit_filter=100, subset_dims=None, subset_filter='all', time_filter='yes', subset_type='index',
                show_index='no', show_id='no', show_time='no', level=1, output_path='default', output_name='default', cdd=None,
                base64='no', ncores=1, exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_EXPLORECUBE
        exportnc(misc='no', output_path='default', output_name='default', cdd=None, force='no', export_metadata='yes', schedule=0,
                 shuffle='no', deflate=0, exec_mode='sync', ncores=1, save='yes', display=False)
          -> None : wrapper of the operator OPH_EXPORTNC
        exportnc2(misc='no', output_path='default', output_name='default', cdd=None, force='no', export_metadata='yes', schedule=0,
                  shuffle='no', deflate=0, exec_mode='sync', ncores=1, save='yes', display=False)
          -> None : wrapper of the operator OPH_EXPORTNC2
        export_array(show_id='no', show_time='no', subset_dims=None, subset_filter=None, time_filter='no')
          -> dict or None : return data from an Ophidia datacube into a Python structure
        info(display=True)
          -> None : call OPH_CUBESIZE and OPH_CUBESCHEMA to fill all Cube attributes
        intercube(cube2=None, cubes=None, operation='sub', missingvalue="-", container='-', exec_mode='sync', ncores=1,
                  description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_INTERCUBE
        intercube2(cubes=None, operation='avg', missingvalue="-", container='-', exec_mode='sync', ncores=1,
                  description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_INTERCUBE2
        merge(nmerge=0, schedule=0, description='-', container='-', exec_mode='sync', ncores=1, save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_MERGE
        metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None,
                 variable_filter=None, metadata_type_filter=None, metadata_value_filter=None, force='no', exec_mode='sync',
                 objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_METADATA
        permute(dim_pos=None, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes',
                display=False)
          -> Cube or None : wrapper of the operator OPH_PERMUTE
        provenance(branch='all', exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_CUBEIO
        publish(ncores=1, content='all', exec_mode='sync', show_id= 'no', show_index='no', schedule=0, show_time='no',
                save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_PUBLISH
        reduce(operation=None, container=None, exec_mode='sync', missingvalue="-", grid='-', group_size='all', ncores=1,
               nthreads=1, schedule=0, order=2, description='-', objkey_filter='all', check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_REDUCE
        reduce2(dim=None, operation=None, concept_level='A', missingvalue="-", container='-', exec_mode='sync', grid='-',
                midnight='24', order=2, description='-', schedule=0, ncores=1, nthreads=1, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_REDUCE2
        rollup(ndim=1, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_ROLLUP
        split(nsplit=2, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_SPLIT
        subset(subset_dims='none', subset_filter='all', container='-', exec_mode='sync', subset_type='index',
               time_filter='yes', offset=0, grid='-', ncores=1, nthreads=1, schedule=0, description='-', check_grid='no',
               save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_SUBSET
        to_dataset()
          -> xarray.core.dataset.Dataset or None : return data from an Ophidia datacube into a Xarray Dataset
        to_dataframe()
          -> pandas.core.frame.DataFrame or None : return data from an Ophidia datacube into a Pandas Dataframe
        unpublish( exec_mode='sync', save='yes', display=False)
          -> None : wrapper of the operator OPH_UNPUBLISH

    Class Methods:
        setclient(username='', password='', server, port='11732', token='', read_env=False, api_mode=True, project=None)
          -> None : Instantiate the Client, common for all Cube objects, for submitting requests
        b2drop(action='put', auth_path='-', src_path=None, dst_path='-', cdd=None, exec_mode='sync', save='yes', display=False)
          -> None : wrapper of the operator OPH_B2DROP
        cancel(id=None, type='kill', objkey_filter='all', display=False)
          -> None : wrapper of the operator OPH_CANCEL
        cluster(action='info', nhost=1, host_partition='all', host_type='io', user_filter='all', exec_mode='sync', display=False)
          -> None : wrapper of the operator OPH_CLUSTER
        containerschema(container=None, cwd=None, exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_CONTAINERSCHEMA
        createcontainer(exec_mode='sync', container=None, cwd=None, dim=None, dim_type="double", hierarchy='oph_base',
                        base_time='1900-01-01 00:00:00', units='d', calendar='standard',
                        month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', leap_year=0, leap_month=2, vocabulary='CF',
                        compressed='no', description='-', save='yes', display=False)
          -> None : wrapper of the operator OPH_CREATECONTAINER
        deletecontainer(container=None, container_pid='-', force='no', cwd=None, nthreads=1, exec_mode='sync', objkey_filter='all',
                        save='yes', display=False)
          -> None : wrapper of the operator OPH_DELETECONTAINER
        explorenc(exec_mode='sync', schedule=0, measure='-', src_path=None, cdd=None, exp_dim='-', imp_dim='-', subset_dims='none',
                  subset_type='index', subset_filter='all', limit_filter=100, show_index='no', show_id='no', show_time='no',
                  show_stats='00000000000000', show_fit='no', level=0, imp_num_point=0, offset=50, operation='avg', wavelet='no',
                  wavelet_ratio=0, wavelet_coeff='no', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_EXPLORENC
        folder(command=None, cwd=None, path=None, exec_mode='sync', save='yes', display=False)
          -> None : wrapper of the operator OPH_FOLDER
        fs(command='ls', dpath='-', file='-', measure='-', cdd=None, recursive='no', depth=0, realpath='no', subset_dims='none',
           subset_type='index', subset_filter='all', time_filter='yes', vocabulary='CF', exec_mode='sync', offset=0, save='yes',
           display=False)
          -> None : wrapper of the operator OPH_FS
        wait(type="clock", timeout=1, timeout_type="duration", key="-", value="-", filename="-", measure="-", message="-",
             subset_dims="none", subset_type="index", subset_filter="all", time_filter="yes", offset=0, run="yes", exec_mode="sync",
             save="yes", display=False)
           -> None : wrapper of the operator OPH_WAIT
        get_config(key='all', objkey_filter='all', display=True)
          -> dict or None : wrapper of the operator OPH_GET_CONFIG
        hierarchy(hierarchy='all', hierarchy_version='latest', exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_HIERARCHY
        importnc(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                 cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                 check_compliance='no', offset=0, ioserver='mysql_table', ncores=1, nfrag=0, nhost=0, subset_dims='none',
                 subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00',
                 calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
                 month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF', description='-',
                 policy='rr', schedule=0, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_IMPORTNC
        importnc2(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                  cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                  check_compliance='no', offset=0, ioserver='ophidiaio_memory', ncores=1, nthreads=1, nfrag=0, nhost=0,
                  subset_dims='none', subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync',
                  base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
                  month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF', description='-',
                  policy='rr', schedule=0, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_IMPORTNC2
        importncs(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                  cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                  check_compliance='no', offset=0, ioserver='ophidiaio_memory', ncores=1, nthreads=1, nfrag=0, nhost=0,
                  subset_dims='none', subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync',
                  base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
                  month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF', description='-',
                  policy='rr', schedule=0, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_IMPORTNCS
        instances(action='read', level=1, host_filter='all', nhost=0, host_partition='all', ioserver_filter='all', host_status='all',
                  exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_INSTANCES
        list(level=1, exec_mode='sync', path='-', cwd=None, container_filter='all', cube='all', host_filter='all', dbms_filter='all',
             measure_filter='all', ntransform='all', src_filter='all', db_filter='all', recursive='no', objkey_filter='all',
             save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_LIST
        loggingbk(session_level=0, job_level=0, mask=000, session_filter='all', session_label_filter='all',
                  session_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', workflowid_filter='all', markerid_filter='all',
                  parent_job_filter='all', job_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_status_filter='all',
                  submission_string_filter='all', job_start_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',
                  job_end_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', nlines=100, objkey_filter='all', exec_mode='sync',
                  save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_LOGGINGBK
        log_info(log_type='server', container_id=0, ioserver='mysql', nlines=10, exec_mode='sync', objkey_filter='all',
                 save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_LOG_INFO
        man(function=None, function_type='operator', function_version='latest', exec_mode='sync', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_MAN
        manage_session(action='list', session='this', key='user', value='null', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_MANAGE_SESSION
        mergecubes(ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', mode='i', hold_values='no', number=1,
                   order='none', description='-', save='yes', display=False)
          -> Cube : wrapper of the operator OPH_MERGECUBES
        mergecubes2(ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', dim_type='long', number=1, order='none',
                    description='-', dim='-', save='yes', display=False)
          -> Cube or None: wrapper of the operator OPH_MERGECUBES2
        movecontainer(container=None, cwd=None, exec_mode='sync', save='yes', display=False)
          -> None : wrapper of the operator OPH_MOVECONTAINER
        operators(operator_filter=None, limit_filter=0, exec_mode='sync', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_OPERATORS_LIST
        primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None,
                   exec_mode='sync', objkey_filter='all', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_PRIMITIVES_LIST
        randcube(ncores=1, exec_mode='sync', container=None, cwd=None, host_partition='auto', ioserver='mysql_table', schedule=0,
                 algorithm='default', policy='rr', nhost=0, run='yes', nfrag=1, ntuple=1, measure=None, measure_type=None,
                 exp_ndim=None, dim=None, concept_level='c', dim_size=None, compressed='no', grid='-', description='-',
                 save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_RANDCUBE
        randcube2(ncores=1, nthreads=1, exec_mode='sync', container=None, cwd=None, host_partition='auto', ioserver='ophidiaio_memory',
                  schedule=0, algorithm='default', policy='rr', nhost=0, run='yes', nfrag=1, ntuple=1, measure=None, measure_type=None,
                  exp_ndim=None, dim=None, concept_level='c', dim_size=None, compressed='no', grid='-', description='-', save='yes',
                  display=False)
          -> Cube or None : wrapper of the operator OPH_RANDCUBE2
        resume(id=0, id_type='workflow', document_type='response', level=1, save='no', session='this', objkey_filter='all',
               user='', execute='no', checkpoint='all', display=True)
          -> dict or None : wrapper of the operator OPH_RESUME
        script(script=':', args=' ', stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync', list='no', space='no',
               python_code=False, save='yes', display=False)
          -> None : wrapper of the operator OPH_SCRIPT
        search(path='-', metadata_value_filter='all', exec_mode='sync', metadata_key_filter='all', container_filter='all',
               objkey_filter='all', cwd=None, recursive='no', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_SEARCH
        service(status='', level=1, enable='none', disable='none', objkey_filter='all', save='yes', display=False)
          -> dict or None : wrapper of the operator OPH_SERVICE
        showgrid(container=None, grid='all', dim='all', show_index='no', cwd=None, exec_mode='sync', objkey_filter='all',
                 save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_SHOWGRID
        tasks(cls, cube_filter='all', path='-', operator_filter='all', cwd=None, recursive='no', container='all', objkey_filter='all',
              exec_mode='sync', save='yes', display=True)
          -> dict or None : wrapper of the operator OPH_TASKS
    """

    client = None

    @classmethod
    def setclient(
        cls,
        username="",
        password="",
        server="",
        port="11732",
        token="",
        read_env=False,
        api_mode=True,
        project=None,
    ):
        """setclient(username='', password='', server='', port='11732', token='', read_env=False, api_mode=True, project=None) -> None : Instantiate the Client, common for all Cube objects, for submitting requests

        :param username: Ophidia user
        :type username: str
        :param password: Ophidia password
        :type password: str
        :param server: Ophidia server address
        :type server: str
        :param port: Ophidia server port
        :type port: str
        :param token: Ophidia token
        :type token: str
        :param read_env: If true read the client variables from the environment
        :type read_env: bool
        :param api_mode: If True, use the class as an API and catch also framework-level errors
        :type api_mode: bool
        :param project: String with project ID to be used for job scheduling
        :type project: str
        :returns: None
        :rtype: None
        """

        try:
            cls.client = _client.Client(
                username,
                password,
                server,
                port,
                token,
                read_env,
                api_mode,
                project,
            )
        except Exception as e:
            print(_get_linenumber(), "Something went wrong in setting the client:", e)
        finally:
            pass

    @classmethod
    def b2drop(
        cls,
        action="put",
        auth_path="-",
        src_path=None,
        dst_path="-",
        cdd=None,
        exec_mode="sync",
        save="yes",
        display=False,
    ):
        """b2drop(action='put', auth_path='-', src_path=None, dst_path='-', cdd=None, exec_mode='sync', save='yes', display=False)
          -> None : wrapper of the operator OPH_B2DROP

        :param action: put|get
        :type action: str
        :param auth_path: absolute path to the netrc file containing the B2DROP credentials
        :type auth_path: str
        :param src_path: path to the file to be uploaded/downloaded to/from B2DROP
        :type src_path: str
        :param dst_path: path where the file will be uploaded on B2DROP or downloaded on disk
        :type dst_path: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or src_path is None:
                raise RuntimeError("Cube.client or src_path is None")

            query = "oph_b2drop "

            if action is not None:
                query += "action=" + str(action) + ";"
            if auth_path is not None:
                query += "auth_path=" + str(auth_path) + ";"
            if src_path is not None:
                query += "src_path=" + str(src_path) + ";"
            if dst_path is not None:
                query += "dst_path=" + str(dst_path) + ";"
            if cdd is not None:
                query += "cdd=" + str(cdd) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def cluster(
        cls,
        action="info",
        nhost=1,
        host_partition="all",
        host_type="io",
        user_filter="all",
        exec_mode="sync",
        display=False,
    ):
        """cluster(action='info', nhost=1, host_partition='all', host_type='io', user_filter='all', exec_mode='sync', display=False) -> None : wrapper of the operator OPH_CLUSTER

        :param action: info|info_cluster|deploy|undeploy
        :type action: str
        :param nhost: number of hosts to be reserved as well as number of I/O servers to be started
        :type nhost: int
        :param host_partition: name of user-defined partition to be used
        :type host_partition: str
        :param host_type: type of partition to be deployed
        :type host_type: str
        :param user_filter: name of user to be used as filter
        :type user_filter: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or Cube.client.host_partition is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_cluster "

            if action is not None:
                query += "action=" + str(action) + ";"
            if nhost is not None:
                query += "nhost=" + str(nhost) + ";"
            if host_partition is not None:
                query += "host_partition=" + str(host_partition) + ";"
            if host_type is not None:
                query += "host_type=" + str(host_type) + ";"
            if user_filter is not None:
                query += "user_filter=" + str(user_filter) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def containerschema(
        cls,
        container=None,
        cwd=None,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """containerschema(container=None, cwd=None, exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_CONTAINERSCHEMA

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param objkey_filter: filter on the output of the operator
        :type objkey_filter: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or container is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client, container or cwd is None")

            query = "oph_containerschema "

            if container is not None:
                query += "container=" + str(container) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def createcontainer(
        cls,
        exec_mode="sync",
        container=None,
        cwd=None,
        dim=None,
        dim_type="double",
        hierarchy="oph_base",
        base_time="1900-01-01 00:00:00",
        units="d",
        calendar="standard",
        month_lengths="31,28,31,30,31,30,31,31,30,31,30,31",
        leap_year=0,
        leap_month=2,
        vocabulary="CF",
        compressed="no",
        description="-",
        save="yes",
        display=False,
    ):
        """createcontainer(exec_mode='sync', container=None, cwd=None, dim=None, dim_type="double", hierarchy='oph_base',
                           base_time='1900-01-01 00:00:00', units='d', calendar='standard',
                           month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', leap_year=0, leap_month=2, vocabulary='CF',
                           compressed='no', description='-', save='yes', display=False) -> dict or None : wrapper of the operator OPH_CREATECONTAINER

        :param exec_mode: async or sync
        :type exec_mode: str
        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param dim: pipe (|) separated list of dimension names
        :type dim: str
        :param dim_type: pipe (|) separated list of dimension types (int|float|long|double)
        :type dim_type: str
        :param hierarchy: pipe (|) separated list of dimension hierarchies (oph_base|oph_time)
        :type hierarchy: str
        :param base_time: reference time
        :type base_time: str
        :param units: unit of time
        :type units: str
        :param calendar: calendar used
        :type calendar: str
        :param month_lengths: comma-separated list of month lengths
        :type month_lengths: str
        :param leap_year: leap year
        :type leap_year: int
        :param leap_month: leap month
        :type leap_month: int
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param compressed: yes or no
        :type compressed: str
        :param description: additional description to be associated with the output container
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or container is None or dim is None or dim_type is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client, container, dim, dim_type or cwd is None")

            query = "oph_createcontainer "

            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if container is not None:
                query += "container=" + str(container) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if dim is not None:
                query += "dim=" + str(dim) + ";"
            if dim_type is not None:
                query += "dim_type=" + str(dim_type) + ";"
            if hierarchy is not None:
                query += "hierarchy=" + str(hierarchy) + ";"
            if base_time is not None:
                query += "base_time=" + str(base_time) + ";"
            if units is not None:
                query += "units=" + str(units) + ";"
            if calendar is not None:
                query += "calendar=" + str(calendar) + ";"
            if month_lengths is not None:
                query += "month_lengths=" + str(month_lengths) + ";"
            if leap_year is not None:
                query += "leap_year=" + str(leap_year) + ";"
            if leap_month is not None:
                query += "leap_month=" + str(leap_month) + ";"
            if vocabulary is not None:
                query += "vocabulary=" + str(vocabulary) + ";"
            if compressed is not None:
                query += "compressed=" + str(compressed) + ";"
            if description is not None:
                query += "description=" + str(description) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def deletecontainer(
        cls,
        container=None,
        container_pid="-",
        force="no",
        cwd=None,
        nthreads=1,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=False,
    ):
        """deletecontainer(container=None, container_pid='-', force='no', cwd=None, nthreads=1, exec_mode='sync', objkey_filter='all', save='yes', display=False)
             -> None : wrapper of the operator OPH_DELETECONTAINER

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param container_pid: PID of the input container
        :type container_pid: str
        :param force: yes or no
        :type force: str
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or ((container is None or (cwd is None and Cube.client.cwd is None)) and container_pid == "-"):
                raise RuntimeError("Cube.client, container and container_pid or cwd is None")

            query = "oph_deletecontainer "

            if container is not None:
                query += "container=" + str(container) + ";"
            if container_pid is not None:
                query += "container_pid=" + str(container_pid) + ";"
            if force is not None:
                query += "force=" + str(force) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if nthreads is not None:
                query += "nthreads=" + str(nthreads) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def cancel(cls, id=None, type="kill", objkey_filter="all", display=False):
        """cancel(id=None, type='kill', objkey_filter='all', display=False) -> None : wrapper of the operator OPH_CANCEL

        :param id: identifier of the workflow to be stopped
        :type id: int
        :param type: kill|abort|stop
        :type type: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or id is None:
                raise RuntimeError("Cube.client or id is None")

            query = "oph_cancel "

            if id is not None:
                query += "id=" + str(id) + ";"
            if type is not None:
                query += "type=" + str(type) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def service(
        cls,
        status="",
        level=1,
        enable="none",
        disable="none",
        objkey_filter="all",
        display=False,
    ):
        """service(status='', level=1, enable='none', disable='none', objkey_filter='all', display=False) -> dict or None : wrapper of the operator OPH_SERVICE

        :param status: up|down
        :type status: str
        :param level: 1|2
        :type level: int
        :param enable: list of the users to be enabled ('all' to enable all users)
        :type enable: str
        :param disable: list of the users to be disabled ('all' to disable all users)
        :type disable: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_service "

            if status is not None:
                query += "status=" + str(status) + ";"
            if level is not None:
                query += "level=" + str(level) + ";"
            if enable is not None:
                query += "enable=" + str(enable) + ";"
            if disable is not None:
                query += "disable=" + str(disable) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def get_config(cls, key="all", objkey_filter="all", display=True):
        """get_config(key='all', objkey_filter='all', display=True) -> dict or None : wrapper of the operator OPH_GET_CONFIG

        :param key: all|OPH_XML_URL|OPH_SESSION_ID|OPH_EXEC_MODE|OPH_NCORES|OPH_DATACUBE|OPH_CWD|OPH_CDD|OPH_BASE_SRC_PATH
        :type key: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_get_config "

            if key is not None:
                query += "key=" + str(key) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def manage_session(
        cls,
        action="list",
        session="this",
        key="user",
        value="null",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """manage_session(action='list', session='this', key='user', value='null', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_MANAGE_SESSION

        :param action: disable|enable|env|grant|list|listusers|new|remove|revoke|setenv
        :type action: str
        :param session: link to intended session
        :type session: str
        :param key: active|autoremove|label|user
        :type key: str
        :param value:  value of the key
        :type value: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client or action is None")

            query = "oph_manage_session "

            if action is not None:
                query += "action=" + str(action) + ";"
            if session is not None:
                query += "session=" + str(session) + ";"
            if key is not None:
                query += "key=" + str(key) + ";"
            if value is not None:
                query += "value=" + str(value) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def instances(
        cls,
        action="read",
        level=1,
        host_filter="all",
        nhost=0,
        host_partition="all",
        ioserver_filter="all",
        host_status="all",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """instances(level=1, action='read', level=1, host_filter='all', nhost=0, host_partition='all', ioserver_filter='all',
                     host_status='all', exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_INSTANCES

        :param action: read|add|remove
        :type action: str
        :param level: 1|2|3
        :type level: int
        :param host_filter: optional filter on host name
        :type host_filter: str
        :param nhost: number of hosts to be grouped in the user-defined partition (add or remove mode)
        :type nhost: int
        :param host_partition: optional filter on host partition name
        :type host_partition: str
        :param ioserver_filter: mysql_table|ophidiaio_memory|all
        :type ioserver_filter: str
        :param host_status: up|down|all
        :type host_status: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_instances "

            if action is not None:
                query += "action=" + str(action) + ";"
            if level is not None:
                query += "level=" + str(level) + ";"
            if host_filter is not None:
                query += "host_filter=" + str(host_filter) + ";"
            if nhost is not None:
                query += "nhost=" + str(nhost) + ";"
            if host_partition is not None:
                query += "host_partition=" + str(host_partition) + ";"
            if ioserver_filter is not None:
                query += "ioserver_filter=" + str(ioserver_filter) + ";"
            if host_status is not None:
                query += "host_status=" + str(host_status) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def log_info(
        cls,
        log_type="server",
        container_id=0,
        ioserver="mysql",
        nlines=10,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """log_info(log_type='server', container_id=0, ioserver='mysql', nlines=10, exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_LOG_INFO

        :param log_type: server|container|ioserver
        :type log_type: str
        :param container_id: id of the container related to the requested log
        :type container_id: int
        :param ioserver: mysql|ophidiaio
        :type ioserver: str
        :param nlines: maximum number of lines to be displayed
        :type nlines: int
        :param objkey_filter: filter the objkey
        :type objkey_filter: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_log_info "

            if log_type is not None:
                query += "log_type=" + str(log_type) + ";"
            if container_id is not None:
                query += "container_id=" + str(container_id) + ";"
            if ioserver is not None:
                query += "ioserver=" + str(ioserver) + ";"
            if nlines is not None:
                query += "nlines=" + str(nlines) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def loggingbk(
        cls,
        session_level=0,
        job_level=0,
        mask=000,
        session_filter="all",
        session_label_filter="all",
        session_creation_filter="1900-01-01 00:00:00,2100-01-01 00:00:00",
        workflowid_filter="all",
        markerid_filter="all",
        parent_job_filter="all",
        job_creation_filter="1900-01-01 00:00:00,2100-01-01 00:00:00",
        job_status_filter="all",
        submission_string_filter="all",
        job_start_filter="1900-01-01 00:00:00,2100-01-01 00:00:00",
        job_end_filter="1900-01-01 00:00:00,2100-01-01 00:00:00",
        nlines=100,
        objkey_filter="all",
        exec_mode="sync",
        save="yes",
        display=True,
    ):
        """loggingbk(session_level=0, job_level=0, mask=000, session_filter='all', session_label_filter='all',
                     session_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', workflowid_filter='all', markerid_filter='all',
                     parent_job_filter='all', job_creation_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', job_status_filter='all',
                     submission_string_filter='all', job_start_filter='1900-01-01 00:00:00,2100-01-01 00:00:00',
                     job_end_filter='1900-01-01 00:00:00,2100-01-01 00:00:00', nlines=100, objkey_filter='all', exec_mode='sync',
                     save='yes', display=True)
             -> dict or None : wrapper of the operator OPH_LOGGINGBK

        :param session_level: 0|1
        :type session_level: int
        :param job_level: 0|1|2
        :type job_level: int
        :param mask: 3-digit mask for job output
        :type mask: str
        :param session_filter: filter on a particular sessionID
        :type session_filter: str
        :param session_label_filter: filter on a particular session label
        :type session_label_filter: str
        :param session_creation_filter: filter on session creation date
        :type session_creation_filter: str
        :param workflowid_filter: filter on a particular workflow ID
        :type workflowid_filter: str
        :param markerid_filter: filter on a particular marker ID
        :type markerid_filter: str
        :param parent_job_filter: filter on a particular parent job ID
        :type parent_job_filter: str
        :param job_creation_filter: filter on job submission date as with session_creation_filter
        :type job_creation_filter: str
        :param job_status_filter: filter on job status
        :type job_status_filter: str
        :param submission_string_filter: filter on submission string
        :type submission_string_filter: str
        :param job_start_filter: filter on job start date as with session_creation_filter
        :type job_start_filter: str
        :param job_end_filter: filter on job end date as with session_creation_filter
        :type job_end_filter: str
        :param nlines: maximum number of lines to be displayed
        :type nlines: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_loggingbk "

            if session_level is not None:
                query += "session_level=" + str(session_level) + ";"
            if job_level is not None:
                query += "job_level=" + str(job_level) + ";"
            if mask is not None:
                query += "mask=" + str(mask) + ";"
            if nlines is not None:
                query += "nlines=" + str(nlines) + ";"
            if session_filter is not None:
                query += "session_filter=" + str(session_filter) + ";"
            if session_label_filter is not None:
                query += "session_label_filter=" + str(session_label_filter) + ";"
            if session_creation_filter is not None:
                query += "session_creation_filter=" + str(session_creation_filter) + ";"
            if workflowid_filter is not None:
                query += "workflowid_filter=" + str(workflowid_filter) + ";"
            if markerid_filter is not None:
                query += "markerid_filter=" + str(markerid_filter) + ";"
            if parent_job_filter is not None:
                query += "parent_job_filter=" + str(parent_job_filter) + ";"
            if job_creation_filter is not None:
                query += "job_creation_filter=" + str(job_creation_filter) + ";"
            if job_status_filter is not None:
                query += "job_status_filter=" + str(job_status_filter) + ";"
            if submission_string_filter is not None:
                query += "submission_string_filter=" + str(submission_string_filter) + ";"
            if job_start_filter is not None:
                query += "job_start_filter=" + str(job_start_filter) + ";"
            if job_end_filter is not None:
                query += "job_end_filter=" + str(job_end_filter) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def folder(
        cls,
        command=None,
        path="-",
        cwd=None,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=False,
    ):
        """folder(command=None, cwd=None, path=None, exec_mode='sync', save='yes', display=False) -> None : wrapper of the operator OPH_FOLDER

        :param command: cd|mkdir|mv|rm
        :type command: str
        :param cwd: current working directory
        :type cwd: str
        :param path: absolute or relative path
        :type path: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or command is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client, command or cwd is None")

            query = "oph_folder "

            if command is not None:
                query += "command=" + str(command) + ";"
            if path is not None:
                query += "path=" + str(path) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def fs(
        cls,
        command="ls",
        dpath="-",
        file="-",
        measure="-",
        cdd=None,
        recursive="no",
        depth=0,
        realpath="no",
        subset_dims="none",
        subset_type="index",
        subset_filter="all",
        time_filter="yes",
        vocabulary="CF",
        offset=0,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=False,
    ):
        """fs(command='ls', dpath='-', file='-', measure='-', cdd=None, recursive='no', depth=0, realpath='no', subset_dims='none',
              subset_type='index', subset_filter='all', time_filter='yes', vocabulary='CF', exec_mode='sync', offset=0, save='yes',
              display=False) -> None : wrapper of the operator OPH_FS

        :param command: ls|cd|mkdir|rm|mv
        :type command: str
        :param dpath: paths needed by commands
        :type dpath: str
        :param file: file filter
        :type file: str
        :param measure: measure filter
        :type measure: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param recursive: if search is done recursively or not
        :type recursive: str
        :param depth: maximum folder depth to be explored in case of recursion
        :type depth: int
        :param realpath: yes|no
        :type realpath: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_type: index|coord
        :type subset_type: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param vocabulary: metadata vocabulary used for time filters
        :type vocabulary: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client, is None")

            query = "oph_fs "

            if command is not None:
                query += "command=" + str(command) + ";"
            if dpath is not None:
                query += "dpath=" + str(dpath) + ";"
            if file is not None:
                query += "file=" + str(file) + ";"
            if measure is not None:
                query += "measure=" + str(measure) + ";"
            if cdd is not None:
                query += "cdd=" + str(cdd) + ";"
            if recursive is not None:
                query += "recursive=" + str(recursive) + ";"
            if depth is not None:
                query += "depth=" + str(depth) + ";"
            if realpath is not None:
                query += "realpath=" + str(realpath) + ";"
            if subset_dims is not None:
                query += "subset_dims=" + str(subset_dims) + ";"
            if subset_type is not None:
                query += "subset_type=" + str(subset_type) + ";"
            if subset_filter is not None:
                query += "subset_filter=" + str(subset_filter) + ";"
            if time_filter is not None:
                query += "time_filter=" + str(time_filter) + ";"
            if vocabulary is not None:
                query += "vocabulary=" + str(vocabulary) + ";"
            if offset is not None:
                query += "offset=" + str(offset) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def wait(
        cls,
        type="clock",
        timeout=1,
        timeout_type="duration",
        key="-",
        value="-",
        filename="-",
        measure="-",
        message="-",
        subset_dims="none",
        subset_type="index",
        subset_filter="all",
        time_filter="yes",
        offset=0,
        run="yes",
        exec_mode="sync",
        save="yes",
        display=False,
    ):
        """wait(type="clock", timeout=1, timeout_type="duration", key="-", value="-", filename="-", measure="-", message="-", subset_dims="none",
                subset_type="index", subset_filter="all", time_filter="yes", offset=0, run="yes", exec_mode="sync", save="yes", display=False)
            -> None : wrapper of the operator OPH_WAIT

        :param type: clock|input|file
        :type type: str
        :param timeout: it is the duration (in seconds) or the end instant of the waiting interval
        :type timeout: int
        :param timeout_type: duration|deadline
        :type timeout_type: str
        :param key: name of the parameter
        :type key: str
        :param value: value of the parameter
        :type value: str
        :param filename: name of the file to be checked
        :type filename: str
        :param measure: name of the measure related to input file
        :type measure: str
        :param message: this user-defined message is appended to response in order to notify the waiting reason
        :type message: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_type: index|coord
        :type subset_type: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param run: yes|no
        :type run: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client, is None")

            query = "oph_wait "

            if type is not None:
                query += "type=" + str(type) + ";"
            if timeout is not None:
                query += "timeout=" + str(timeout) + ";"
            if timeout_type is not None:
                query += "timeout_type=" + str(timeout_type) + ";"
            if measure is not None:
                query += "measure=" + str(measure) + ";"
            if filename is not None:
                query += "filename=" + str(filename) + ";"
            if message is not None:
                query += "message=" + str(message) + ";"
            if key is not None:
                query += "key=" + str(key) + ";"
            if value is not None:
                query += "value=" + str(value) + ";"
            if subset_dims is not None:
                query += "subset_dims=" + str(subset_dims) + ";"
            if subset_type is not None:
                query += "subset_type=" + str(subset_type) + ";"
            if subset_filter is not None:
                query += "subset_filter=" + str(subset_filter) + ";"
            if time_filter is not None:
                query += "time_filter=" + str(time_filter) + ";"
            if offset is not None:
                query += "offset=" + str(offset) + ";"
            if run is not None:
                query += "run=" + str(run) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def tasks(
        cls,
        cube_filter="all",
        operator_filter="all",
        path="-",
        cwd=None,
        recursive="no",
        container="all",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """tasks(cls, cube_filter='all', path='-', operator_filter='all', cwd=None, recursive='no', container='all',
                 objkey_filter='all', exec_mode='sync', save='yes', display=True)
             -> dict or None : wrapper of the operator OPH_tasks

        :param cube_filter: optional filter on cube
        :type cube_filter: str
        :param operator_filter: optional filter on the name of the operators
        :type operator_filter: str
        :param path: optional filter on absolute or relative path
        :type path: str
        :param cwd: current working directory
        :type cwd: str
        :param recursive: if the search is done recursively or not
        :type recursive: yes|no
        :param container: optional filter on container name
        :type container: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """
        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_tasks "

            if cube_filter is not None:
                query += "cube_filter=" + str(cube_filter) + ";"
            if operator_filter is not None:
                query += "operator_filter=" + str(operator_filter) + ";"
            if path is not None:
                query += "path=" + str(path) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if recursive is not None:
                query += "recursive=" + str(recursive) + ";"
            if container is not None:
                query += "container=" + str(container) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def showgrid(
        cls,
        container=None,
        grid="all",
        dim="all",
        show_index="no",
        cwd=None,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """showgrid(container=None, grid='all', dim='all', show_index='no', cwd=None, exec_mode='sync', objkey_filter='all',
                    save='yes', display=True) -> dict or None : wrapper of the operator OPH_SHOWGRID

        :param container: name of the input container
        :type container: str
        :param grid: name of grid to show
        :type grid: str
        :param dim: name of dimension to show
        :type dim: str
        :param show_index: yes|no
        :type show_index: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or container is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client, container or cwd is None")

            query = "oph_showgrid "

            if container is not None:
                query += "container=" + str(container) + ";"
            if grid is not None:
                query += "grid=" + str(grid) + ";"
            if dim is not None:
                query += "dim=" + str(dim) + ";"
            if show_index is not None:
                query += "show_index=" + str(show_index) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def search(
        cls,
        container_filter="all",
        metadata_key_filter="all",
        metadata_value_filter="all",
        path="-",
        cwd=None,
        recursive="no",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """search(path='-', metadata_value_filter='all', exec_mode='sync', metadata_key_filter='all', container_filter='all',
                 objkey_filter='all', cwd=None, recursive='no', save='yes', display=True)
             -> dict or None : wrapper of the operator OPH_SEARCH

        :param container_filter: filter on container name
        :type container_filter: str
        :param metadata_key_filter: name of the key (or the enumeration of keys) identifying requested metadata
        :type metadata_key_filter: str
        :param metadata_value_filter: value of the key (or the enumeration of keys) identifying requested metadata
        :type metadata_value_filter: str
        :param path: absolute/relative path used as the starting point of the recursive search
        :type path: str
        :param cwd: current working directory
        :type cwd: str
        :param recursive: if the search is done recursively or not
        :type recursive: yes|no
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client or cwd is None")

            query = "oph_search "

            if container_filter is not None:
                query += "container_filter=" + str(container_filter) + ";"
            if metadata_key_filter is not None:
                query += "metadata_key_filter=" + str(metadata_key_filter) + ";"
            if metadata_value_filter is not None:
                query += "metadata_value_filter=" + str(metadata_value_filter) + ";"
            if path is not None:
                query += "path=" + str(path) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if recursive is not None:
                query += "recursive=" + str(recursive) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def hierarchy(
        cls,
        hierarchy="all",
        hierarchy_version="latest",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """hierarchy(hierarchy='all', hierarchy_version='latest', exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_HIERARCHY

        :param hierarchy: name of the requested hierarchy
        :type hierarchy: str
        :param hierarchy_version: version of the requested hierarchy
        :type hierarchy_version: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_hierarchy "

            if hierarchy is not None:
                query += "hierarchy=" + str(hierarchy) + ";"
            if hierarchy_version is not None:
                query += "hierarchy_version=" + str(hierarchy_version) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def list(
        cls,
        level=1,
        exec_mode="sync",
        path="-",
        cwd=None,
        container_filter="all",
        cube="all",
        host_filter="all",
        dbms_filter="all",
        measure_filter="all",
        ntransform="all",
        src_filter="all",
        db_filter="all",
        recursive="no",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """list(level=1, exec_mode='sync', path='-', cwd=None, container_filter='all', cube='all', host_filter='all', dbms_filter='all',
                measure_filter='all', ntransform='all', src_filter='all', db_filter='all', recursive='no', objkey_filter='all',
                save='yes', display=True) -> dict or None : wrapper of the operator OPH_LIST

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
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client or cwd is None")

            query = "oph_list "

            if level is not None:
                query += "level=" + str(level) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if path is not None:
                query += "path=" + str(path) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if container_filter is not None:
                query += "container_filter=" + str(container_filter) + ";"
            if cube is not None:
                query += "cube=" + str(cube) + ";"
            if host_filter is not None:
                query += "host_filter=" + str(host_filter) + ";"
            if dbms_filter is not None:
                query += "dbms_filter=" + str(dbms_filter) + ";"
            if measure_filter is not None:
                query += "measure_filter=" + str(measure_filter) + ";"
            if ntransform is not None:
                query += "ntransform=" + str(ntransform) + ";"
            if src_filter is not None:
                query += "src_filter=" + str(src_filter) + ";"
            if db_filter is not None:
                query += "db_filter=" + str(db_filter) + ";"
            if recursive is not None:
                query += "recursive=" + str(recursive) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def randcube(
        cls,
        ncores=1,
        exec_mode="sync",
        container=None,
        cwd=None,
        host_partition="auto",
        ioserver="mysql_table",
        schedule=0,
        algorithm="default",
        policy="rr",
        nhost=0,
        run="yes",
        nfrag=1,
        ntuple=1,
        measure=None,
        measure_type=None,
        exp_ndim=None,
        dim=None,
        concept_level="c",
        dim_size=None,
        compressed="no",
        grid="-",
        description="-",
        save="yes",
        display=False,
    ):
        """randcube(ncores=1, exec_mode='sync', container=None, cwd=None, host_partition='auto', ioserver='mysql_table', schedule=0,
                    algorithm='default', policy='rr', nhost=0, run='yes', nfrag=1, ntuple=1, measure=None, measure_type=None,
                    exp_ndim=None, dim=None, concept_level='c', dim_size=None, compressed='no', grid='-', description='-',
                    save='yes', display=False) -> Cube or None : wrapper of the operator OPH_RANDCUBE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param host_partition: host partition name
        :type host_partition: str
        :param algorithm: default|temperatures
        :type algorithm: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param ioserver: mysql_table|ophdiaio_memory
        :type ioserver: str
        :param schedule: 0
        :type schedule: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param run: yes|no
        :type run: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param ntuple: number of tuples/fragment to use
        :type ntuple: int
        :param measure: measure to be imported
        :type measure: str
        :param measure_type: double|float|int|long|short|byte
        :type measure_type: str
        :param exp_ndim: number of explicit dimensions in dim
        :type exp_ndim: int
        :param dim: pipe (|) separated list of dimension names
        :type dim: str
        :param concept_level: pipe (|) separated list of dimensions hierarchy levels
        :type concept_level: str
        :param dim_size: pipe (|) separated list of dimension sizes
        :type dim_size: str
        :param compressed: yes|no
        :type compressed: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if (
            Cube.client is None
            or (cwd is None and Cube.client.cwd is None)
            or container is None
            or nfrag is None
            or ntuple is None
            or measure is None
            or measure_type is None
            or exp_ndim is None
            or dim is None
            or dim_size is None
        ):
            raise RuntimeError("Cube.client, cwd, container, nfrag, ntuple, measure, measure_type, exp_ndim, dim or dim_size is None")
        newcube = None

        query = "oph_randcube "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if cwd is not None:
            query += "cwd=" + str(cwd) + ";"
        if host_partition is not None:
            query += "host_partition=" + str(host_partition) + ";"
        if algorithm is not None:
            query += "algorithm=" + str(algorithm) + ";"
        if policy is not None:
            query += "policy=" + str(policy) + ";"
        if ioserver is not None:
            query += "ioserver=" + str(ioserver) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nhost is not None:
            query += "nhost=" + str(nhost) + ";"
        if run is not None:
            query += "run=" + str(run) + ";"
        if nfrag is not None:
            query += "nfrag=" + str(nfrag) + ";"
        if ntuple is not None:
            query += "ntuple=" + str(ntuple) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if measure_type is not None:
            query += "measure_type=" + str(measure_type) + ";"
        if exp_ndim is not None:
            query += "exp_ndim=" + str(exp_ndim) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if concept_level is not None:
            query += "concept_level=" + str(concept_level) + ";"
        if dim_size is not None:
            query += "dim_size=" + str(dim_size) + ";"
        if compressed is not None:
            query += "compressed=" + str(compressed) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def randcube2(
        cls,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        container=None,
        cwd=None,
        host_partition="auto",
        ioserver="ophidiaio_memory",
        schedule=0,
        algorithm="default",
        policy="rr",
        nhost=0,
        run="yes",
        nfrag=1,
        ntuple=1,
        measure=None,
        measure_type=None,
        exp_ndim=None,
        dim=None,
        concept_level="c",
        dim_size=None,
        compressed="no",
        grid="-",
        description="-",
        save="yes",
        display=False,
    ):
        """randcube2(ncores=1, nthreads=1, exec_mode='sync', container=None, cwd=None, host_partition='auto',
                    ioserver='ophidiaio_memory', schedule=0, algorithm='default', policy='rr', nhost=0, run='yes', nfrag=1,
                    ntuple=1, measure=None, measure_type=None, exp_ndim=None, dim=None, concept_level='c', dim_size=None,
                    compressed='no', grid='-', description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_RANDCUBE2

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param host_partition: host partition name
        :type host_partition: str
        :param algorithm: default|temperatures
        :type algorithm: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param ioserver: ophdiaio_memory
        :type ioserver: str
        :param schedule: 0
        :type schedule: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param run: yes|no
        :type run: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param ntuple: number of tuples/fragment to use
        :type ntuple: int
        :param measure: measure to be imported
        :type measure: str
        :param measure_type: double|float|int|long|short|byte
        :type measure_type: str
        :param exp_ndim: number of explicit dimensions in dim
        :type exp_ndim: int
        :param dim: pipe (|) separated list of dimension names
        :type dim: str
        :param concept_level: pipe (|) separated list of dimensions hierarchy levels
        :type concept_level: str
        :param dim_size: pipe (|) separated list of dimension sizes
        :type dim_size: str
        :param compressed: yes|no
        :type compressed: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if (
            Cube.client is None
            or (cwd is None and Cube.client.cwd is None)
            or container is None
            or nfrag is None
            or ntuple is None
            or measure is None
            or measure_type is None
            or exp_ndim is None
            or dim is None
            or dim_size is None
        ):
            raise RuntimeError("Cube.client, cwd, container, nfrag, ntuple, measure, measure_type, exp_ndim, dim or dim_size is None")
        newcube = None

        query = "oph_randcube2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if cwd is not None:
            query += "cwd=" + str(cwd) + ";"
        if host_partition is not None:
            query += "host_partition=" + str(host_partition) + ";"
        if algorithm is not None:
            query += "algorithm=" + str(algorithm) + ";"
        if policy is not None:
            query += "policy=" + str(policy) + ";"
        if ioserver is not None:
            query += "ioserver=" + str(ioserver) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nhost is not None:
            query += "nhost=" + str(nhost) + ";"
        if run is not None:
            query += "run=" + str(run) + ";"
        if nfrag is not None:
            query += "nfrag=" + str(nfrag) + ";"
        if ntuple is not None:
            query += "ntuple=" + str(ntuple) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if measure_type is not None:
            query += "measure_type=" + str(measure_type) + ";"
        if exp_ndim is not None:
            query += "exp_ndim=" + str(exp_ndim) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if concept_level is not None:
            query += "concept_level=" + str(concept_level) + ";"
        if dim_size is not None:
            query += "dim_size=" + str(dim_size) + ";"
        if compressed is not None:
            query += "compressed=" + str(compressed) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def explorenc(
        cls,
        exec_mode="sync",
        schedule=0,
        measure="-",
        src_path=None,
        cdd=None,
        exp_dim="-",
        imp_dim="-",
        subset_dims="none",
        subset_type="index",
        subset_filter="all",
        limit_filter=100,
        show_index="no",
        show_id="no",
        show_time="no",
        show_stats="00000000000000",
        show_fit="no",
        level=0,
        imp_num_point=0,
        offset=50,
        operation="avg",
        wavelet="no",
        wavelet_ratio=0,
        wavelet_coeff="no",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """explorenc(exec_mode='sync', schedule=0, measure='-', src_path=None, cdd=None, exp_dim='-', imp_dim='-', subset_dims='none',
                     subset_type='index', subset_filter='all', limit_filter=100, show_index='no', show_id='no', show_time='no',
                     show_stats='00000000000000', show_fit='no', level=0, imp_num_point=0, offset=50, operation='avg', wavelet='no',
                     wavelet_ratio=0, wavelet_coeff='no', objkey_filter='all', save='yes', display=True)
             -> None : wrapper of the operator OPH_EXPLORENC

        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param measure: name of the measure related to the NetCDF file
        :type measure: str
        :param src_path: path of file to be imported
        :type src_path: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param exp_dim: pipe (|) separated list of explicit dimension names
        :type exp_dim: str
        :param imp_dim: pipe (|) separated list of implicit dimension names
        :type imp_dim: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param limit_filter: max number of lines
        :type limit_filter: int
        :param show_index: yes|no
        :type show_index: str
        :param show_id: yes|no
        :type show_id: str
        :param show_time: yes|no
        :type show_time: str
        :param show_stats: (15-bit) mask to set statistics to be computed for each time serie
        :type show_stats: str
        :param show_fit: yes|no
        :type show_fit: str
        :param level: 0|1|2
        :type level: int
        :param imp_num_point: number of points which measure values must be distribuited along by interpolation
        :type imp_num_point: int
        :param offset: relative offset to be used to set reduction interval bounds (percentage)
        :type offset: float
        :param operation: max|min|avg|sum
        :type operation: str
        :param wavelet: yes|no|only
        :type wavelet: str
        :param wavelet_ratio: fraction of wavelet transform coefficients that are cleared by the filter (percentage)
        :type wavelet_ratio: float
        :param wavelet_coeff: yes|no
        :type wavelet_coeff: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or src_path is None:
                raise RuntimeError("Cube.client or src_path")

            query = "oph_explorenc "

            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if schedule is not None:
                query += "schedule=" + str(schedule) + ";"
            if measure is not None:
                query += "measure=" + str(measure) + ";"
            if src_path is not None:
                query += "src_path=" + str(src_path) + ";"
            if cdd is not None:
                query += "cdd=" + str(cdd) + ";"
            if exp_dim is not None:
                query += "exp_dim=" + str(exp_dim) + ";"
            if imp_dim is not None:
                query += "imp_dim=" + str(imp_dim) + ";"
            if subset_dims is not None:
                query += "subset_dims=" + str(subset_dims) + ";"
            if subset_type is not None:
                query += "subset_type=" + str(subset_type) + ";"
            if subset_filter is not None:
                query += "subset_filter=" + str(subset_filter) + ";"
            if limit_filter is not None:
                query += "limit_filter=" + str(limit_filter) + ";"
            if show_index is not None:
                query += "show_index=" + str(show_index) + ";"
            if show_id is not None:
                query += "show_id=" + str(show_id) + ";"
            if show_time is not None:
                query += "show_time=" + str(show_time) + ";"
            if show_stats is not None:
                query += "show_stats=" + str(show_stats) + ";"
            if show_fit is not None:
                query += "show_fit=" + str(show_fit) + ";"
            if level is not None:
                query += "level=" + str(level) + ";"
            if imp_num_point is not None:
                query += "imp_num_point=" + str(imp_num_point) + ";"
            if offset is not None:
                query += "offset=" + str(offset) + ";"
            if operation is not None:
                query += "operation=" + str(operation) + ";"
            if wavelet is not None:
                query += "wavelet=" + str(wavelet) + ";"
            if wavelet_ratio is not None:
                query += "wavelet_ratio=" + str(wavelet_ratio) + ";"
            if wavelet_coeff is not None:
                query += "wavelet_coeff=" + str(wavelet_coeff) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def importnc(
        cls,
        container="-",
        cwd=None,
        exp_dim="auto",
        host_partition="auto",
        imp_dim="auto",
        measure=None,
        src_path=None,
        cdd=None,
        compressed="no",
        exp_concept_level="c",
        grid="-",
        imp_concept_level="c",
        import_metadata="yes",
        check_compliance="no",
        offset=0,
        ioserver="mysql_table",
        ncores=1,
        nfrag=0,
        nhost=0,
        subset_dims="none",
        subset_filter="all",
        time_filter="yes",
        subset_type="index",
        exec_mode="sync",
        base_time="1900-01-01 00:00:00",
        calendar="standard",
        hierarchy="oph_base",
        leap_month=2,
        leap_year=0,
        month_lengths="31,28,31,30,31,30,31,31,30,31,30,31",
        run="yes",
        units="d",
        vocabulary="CF",
        description="-",
        policy="rr",
        schedule=0,
        check_grid="no",
        save="yes",
        display=False,
    ):
        """importnc(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                    cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                    check_compliance='no', offset=0, ioserver='mysql_table', ncores=1, nfrag=0, nhost=0, subset_dims='none',
                    subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00',
                    calendar='standard', hierarchy='oph_base', leap_month=2, leap_year=0,
                    month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF', description='-',
                    policy='rr', schedule=0, check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_IMPORTNC

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
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
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param compressed: yes|no
        :type compressed: str
        :param exp_concept_level: pipe (|) separated list of explicit dimensions hierarchy levels
        :type exp_concept_level: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param imp_concept_level: pipe (|) separated list of implicit dimensions hierarchy levels
        :type imp_concept_level: str
        :param import_metadata: yes|no
        :type import_metadata: str
        :param check_compliance: yes|no
        :type check_compliance: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param ioserver: mysql_table|ophdiaio_memory
        :type ioserver: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param base_time: reference time
        :type base_time: str
        :param calendar: calendar used (standard|gregorian|proleptic_gregorian|julian|360_day|no_leap|all_leap|user_defined)
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
        :param units: unit of time (s|m|h|3|6|d)
        :type units: str
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or measure is None or src_path is None:
            raise RuntimeError("Cube.client, measure or src_path is None")
        newcube = None

        query = "oph_importnc "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if cwd is not None:
            query += "cwd=" + str(cwd) + ";"
        if host_partition is not None:
            query += "host_partition=" + str(host_partition) + ";"
        if ioserver is not None:
            query += "ioserver=" + str(ioserver) + ";"
        if import_metadata is not None:
            query += "import_metadata=" + str(import_metadata) + ";"
        if check_compliance is not None:
            query += "check_compliance=" + str(check_compliance) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nhost is not None:
            query += "nhost=" + str(nhost) + ";"
        if nfrag is not None:
            query += "nfrag=" + str(nfrag) + ";"
        if run is not None:
            query += "run=" + str(run) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if src_path is not None:
            query += "src_path=" + str(src_path) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if exp_dim is not None:
            query += "exp_dim=" + str(exp_dim) + ";"
        if imp_dim is not None:
            query += "imp_dim=" + str(imp_dim) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if exp_concept_level is not None:
            query += "exp_concept_level=" + str(exp_concept_level) + ";"
        if imp_concept_level is not None:
            query += "imp_concept_level=" + str(imp_concept_level) + ";"
        if compressed is not None:
            query += "compressed=" + str(compressed) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if hierarchy is not None:
            query += "hierarchy=" + str(hierarchy) + ";"
        if vocabulary is not None:
            query += "vocabulary=" + str(vocabulary) + ";"
        if base_time is not None:
            query += "base_time=" + str(base_time) + ";"
        if units is not None:
            query += "units=" + str(units) + ";"
        if calendar is not None:
            query += "calendar=" + str(calendar) + ";"
        if month_lengths is not None:
            query += "month_lengths=" + str(month_lengths) + ";"
        if leap_year is not None:
            query += "leap_year=" + str(leap_year) + ";"
        if leap_month is not None:
            query += "leap_month=" + str(leap_month) + ";"
        if policy is not None:
            query += "policy=" + str(policy) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def importnc2(
        cls,
        container="-",
        cwd=None,
        exp_dim="auto",
        host_partition="auto",
        imp_dim="auto",
        measure=None,
        src_path=None,
        cdd=None,
        compressed="no",
        exp_concept_level="c",
        grid="-",
        imp_concept_level="c",
        import_metadata="yes",
        check_compliance="no",
        offset=0,
        ioserver="ophidiaio_memory",
        ncores=1,
        nthreads=1,
        nfrag=0,
        nhost=0,
        subset_dims="none",
        subset_filter="all",
        time_filter="yes",
        subset_type="index",
        exec_mode="sync",
        base_time="1900-01-01 00:00:00",
        calendar="standard",
        hierarchy="oph_base",
        leap_month=2,
        leap_year=0,
        month_lengths="31,28,31,30,31,30,31,31,30,31,30,31",
        run="yes",
        units="d",
        vocabulary="CF",
        description="-",
        policy="rr",
        schedule=0,
        check_grid="no",
        save="yes",
        display=False,
    ):
        """importnc2(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                     cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                     check_compliance='no', offset=0, ioserver='ophidiaio_memory', ncores=1, nthreads=1, nfrag=0, nhost=0,
                     subset_dims='none', subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync',
                     base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2,
                     leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF',
                     description='-', policy='rr', schedule=0, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_IMPORTNC2


        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
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
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param compressed: yes|no
        :type compressed: str
        :param exp_concept_level: pipe (|) separated list of explicit dimensions hierarchy levels
        :type exp_concept_level: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param imp_concept_level: pipe (|) separated list of implicit dimensions hierarchy levels
        :type imp_concept_level: str
        :param import_metadata: yes|no
        :type import_metadata: str
        :param check_compliance: yes|no
        :type check_compliance: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param ioserver: ophdiaio_memory
        :type ioserver: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param base_time: reference time
        :type base_time: str
        :param calendar: calendar used (standard|gregorian|proleptic_gregorian|julian|360_day|no_leap|all_leap|user_defined)
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
        :param units: unit of time (s|m|h|3|6|d)
        :type units: str
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or measure is None or src_path is None:
            raise RuntimeError("Cube.client, measure or src_path is None")
        newcube = None

        query = "oph_importnc2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if cwd is not None:
            query += "cwd=" + str(cwd) + ";"
        if host_partition is not None:
            query += "host_partition=" + str(host_partition) + ";"
        if ioserver is not None:
            query += "ioserver=" + str(ioserver) + ";"
        if import_metadata is not None:
            query += "import_metadata=" + str(import_metadata) + ";"
        if check_compliance is not None:
            query += "check_compliance=" + str(check_compliance) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nhost is not None:
            query += "nhost=" + str(nhost) + ";"
        if nfrag is not None:
            query += "nfrag=" + str(nfrag) + ";"
        if run is not None:
            query += "run=" + str(run) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if src_path is not None:
            query += "src_path=" + str(src_path) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if exp_dim is not None:
            query += "exp_dim=" + str(exp_dim) + ";"
        if imp_dim is not None:
            query += "imp_dim=" + str(imp_dim) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if exp_concept_level is not None:
            query += "exp_concept_level=" + str(exp_concept_level) + ";"
        if imp_concept_level is not None:
            query += "imp_concept_level=" + str(imp_concept_level) + ";"
        if compressed is not None:
            query += "compressed=" + str(compressed) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if hierarchy is not None:
            query += "hierarchy=" + str(hierarchy) + ";"
        if vocabulary is not None:
            query += "vocabulary=" + str(vocabulary) + ";"
        if base_time is not None:
            query += "base_time=" + str(base_time) + ";"
        if units is not None:
            query += "units=" + str(units) + ";"
        if calendar is not None:
            query += "calendar=" + str(calendar) + ";"
        if month_lengths is not None:
            query += "month_lengths=" + str(month_lengths) + ";"
        if leap_year is not None:
            query += "leap_year=" + str(leap_year) + ";"
        if leap_month is not None:
            query += "leap_month=" + str(leap_month) + ";"
        if policy is not None:
            query += "policy=" + str(policy) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def importncs(
        cls,
        container="-",
        cwd=None,
        exp_dim="auto",
        host_partition="auto",
        imp_dim="auto",
        measure=None,
        src_path=None,
        cdd=None,
        compressed="no",
        exp_concept_level="c",
        grid="-",
        imp_concept_level="c",
        import_metadata="yes",
        check_compliance="no",
        offset=0,
        ioserver="ophidiaio_memory",
        ncores=1,
        nthreads=1,
        nfrag=0,
        nhost=0,
        subset_dims="none",
        subset_filter="all",
        time_filter="yes",
        subset_type="index",
        exec_mode="sync",
        base_time="1900-01-01 00:00:00",
        calendar="standard",
        hierarchy="oph_base",
        leap_month=2,
        leap_year=0,
        month_lengths="31,28,31,30,31,30,31,31,30,31,30,31",
        run="yes",
        units="d",
        vocabulary="CF",
        description="-",
        policy="rr",
        schedule=0,
        check_grid="no",
        save="yes",
        display=False,
    ):
        """importncs(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None,
                     cdd=None, compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='yes',
                     check_compliance='no', offset=0, ioserver='ophidiaio_memory', ncores=1, nthreads=1, nfrag=0, nhost=0,
                     subset_dims='none', subset_filter='all', time_filter='yes', subset_type='index', exec_mode='sync',
                     base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base', leap_month=2,
                     leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='CF',
                     description='-', policy='rr', schedule=0, check_grid='no', save='yes', display=False)
          -> Cube or None : wrapper of the operator OPH_IMPORTNCS


        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
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
        :param src_path: list of file paths to be imported
        :type src_path: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param compressed: yes|no
        :type compressed: str
        :param exp_concept_level: pipe (|) separated list of explicit dimensions hierarchy levels
        :type exp_concept_level: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param imp_concept_level: pipe (|) separated list of implicit dimensions hierarchy levels
        :type imp_concept_level: str
        :param import_metadata: yes|no
        :type import_metadata: str
        :param check_compliance: yes|no
        :type check_compliance: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param ioserver: ophdiaio_memory
        :type ioserver: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param base_time: reference time
        :type base_time: str
        :param calendar: calendar used (standard|gregorian|proleptic_gregorian|julian|360_day|no_leap|all_leap|user_defined)
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
        :param units: unit of time (s|m|h|3|6|d)
        :type units: str
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or measure is None or src_path is None:
            raise RuntimeError("Cube.client, measure or src_path is None")
        newcube = None

        query = "oph_importncs "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if cwd is not None:
            query += "cwd=" + str(cwd) + ";"
        if host_partition is not None:
            query += "host_partition=" + str(host_partition) + ";"
        if ioserver is not None:
            query += "ioserver=" + str(ioserver) + ";"
        if import_metadata is not None:
            query += "import_metadata=" + str(import_metadata) + ";"
        if check_compliance is not None:
            query += "check_compliance=" + str(check_compliance) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nhost is not None:
            query += "nhost=" + str(nhost) + ";"
        if nfrag is not None:
            query += "nfrag=" + str(nfrag) + ";"
        if run is not None:
            query += "run=" + str(run) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if src_path is not None:
            if type(src_path) == list:
                query += "src_path="
                for i in range(0, len(src_path)):
                    query += str(src_path[i])
                    if i < len(src_path) - 1:
                        query += "|"
                query += ";"
            else:
                query += "src_path=" + str(src_path) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if exp_dim is not None:
            query += "exp_dim=" + str(exp_dim) + ";"
        if imp_dim is not None:
            query += "imp_dim=" + str(imp_dim) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if exp_concept_level is not None:
            query += "exp_concept_level=" + str(exp_concept_level) + ";"
        if imp_concept_level is not None:
            query += "imp_concept_level=" + str(imp_concept_level) + ";"
        if compressed is not None:
            query += "compressed=" + str(compressed) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if hierarchy is not None:
            query += "hierarchy=" + str(hierarchy) + ";"
        if vocabulary is not None:
            query += "vocabulary=" + str(vocabulary) + ";"
        if base_time is not None:
            query += "base_time=" + str(base_time) + ";"
        if units is not None:
            query += "units=" + str(units) + ";"
        if calendar is not None:
            query += "calendar=" + str(calendar) + ";"
        if month_lengths is not None:
            query += "month_lengths=" + str(month_lengths) + ";"
        if leap_year is not None:
            query += "leap_year=" + str(leap_year) + ";"
        if leap_month is not None:
            query += "leap_month=" + str(leap_month) + ";"
        if policy is not None:
            query += "policy=" + str(policy) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def man(
        cls,
        function=None,
        function_version="latest",
        function_type="operator",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """man(function=None, function_type='operator', function_version='latest', exec_mode='sync', save='yes', display=True) -> dict or None : wrapper of the operator OPH_MAN

        :param function: operator or primitive name
        :type function: str
        :param function_type: operator|primitive
        :type function_type: str
        :param function_version: operator or primitive version
        :type function_version: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None or function is None:
                raise RuntimeError("Cube.client or function is None")

            query = "oph_man "

            if function is not None:
                query += "function=" + str(function) + ";"
            if function_version is not None:
                query += "function_version=" + str(function_version) + ";"
            if function_type is not None:
                query += "function_type=" + str(function_type) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def movecontainer(
        cls,
        container=None,
        cwd=None,
        exec_mode="sync",
        save="yes",
        display=False,
    ):
        """movecontainer(container=None, cwd=None, exec_mode='sync', save='yes', display=False) -> None : wrapper of the operator OPH_MOVECONTAINER

        :param container: container name
        :type container: str
        :param cwd: current working directory
        :type cwd: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        try:
            if Cube.client is None or container is None or (cwd is None and Cube.client.cwd is None):
                raise RuntimeError("Cube.client, container or cwd is None")

            query = "oph_movecontainer "

            if container is not None:
                query += "container=" + str(container) + ";"
            if cwd is not None:
                query += "cwd=" + str(cwd) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def operators(
        cls,
        operator_filter=None,
        limit_filter=0,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """operators(operator_filter=None, limit_filter=0, exec_mode='sync', save='yes', display=True) -> dict or None : wrapper of the operator OPH_OPERATORS_LIST

        :param operator_filter: filter on operator name
        :type operator_filter: str
        :param limit_filter: max number of lines
        :type limit_filter: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_operators_list "

            if operator_filter is not None:
                query += "operator_filter=" + str(operator_filter) + ";"
            if limit_filter is not None:
                query += "limit_filter=" + str(limit_filter) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def primitives(
        cls,
        level=1,
        dbms_filter=None,
        return_type="all",
        primitive_type="all",
        primitive_filter="",
        limit_filter=0,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """primitives(dbms_filter=None, level=1, limit_filter=0, primitive_filter=None, primitive_type=None, return_type=None, exec_mode='sync', objkey_filter='all', save='yes', display=True) ->
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
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_primitives_list "

            if level is not None:
                query += "level=" + str(level) + ";"
            if dbms_filter is not None:
                query += "dbms_filter=" + str(dbms_filter) + ";"
            if return_type is not None:
                query += "return_type=" + str(return_type) + ";"
            if primitive_type is not None:
                query += "primitive_type=" + str(primitive_type) + ";"
            if primitive_filter is not None:
                query += "primitive_filter=" + str(primitive_filter) + ";"
            if limit_filter is not None:
                query += "limit_filter=" + str(limit_filter) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def script(
        cls,
        script=":",
        args=" ",
        stdout="stdout",
        stderr="stderr",
        list="no",
        space="no",
        python_code=False,
        exec_mode="sync",
        ncores=1,
        save="yes",
        display=False,
    ):
        """script(script=':', args=' ', stdout='stdout', stderr='stderr', ncores=1, exec_mode='sync', list='no', space='no', python_code=False, save='yes', display=False) -> None : wrapper of the operator OPH_SCRIPT

        :param script: script/executable filename
        :type script: str
        :param args: pipe (|) separated list of arguments for the script
        :type args: str
        :param stdout: file/stream where stdout is redirected
        :type stdout: str
        :param stderr: file/stream where stderr is redirected
        :type stderr: str
        :param list: yes|no
        :type list: str
        :param space: yes|no
        :type space: str
        :param python_code: yes|no
        :type python_code: bool
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        def createScript(function):

            from inspect import getsource, signature
            import stat
            from os.path import expanduser, isdir
            from os import mkdir, chmod
            from time import time

            base_path = expanduser("~") + "/.ophidia/"
            script_path = base_path + function.__name__ + str(int(time() * 10**6)) + ".py"

            try:
                # Check if hidden folder exists or create it otherwise
                if not isdir(base_path):
                    mkdir(base_path, stat.S_IRWXU)

                fnct_text = getsource(function)
                fnct_signature = signature(function)
                fnct_args = fnct_signature.parameters
                fnct_args_num = len(fnct_args)

                script_args = "("
                if fnct_args_num > 0:
                    for i in range(1, fnct_args_num + 1):
                        script_args = script_args + "sys.argv[" + str(i) + "], "

                    script_args = script_args[:-2] + ")"
                else:
                    script_args = script_args + ")"

                script_text = (
                    """#!/bin/python

"""
                    + fnct_text
                    + """

if __name__ == '__main__':
    import sys
    if len(sys.argv) <= """
                    + str(fnct_args_num)
                    + """:
        print('Some input arguments are missing')
        sys.exit(1)

    if """
                    + function.__name__
                    + script_args
                    + """:
        sys.exit(1)
"""
                )

                with open(script_path, "w") as file:
                    file.write(script_text)
                    chmod(script_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP)

            except (IOError, ValueError, TypeError, OSError) as e:
                print(_get_linenumber(), "Python function error: ", e)
                raise RuntimeError()

            return script_path

        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_script "

            if script is not None:
                if python_code:
                    if sys.version_info[0] < 3:
                        raise RuntimeError("Python 3 is required to use a Python function as a scripts")
                    else:
                        script_path = createScript(script)
                        query += "script=" + str(script_path) + ";"
                else:
                    query += "script=" + str(script) + ";"

            if args is not None:
                query += "args=" + str(args) + ";"
            if stdout is not None:
                query += "stdout=" + str(stdout) + ";"
            if stderr is not None:
                query += "stderr=" + str(stderr) + ";"
            if list is not None:
                query += "list=" + str(list) + ";"
            if space is not None:
                query += "space=" + str(space) + ";"
            if exec_mode is not None:
                query += "exec_mode=" + str(exec_mode) + ";"
            if ncores is not None:
                query += "ncores=" + str(ncores) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                if script is not None and python_code:
                    os.remove(script_path)

                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    @classmethod
    def resume(
        cls,
        session="this",
        id=0,
        id_type="workflow",
        document_type="response",
        level=1,
        user="",
        status_filter="11111111",
        execute="no",
        checkpoint="all",
        save="no",
        objkey_filter="all",
        display=True,
    ):
        """resume(id=0, id_type='workflow', document_type='response', level=1, save='no', session='this', objkey_filter='all', user='',
                  execute='no', checkpoint='all', display=True) -> dict or None : wrapper of the operator OPH_RESUME

        :param session: identifier of the intended session, by default it is the working session
        :type session: str
        :param id: identifier of the intended workflow or marker, by default no filter is applied
        :type id: int
        :param id_type: workflow|marker
        :type id_type: str
        :param document_type: request|response
        :type document_type: str
        :param level: 0|1|2|3|4|5
        :type level: int
        :param user: filter by name of the submitter, by default no filter is applied
        :type user: str
        :param status_filter: filter by job status (bitmap)
        :type status_filter: str

        :param execute: yes|no
        :type execute: str
        :param checkpoint: retrieve the sub-workflow associated with a checkpoint
        :type checkpoint: str


        :param save: yes|no
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        response = None
        try:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")

            query = "oph_resume "

            if session is not None:
                query += "session=" + str(session) + ";"
            if id is not None:
                query += "id=" + str(id) + ";"
            if id_type is not None:
                query += "id_type=" + str(id_type) + ";"
            if document_type is not None:
                query += "document_type=" + str(document_type) + ";"
            if level is not None:
                query += "level=" + str(level) + ";"
            if user is not None:
                query += "user=" + str(user) + ";"
            if status_filter is not None:
                query += "status_filter=" + str(status_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"
            if objkey_filter is not None:
                query += "objkey_filter=" + str(objkey_filter) + ";"
            if save is not None:
                query += "save=" + str(save) + ";"

            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    @classmethod
    def mergecubes(
        cls,
        ncores=1,
        exec_mode="sync",
        cubes=None,
        schedule=0,
        container="-",
        mode="i",
        hold_values="no",
        number=1,
        order="none",
        description="-",
        save="yes",
        display=False,
    ):
        """mergecubes(ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', mode='i', hold_values='no', number=1, order='none', description='-', save='yes', display=False) -> Cube : wrapper of the operator OPH_MERGECUBES

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param container: optional container name
        :type container: str
        :param mode: interlace or append measures
        :type mode: str
        :param hold_values: enables the copy of the original values of implicit dimension
        :type hold_values: str
        :param number: number of replies of the first cube
        :type number: int
        :param order: criteria on which input cubes are ordered before merging
        :type order: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cubes is None:
            raise RuntimeError("Cube.client or cubes is None")
        newcube = None

        query = "oph_mergecubes "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if cubes is not None:
            query += "cubes=" + str(cubes) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if mode is not None:
            query += "mode=" + str(mode) + ";"
        if hold_values is not None:
            query += "hold_values=" + str(hold_values) + ";"
        if number is not None:
            query += "number=" + str(number) + ";"
        if order is not None:
            query += "order=" + str(order) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def mergecubes2(
        cls,
        ncores=1,
        exec_mode="sync",
        cubes=None,
        schedule=0,
        container="-",
        dim_type="long",
        number=1,
        order="none",
        description="-",
        dim="-",
        save="yes",
        display=False,
    ):
        """mergecubes2(ncores=1, exec_mode='sync', cubes=None, schedule=0, container='-', dim_type='long', number=1, order='none', description='-', dim='-', save='yes', display=False) -> Cube or None: wrapper of the operator OPH_MERGECUBES2

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param container: optional container name
        :type container: str
        :param dim_type: data type of the new dimension
        :type dim_type: str
        :param number: number of replies of the first cube
        :type number: int
        :param order: criteria on which input cubes are ordered before merging
        :type order: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param dim: name of the new dimension to be created
        :type dim: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cubes is None:
            raise RuntimeError("Cube.client or cubes is None")
        newcube = None

        query = "oph_mergecubes2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if cubes is not None:
            query += "cubes=" + str(cubes) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if number is not None:
            query += "number=" + str(number) + ";"
        if order is not None:
            query += "order=" + str(order) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if dim_type is not None:
            query += "dim_type=" + str(dim_type) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def __init__(
        self,
        container="-",
        cwd=None,
        exp_dim="auto",
        host_partition="auto",
        imp_dim="auto",
        measure=None,
        src_path=None,
        cdd=None,
        compressed="no",
        exp_concept_level="c",
        grid="-",
        imp_concept_level="c",
        import_metadata="no",
        check_compliance="no",
        offset=0,
        ioserver="mysql_table",
        ncores=1,
        nfrag=0,
        nhost=0,
        subset_dims="none",
        subset_filter="all",
        time_filter="yes",
        subset_type="index",
        exec_mode="sync",
        base_time="1900-01-01 00:00:00",
        calendar="standard",
        hierarchy="oph_base",
        leap_month=2,
        leap_year=0,
        month_lengths="31,28,31,30,31,30,31,31,30,31,30,31",
        run="yes",
        units="d",
        vocabulary="-",
        description="-",
        policy="rr",
        schedule=0,
        pid=None,
        check_grid="no",
        save="yes",
        display=False,
    ):
        """Cube(container='-', cwd=None, exp_dim='auto', host_partition='auto', imp_dim='auto', measure=None, src_path=None, cdd=None,
                compressed='no', exp_concept_level='c', grid='-', imp_concept_level='c', import_metadata='no', check_compliance='no',
                offset=0, ioserver='mysql_table', ncores=1, nfrag=0, nhost=0, subset_dims='none', subset_filter='all', time_filter='yes',
                subset_type='index', exec_mode='sync', base_time='1900-01-01 00:00:00', calendar='standard', hierarchy='oph_base',
                leap_month=2, leap_year=0, month_lengths='31,28,31,30,31,30,31,31,30,31,30,31', run='yes', units='d', vocabulary='-',
                description='-', policy='rr', schedule=0, pid=None, check_grid='no', save='yes', display=False) -> obj
             or Cube(pid=None) -> obj

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
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
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param compressed: yes|no
        :type compressed: str
        :param exp_concept_level: pipe (|) separated list of explicit dimensions hierarchy levels
        :type exp_concept_level: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param imp_concept_level: pipe (|) separated list of implicit dimensions hierarchy levels
        :type imp_concept_level: str
        :param import_metadata: yes|no
        :type import_metadata: str
        :param check_compliance: yes|no
        :type check_compliance: str
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param ioserver: mysql_table|ophdiaio_memory
        :type ioserver: str
        :param nfrag: number of fragments/db to use
        :type nfrag: int
        :param nhost: number of hosts to use
        :type nhost: int
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param base_time: reference time
        :type base_time: str
        :param calendar: calendar used (standard|gregorian|proleptic_gregorian|julian|360_day|no_leap|all_leap|user_defined)
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
        :param units: unit of time (s|m|h|3|6|d)
        :type units: str
        :param vocabulary: metadata vocabulary
        :type vocabulary: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param policy: rule to select how data are distribuited over hosts (rr|port)
        :type policy: str
        :param pid: PID of an existing cube (if used all other parameters are ignored)
        :type pid: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: obj or None
        :rtype: Cube or None
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
        self.fragxdb = None
        self.rowsxfrag = None
        self.elementsxrow = None
        self.compressed = None
        self.size = None
        self.nelements = None
        self.dim_info = None

        if pid is not None:
            if Cube.client is None:
                raise RuntimeError("Cube.client is None")
            self.pid = pid
        else:
            if (Cube.client is not None) and (cwd is not None or measure is not None or src_path is not None):
                if (cwd is None and Cube.client.cwd is None) or measure is None or src_path is None:
                    raise RuntimeError("one or more required parameters are None")

                else:
                    query = "oph_importnc "

                    if container is not None:
                        query += "container=" + str(container) + ";"
                    if cwd is not None:
                        query += "cwd=" + str(cwd) + ";"
                    if exp_dim is not None:
                        query += "exp_dim=" + str(exp_dim) + ";"
                    if host_partition is not None:
                        query += "host_partition=" + str(host_partition) + ";"
                    if imp_dim is not None:
                        query += "imp_dim=" + str(imp_dim) + ";"
                    if measure is not None:
                        query += "measure=" + str(measure) + ";"
                    if src_path is not None:
                        query += "src_path=" + str(src_path) + ";"
                    if cdd is not None:
                        query += "cdd=" + str(cdd) + ";"
                    if compressed is not None:
                        query += "compressed=" + str(compressed) + ";"
                    if exp_concept_level is not None:
                        query += "exp_concept_level=" + str(exp_concept_level) + ";"
                    if grid is not None:
                        query += "grid=" + str(grid) + ";"
                    if imp_concept_level is not None:
                        query += "imp_concept_level=" + str(imp_concept_level) + ";"
                    if import_metadata is not None:
                        query += "import_metadata=" + str(import_metadata) + ";"
                    if check_compliance is not None:
                        query += "check_compliance=" + str(check_compliance) + ";"
                    if ioserver is not None:
                        query += "ioserver=" + str(ioserver) + ";"
                    if ncores is not None:
                        query += "ncores=" + str(ncores) + ";"
                    if nfrag is not None:
                        query += "nfrag=" + str(nfrag) + ";"
                    if nhost is not None:
                        query += "nhost=" + str(nhost) + ";"
                    if subset_dims is not None:
                        query += "subset_dims=" + str(subset_dims) + ";"
                    if subset_filter is not None:
                        query += "subset_filter=" + str(subset_filter) + ";"
                    if time_filter is not None:
                        if subset_type == "index":
                            query += "time_filter=no;"
                        else:
                            query += "time_filter=" + str(time_filter) + ";"
                    if offset is not None:
                        query += "offset=" + str(offset) + ";"
                    if subset_type is not None:
                        query += "subset_type=" + str(subset_type) + ";"
                    if exec_mode is not None:
                        query += "exec_mode=" + str(exec_mode) + ";"
                    if base_time is not None:
                        query += "base_time=" + str(base_time) + ";"
                    if calendar is not None:
                        query += "calendar=" + str(calendar) + ";"
                    if hierarchy is not None:
                        query += "hierarchy=" + str(hierarchy) + ";"
                    if leap_month is not None:
                        query += "leap_month=" + str(leap_month) + ";"
                    if leap_year is not None:
                        query += "leap_year=" + str(leap_year) + ";"
                    if month_lengths is not None:
                        query += "month_lengths=" + str(month_lengths) + ";"
                    if run is not None:
                        query += "run=" + str(run) + ";"
                    if units is not None:
                        query += "units=" + str(units) + ";"
                    if vocabulary is not None:
                        query += "vocabulary=" + str(vocabulary) + ";"
                    if schedule is not None:
                        query += "schedule=" + str(schedule) + ";"
                    if policy is not None:
                        query += "policy=" + str(policy) + ";"
                    if description is not None:
                        query += "description=" + str(description) + ";"
                    if check_grid is not None:
                        query += "check_grid=" + str(check_grid) + ";"
                    if save is not None:
                        query += "save=" + str(save) + ";"

                    try:
                        if Cube.client.submit(query, display) is None:
                            raise RuntimeError()

                        if Cube.client.last_response is not None:
                            if Cube.client.cube:
                                self.pid = Cube.client.cube
                    except Exception as e:
                        print(_get_linenumber(), "Something went wrong in instantiating the cube", e)
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
        del self.fragxdb
        del self.rowsxfrag
        del self.elementsxrow
        del self.compressed
        del self.size
        del self.nelements
        del self.dim_info

    def info(self, display=True):
        """info(display=True) -> None : call OPH_CUBESIZE and OPH_CUBESCHEMA to fill all Cube attributes

        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client is None or pid is None")
        query = "oph_cubesize exec_mode=sync;cube=" + str(self.pid) + ";"
        if Cube.client.submit(query, display=False) is None:
            raise RuntimeError()
        query = "oph_cubeschema exec_mode=sync;cube=" + str(self.pid) + ";"
        if Cube.client.submit(query, display) is None:
            raise RuntimeError()
        res = Cube.client.deserialize_response()
        if res is not None:
            for res_i in res["response"]:
                if res_i["objkey"] == "cubeschema_cubeinfo":
                    self.pid = res_i["objcontent"][0]["rowvalues"][0][0]
                    self.creation_date = res_i["objcontent"][0]["rowvalues"][0][1]
                    self.measure = res_i["objcontent"][0]["rowvalues"][0][2]
                    self.measure_type = res_i["objcontent"][0]["rowvalues"][0][3]
                    self.level = res_i["objcontent"][0]["rowvalues"][0][4]
                    self.nfragments = res_i["objcontent"][0]["rowvalues"][0][5]
                    self.source_file = res_i["objcontent"][0]["rowvalues"][0][6]
                elif res_i["objkey"] == "cubeschema_morecubeinfo":
                    self.hostxcube = res_i["objcontent"][0]["rowvalues"][0][1]
                    self.fragxdb = res_i["objcontent"][0]["rowvalues"][0][2]
                    self.rowsxfrag = res_i["objcontent"][0]["rowvalues"][0][3]
                    self.elementsxrow = res_i["objcontent"][0]["rowvalues"][0][4]
                    self.compressed = res_i["objcontent"][0]["rowvalues"][0][5]
                    self.size = res_i["objcontent"][0]["rowvalues"][0][6] + " " + res_i["objcontent"][0]["rowvalues"][0][7]
                    self.nelements = res_i["objcontent"][0]["rowvalues"][0][8]
                elif res_i["objkey"] == "cubeschema_diminfo":
                    self.dim_info = list()
                    for row_i in res_i["objcontent"][0]["rowvalues"]:
                        element = dict()
                        element["name"] = row_i[0]
                        element["type"] = row_i[1]
                        element["size"] = row_i[2]
                        element["hierarchy"] = row_i[3]
                        element["concept_level"] = row_i[4]
                        element["array"] = row_i[5]
                        element["level"] = row_i[6]
                        element["lattice_name"] = row_i[7]
                        self.dim_info.append(element)

    def exportnc(
        self,
        misc="no",
        output_path="default",
        output_name="default",
        cdd=None,
        force="no",
        export_metadata="yes",
        schedule=0,
        exec_mode="sync",
        shuffle="no",
        deflate=0,
        ncores=1,
        save="yes",
        display=False,
    ):
        """exportnc(misc='no', output_path='default', output_name='default', cdd=None, force='no', export_metadata='yes', schedule=0, shuffle='no', deflate=0,
                    exec_mode='sync', ncores=1, save='yes', display=False) -> None : wrapper of the operator OPH_EXPORTNC

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param export_metadata: yes|no
        :type export_metadata: str
        :param misc: yes|no
        :type misc: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param force: yes|no
        :type force: str
        :param output_path: directory of the output file
        :type output_path: str
        :param output_name: name of the output file
        :type output_name: str
        :param shuffle: flag to activate shuffle filter on compression (yes|no)
        :type shuffle: str
        :param deflate: deflate level (from 1 to 9) compression. 0 is no compression
        :type deflate: int
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")

        query = "oph_exportnc "

        if misc is not None:
            query += "misc=" + str(misc) + ";"
        if output_path is not None:
            query += "output_path=" + str(output_path) + ";"
        if output_name is not None:
            query += "output_name=" + str(output_name) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if force is not None:
            query += "force=" + str(force) + ";"
        if export_metadata is not None:
            query += "export_metadata=" + str(export_metadata) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if shuffle is not None:
            query += "shuffle=" + str(shuffle) + ";"
        if deflate is not None:
            query += "deflate=" + str(deflate) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def exportnc2(
        self,
        misc="no",
        output_path="default",
        output_name="default",
        cdd=None,
        force="no",
        export_metadata="yes",
        schedule=0,
        shuffle="no",
        deflate=0,
        exec_mode="sync",
        ncores=1,
        save="yes",
        display=False,
    ):
        """exportnc2(misc='no', output_path='default', output_name='default', cdd=None, force='no', export_metadata='yes', schedule=0, shuffle='no', deflate=0,
                     exec_mode='sync', ncores=1, save='yes', display=False) -> None : wrapper of the operator OPH_EXPORTNC2

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param export_metadata: yes|no|postpone
        :type export_metadata: str
        :param misc: yes|no
        :type misc: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param force: yes|no
        :type force: str
        :param output_path: directory of the output file
        :type output_path: str
        :param output_name: name of the output file
        :type output_name: str
        :param shuffle: flag to activate shuffle filter on compression (yes|no)
        :type shuffle: str
        :param deflate: deflate level (from 1 to 9) compression. 0 is no compression
        :type deflate: int
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")

        query = "oph_exportnc2 "

        if misc is not None:
            query += "misc=" + str(misc) + ";"
        if output_path is not None:
            query += "output_path=" + str(output_path) + ";"
        if output_name is not None:
            query += "output_name=" + str(output_name) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if force is not None:
            query += "force=" + str(force) + ";"
        if export_metadata is not None:
            query += "export_metadata=" + str(export_metadata) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if shuffle is not None:
            query += "shuffle=" + str(shuffle) + ";"
        if deflate is not None:
            query += "deflate=" + str(deflate) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def aggregate(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        group_size="all",
        operation=None,
        missingvalue="-",
        grid="-",
        container="-",
        description="-",
        check_grid="no",
        save="yes",
        display=False,
    ):
        """aggregate( ncores=1, nthreads=1, exec_mode='sync', schedule=0, group_size='all', operation=None, missingvalue='-', grid='-', container='-', description='-', check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_AGGREGATE

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param operation: count|max|min|avg|sum
        :type operation: str
        :param container: optional container name
        :type container: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param group_size: number of tuples per group to consider in the aggregation function
        :type group_size: int or str
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or operation is None:
            raise RuntimeError("Cube.client, pid or operation is None")
        newcube = None

        query = "oph_aggregate "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if group_size is not None:
            query += "group_size=" + str(group_size) + ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def aggregate2(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        dim="-",
        concept_level="A",
        midnight="24",
        operation=None,
        grid="-",
        missingvalue="-",
        container="-",
        description="-",
        check_grid="no",
        save="yes",
        display=False,
    ):
        """aggregate2(ncores=1, nthreads=1, exec_mode='sync', schedule=0, dim='-', concept_level='A', midnight='24', operation=None, grid='-', missingvalue='-', container='-', description='-',
                      check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_AGGREGATE2

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param dim: name of dimension on which the operation will be applied
        :type dim: str
        :param operation: count|max|min|avg|sum
        :type operation: str
        :param concept_level: concept level inside the hierarchy used for the operation
        :type concept_level: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
        :type grid: str
        :param midnight: 00|24
        :type midnight: str
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or operation is None:
            raise RuntimeError("Cube.client, pid, dim or operation is None")
        newcube = None

        query = "oph_aggregate2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if concept_level is not None:
            query += "concept_level=" + str(concept_level) + ";"
        if midnight is not None:
            query += "midnight=" + str(midnight) + ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def apply(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        query="measure",
        dim_query="null",
        measure="null",
        measure_type="manual",
        dim_type="manual",
        check_type="yes",
        on_reduce="skip",
        compressed="auto",
        schedule=0,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """apply(ncores=1, nthreads=1, exec_mode='sync', query='measure', dim_query='null', measure='null', measure_type='manual', dim_type='manual', check_type='yes', on_reduce='skip', compressed='auto',
                 schedule=0, container='-', description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_APPLY

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param query: query to be submitted
        :type query: str
        :param check_type: yes|no
        :type check_type: str
        :param on_reduce: skip|update
        :type on_reduce: str
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
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client, pid or query is None")
        newcube = None

        internal_query = "oph_apply "

        if ncores is not None:
            internal_query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            internal_query += "exec_mode=" + str(exec_mode) + ";"
        if query is not None:
            internal_query += "query=" + str(query) + ";"
        if dim_query is not None:
            internal_query += "dim_query=" + str(dim_query) + ";"
        if measure is not None:
            internal_query += "measure=" + str(measure) + ";"
        if measure_type is not None:
            internal_query += "measure_type=" + str(measure_type) + ";"
        if dim_type is not None:
            internal_query += "dim_type=" + str(dim_type) + ";"
        if check_type is not None:
            internal_query += "check_type=" + str(check_type) + ";"
        if on_reduce is not None:
            internal_query += "on_reduce=" + str(on_reduce) + ";"
        if compressed is not None:
            internal_query += "compressed=" + str(compressed) + ";"
        if schedule is not None:
            internal_query += "schedule=" + str(schedule) + ";"
        if container is not None:
            internal_query += "container=" + str(container) + ";"
        if description is not None:
            internal_query += "description=" + str(description) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        internal_query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(internal_query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def concatnc(
        self,
        src_path=None,
        cdd=None,
        grid="-",
        check_exp_dim="yes",
        dim_offset="-",
        dim_continue="no",
        offset=0,
        description="-",
        subset_dims="none",
        subset_filter="all",
        subset_type="index",
        time_filter="yes",
        ncores=1,
        exec_mode="sync",
        schedule=0,
        save="yes",
        display=False,
    ):
        """concatnc(src_path=None, cdd=None, grid='-', check_exp_dim='yes', dim_offset='-', dim_continue='no', offset=0, description='-', subset_dims='none',
        subset_filter='all', subset_type='index', time_filter='yes', ncores=1, exec_mode='sync', schedule=0, save='yes', display=False)
        -> Cube or None : wrapper of the operator OPH_CONCATNC

        :param src_path: path of file to be imported
        :type src_path: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param check_exp_dim: yes|no
        :type check_exp_dim: str
        :param dim_offset: offset to be added to dimension values of imported data
        :type dim_offset: float
        :param dim_continue: yes|no
        :type dim_continue: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or src_path is None:
            raise RuntimeError("Cube.client, pid or src_path is None")
        newcube = None

        query = "oph_concatnc "

        if src_path is not None:
            query += "src_path=" + str(src_path) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_exp_dim is not None:
            query += "check_exp_dim=" + str(check_exp_dim) + ";"
        if dim_offset is not None:
            query += "dim_offset=" + str(dim_offset) + ";"
        if dim_continue is not None:
            query += "dim_continue=" + str(dim_continue) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def concatnc2(
        self,
        src_path=None,
        cdd=None,
        grid="-",
        check_exp_dim="yes",
        dim_offset="-",
        dim_continue="no",
        offset=0,
        description="-",
        subset_dims="none",
        subset_filter="all",
        subset_type="index",
        time_filter="yes",
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        save="yes",
        display=False,
    ):
        """concatnc(src_path=None, cdd=None, grid='-', check_exp_dim='yes', dim_offset='-', dim_continue='no', offset=0, description='-', subset_dims='none',
        subset_filter='all', subset_type='index', time_filter='yes', ncores=1, nthreads=1, exec_mode='sync', schedule=0, save='yes', display=False)
        -> Cube or None : wrapper of the operator OPH_CONCATNC2

        :param src_path: path of file to be imported
        :type src_path: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param grid: optionally group dimensions in a grid
        :type grid: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: index|coord
        :type subset_type: str
        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param offset: it is added to the bounds of subset intervals
        :type offset: int
        :param check_exp_dim: yes|no
        :type check_exp_dim: str
        :param dim_offset: offset to be added to dimension values of imported data
        :type dim_offset: float
        :param dim_continue: yes|no
        :type dim_continue: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or src_path is None:
            raise RuntimeError("Cube.client, pid or src_path is None")
        newcube = None

        query = "oph_concatnc2 "

        if src_path is not None:
            query += "src_path=" + str(src_path) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_exp_dim is not None:
            query += "check_exp_dim=" + str(check_exp_dim) + ";"
        if dim_offset is not None:
            query += "dim_offset=" + str(dim_offset) + ";"
        if dim_continue is not None:
            query += "dim_continue=" + str(dim_continue) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def provenance(
        self,
        branch="all",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """provenance(branch='all', exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_CUBEIO

        :param branch: parent|children|all
        :type branch: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_cubeio "

        if branch is not None:
            query += "branch=" + str(branch) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def delete(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        save="yes",
        display=False,
    ):
        """delete(ncores=1, nthreads=1, exec_mode='sync', schedule=0, save='yes', display=False) -> None : wrapper of the operator OPH_DELETE

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")

        query = "oph_delete "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def drilldown(
        self,
        ncores=1,
        exec_mode="sync",
        schedule=0,
        ndim=1,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """drilldown(ndim=1, container='-', ncores=1, exec_mode='sync', schedule=0, description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_DRILLDOWN

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param ndim: number of implicit dimensions that will be transformed in explicit dimensions
        :type ndim: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        newcube = None

        query = "oph_drilldown "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if ndim is not None:
            query += "ndim=" + str(ndim) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def duplicate(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """duplicate(container='-', ncores=1, nthreads=1, exec_mode='sync', description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_DUPLICATE

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        newcube = None

        query = "oph_duplicate "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def explore(
        self,
        schedule=0,
        limit_filter=100,
        subset_dims=None,
        subset_filter="all",
        time_filter="yes",
        subset_type="index",
        show_index="no",
        show_id="no",
        show_time="no",
        level=1,
        output_path="default",
        output_name="default",
        cdd=None,
        base64="no",
        ncores=1,
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """explore(schedule=0, limit_filter=100, subset_dims=None, subset_filter='all', time_filter='yes', subset_type='index', show_index='no', show_id='no', show_time='no', level=1, output_path='default',
                   output_name='default', cdd=None, base64='no', ncores=1, exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_EXPLORECUBE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param level: 1|2
        :type level: int
        :param limit_filter: max number of rows
        :type limit_filter: int
        :param output_path: absolute path of the JSON Response
        :type output_path: str
        :param output_name: filename of the JSON Response
        :type output_name: str
        :param time_filter: yes|no
        :type time_filter: str
        :param subset_type: if subset is applied on dimension values or indexes
        :type subset_type: str
        :param show_id: yes|no
        :type show_id: str
        :param show_index: yes|no
        :type show_index: str
        :param show_time: yes|no
        :type show_time: str
        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param base64: yes|no
        :type base64: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_explorecube "

        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if limit_filter is not None:
            query += "limit_filter=" + str(limit_filter) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if show_index is not None:
            query += "show_index=" + str(show_index) + ";"
        if show_id is not None:
            query += "show_id=" + str(show_id) + ";"
        if show_time is not None:
            query += "show_time=" + str(show_time) + ";"
        if level is not None:
            query += "level=" + str(level) + ";"
        if output_path is not None:
            query += "output_path=" + str(output_path) + ";"
        if output_name is not None:
            query += "output_name=" + str(output_name) + ";"
        if cdd is not None:
            query += "cdd=" + str(cdd) + ";"
        if base64 is not None:
            query += "base64=" + str(base64) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def publish(
        self,
        content="all",
        schedule=0,
        show_index="no",
        show_id="no",
        show_time="no",
        ncores=1,
        exec_mode="sync",
        save="yes",
        display=True,
    ):
        """publish( ncores=1, content='all', exec_mode='sync', show_id= 'no', show_index='no', schedule=0, show_time='no', save='yes', display=True) -> dict or None : wrapper of the operator OPH_PUBLISH

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param show_index: yes|no
        :type show_index: str
        :param show_id: yes|no
        :type show_id: str
        :param show_time: yes|no
        :type show_time: str
        :param content: all|data|metadata
        :type content: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_publish "

        if content is not None:
            query += "content=" + str(content) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if show_index is not None:
            query += "show_index=" + str(show_index) + ";"
        if show_id is not None:
            query += "show_id=" + str(show_id) + ";"
        if show_time is not None:
            query += "show_time=" + str(show_time) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def unpublish(self, exec_mode="sync", save="yes", display=False):
        """unpublish( exec_mode='sync', save='yes', display=False) -> None : wrapper of the operator OPH_UNPUBLISH

        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")

        query = "oph_unpublish ncores=1;"

        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"
        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def cubeschema(
        self,
        level=0,
        dim="all",
        show_index="no",
        show_time="no",
        base64="no",
        action="read",
        concept_level="c",
        dim_level=1,
        dim_array="yes",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """cubeschema( objkey_filter='all', exec_mode='sync', level=0, dim=None, show_index='no', show_time='no', base64='no', action='read', concept_level='c', dim_level=1, dim_array='yes', save='yes', display=True) -> dict or None : wrapper of the operator OPH_CUBESCHEMA

        :param level: 0|1|2
        :type level: int
        :param dim: names of dimensions to show. Only valid with level bigger than 0
        :type dim: str
        :param show_index: yes|no
        :type show_index: str
        :param show_time: yes|no
        :type show_time: str
        :param base64: yes|no
        :type base64: str
        :param action: read|add|clear
        :type action: str
        :param concept_level: hierarchy level of a new dimension to be added (default is 'c')
        :type concept_level: str
        :param dim_level: level of a new dimension to be added, greater than 0 (default is 1)
        :type dim_level: int
        :param dim_array: yes|no
        :type dim_array: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_cubeschema ncores=1;"

        if level is not None:
            query += "level=" + str(level) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if show_index is not None:
            query += "show_index=" + str(show_index) + ";"
        if show_time is not None:
            query += "show_time=" + str(show_time) + ";"
        if base64 is not None:
            query += "base64=" + str(base64) + ";"
        if action is not None:
            query += "action=" + str(action) + ";"
        if concept_level is not None:
            query += "concept_level=" + str(concept_level) + ";"
        if dim_level is not None:
            query += "dim_level=" + str(dim_level) + ";"
        if dim_array is not None:
            query += "dim_array=" + str(dim_array) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def cubesize(
        self,
        schedule=0,
        exec_mode="sync",
        byte_unit="MB",
        algorithm="euristic",
        ncores=1,
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """cubesize( schedule=0, ncores=1, byte_unit='MB', algorithm='euristic', objkey_filter='all', exec_mode='sync', save='yes', display=True) -> dict or None : wrapper of the operator OPH_CUBESIZE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param byte_unit: KB|MB|GB|TB|PB
        :type byte_unit: str
        :param algorithm: euristic|count
        :type algorithm: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_cubesize "

        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if byte_unit is not None:
            query += "byte_unit=" + str(byte_unit) + ";"
        if algorithm is not None:
            algorithm += "algorithm=" + str(algorithm) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def cubeelements(
        self,
        schedule=0,
        exec_mode="sync",
        algorithm="dim_product",
        ncores=1,
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """cubeelements( schedule=0, algorithm='dim_product', ncores=1, exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_CUBEELEMENTS

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param algorithm: dim_product|count
        :type algorithm: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_cubeelements "

        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if algorithm is not None:
            query += "algorithm=" + str(algorithm) + ";"
        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def intercube(
        self,
        ncores=1,
        exec_mode="sync",
        cube2=None,
        cubes=None,
        operation="sub",
        missingvalue="-",
        measure="null",
        schedule=0,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """intercube(ncores=1, exec_mode='sync', cube2=None, cubes=None, operation='sub', missingvalue='-', measure='null', schedule=0, container='-', description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_INTERCUBE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param cube2: PID of the second cube
        :type cube2: str
        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param operation: sum|sub|mul|div|abs|arg|corr|mask|max|min|arg_max|arg_min
        :type operation: str
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param measure: new measure name
        :type measure: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or ((self.pid is None or cube2 is None) and cubes is None):
            raise RuntimeError("Cube.client, pid, cube2 or cubes is None")
        newcube = None

        query = "oph_intercube "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if cubes is not None:
            query += "cubes=" + str(cubes) + ";"
        else:
            if self.pid is not None:
                query += "cube=" + str(self.pid) + ";"
            if cube2 is not None:
                query += "cube2=" + str(cube2) + ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    @classmethod
    def intercube2(
        cls,
        ncores=1,
        exec_mode="sync",
        cubes=None,
        operation="avg",
        missingvalue="-",
        measure="null",
        schedule=0,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """intercube2(ncores=1, exec_mode='sync', cubes=None, operation='avg', missingvalue='-', measure='null', schedule=0, container='-', description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_INTERCUBE2

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param cubes: pipe (|) separated list of cubes
        :type cubes: str
        :param operation: sum|avg|mul|max|min|arg_max|arg_min
        :type operation: str
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param measure: new measure name
        :type measure: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or cubes is None:
            raise RuntimeError("Cube.client or cubes is None")
        newcube = None

        query = "oph_intercube2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if cubes is not None:
            query += "cubes="
            for i in range(0, len(cubes)):
                query += str(cubes[i].pid)
                if i < len(cubes) - 1:
                    query += "|"
            query += ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if measure is not None:
            query += "measure=" + str(measure) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def merge(
        self,
        ncores=1,
        exec_mode="sync",
        schedule=0,
        nmerge=0,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """merge(nmerge=0, schedule=0, description='-', container='-', exec_mode='sync', ncores=1, save='yes', display=False) -> Cube or None : wrapper of the operator OPH_MERGE

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param nmerge: number of input fragments to merge in an output fragment, 0 for all
        :type nmerge: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        newcube = None

        query = "oph_merge "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nmerge is not None:
            query += "nmerge=" + str(nmerge) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def metadata(
        self,
        mode="read",
        metadata_key="all",
        variable="global",
        metadata_id=0,
        metadata_type="text",
        metadata_value="-",
        variable_filter="all",
        metadata_type_filter="all",
        metadata_value_filter="all",
        force="no",
        exec_mode="sync",
        objkey_filter="all",
        save="yes",
        display=True,
    ):
        """metadata(mode='read', metadata_id=0, metadata_key='all', variable='global', metadata_type='text', metadata_value=None, variable_filter=None, metadata_type_filter=None,
                    metadata_value_filter=None, force='no', exec_mode='sync', objkey_filter='all', save='yes', display=True) -> dict or None : wrapper of the operator OPH_METADATA

        :param mode: insert|read|update|delete
        :type mode: str
        :param metadata_id: id of the particular metadata instance to interact with
        :type metadata_id: int
        :param metadata_key: name of the key (or the enumeration of keys) identifying requested metadata
        :type metadata_key: str
        :param variable: name of the variable to which we can associate a new metadata key
        :type variable: str
        :param metadata_type: text|image|video|audio|url|double|float|long|int|short
        :type metadata_type: str
        :param metadata_value: string value to be assigned to specified metadata
        :type metadata_value: str
        :param variable_filter: filter on variable name
        :type variable_filter: str
        :param metadata_type_filter: filter on metadata type
        :type metadata_type_filter: str
        :param metadata_value_filter: filter on metadata value
        :type metadata_value_filter: str
        :param force: force update or deletion of functional metadata associated to a vocabulary, default is no
        :type force: str
        :param exec_mode: async or sync
        :type exec_mode: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is True)
        :type display: bool
        :returns: response or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_metadata "

        if mode is not None:
            query += "mode=" + str(mode) + ";"
        if metadata_key is not None:
            query += "metadata_key=" + str(metadata_key) + ";"
        if variable is not None:
            query += "variable=" + str(variable) + ";"
        if metadata_id is not None:
            query += "metadata_id=" + str(metadata_id) + ";"
        if metadata_type is not None:
            query += "metadata_type=" + str(metadata_type) + ";"
        if metadata_value is not None:
            query += "metadata_value=" + str(metadata_value) + ";"
        if variable_filter is not None:
            query += "variable_filter=" + str(variable_filter) + ";"
        if metadata_type_filter is not None:
            query += "metadata_type_filter=" + str(metadata_type_filter) + ";"
        if metadata_value_filter is not None:
            query += "metadata_value_filter=" + str(metadata_value_filter) + ";"
        if force is not None:
            query += "force=" + str(force) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if objkey_filter is not None:
            query += "objkey_filter=" + str(objkey_filter) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None and display is False:
                response = Cube.client.deserialize_response()["response"]
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return response

    def permute(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        dim_pos=None,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """permute(dim_pos=None, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_PERMUTE

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param dim_pos: permutation of implicit dimensions as a comma-separated list of dimension levels
        :type dim_pos: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or dim_pos is None:
            raise RuntimeError("Cube.client, pid or dim_pos is None")
        newcube = None

        query = "oph_permute "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if dim_pos is not None:
            query += "dim_pos=" + str(dim_pos) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def reduce(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        group_size="all",
        operation=None,
        order=2,
        missingvalue="-",
        grid="-",
        container="-",
        description="-",
        check_grid="no",
        save="yes",
        display=False,
    ):
        """reduce(operation=None, container=None, exec_mode='sync', missingvalue='-', grid='-', group_size='all', ncores=1, nthreads=1, schedule=0, order=2, description='-', objkey_filter='all', check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_REDUCE

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param operation: count|max|min|avg|sum|std|var|cmoment|acmoment|rmoment|armoment|quantile|arg_max|arg_min
        :type operation: str
        :param order: order used in evaluation the moments or value of the quantile in range [0, 1]
        :type order: float
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
        :type grid: str
        :param group_size: size of the aggregation set, all for the entire array
        :type group_size: int or str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or operation is None:
            raise RuntimeError("Cube.client, pid or operation is None")
        newcube = None

        query = "oph_reduce "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if group_size is not None:
            query += "group_size=" + str(group_size) + ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if order is not None:
            query += "order=" + str(order) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def reduce2(
        self,
        ncores=1,
        exec_mode="sync",
        schedule=0,
        dim=None,
        concept_level="A",
        midnight="24",
        operation=None,
        order=2,
        missingvalue="-",
        grid="-",
        container="-",
        description="-",
        nthreads=1,
        check_grid="no",
        save="yes",
        display=False,
    ):
        """reduce2(dim=None, operation=None, concept_level='A', container='-', exec_mode='sync', grid='-', midnight='24', order=2, missingvalue="-", description='-', schedule=0, ncores=1, nthreads=1, check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_REDUCE2

        :param ncores: number of cores to use
        :type ncores: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param dim: name of dimension on which the operation will be applied
        :type dim: str
        :param operation: count|max|min|avg|sum|std|var|cmoment|acmoment|rmoment|armoment|quantile|arg_max|arg_min
        :type operation: str
        :param concept_level: concept level inside the hierarchy used for the operation
        :type concept_level: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
        :type grid: str
        :param midnight: 00|24
        :type midnight: str
        :param order: order used in evaluation the moments or value of the quantile in range [0, 1]
        :type order: float
        :param missingvalue: missing value; by default it is the value from the file if defined, NAN otherwise (for float and double)
        :type missingvalue: float
        :param description: additional description to be associated with the output cube
        :type description: str
        :param nthreads: number of threads to use
        :type nthreads: int
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or dim is None or operation is None:
            raise RuntimeError("Cube.client, pid, dim or operation is None")
        newcube = None

        query = "oph_reduce2 "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if dim is not None:
            query += "dim=" + str(dim) + ";"
        if concept_level is not None:
            query += "concept_level=" + str(concept_level) + ";"
        if midnight is not None:
            query += "midnight=" + str(midnight) + ";"
        if operation is not None:
            query += "operation=" + str(operation) + ";"
        if order is not None:
            query += "order=" + str(order) + ";"
        if missingvalue is not None:
            query += "missingvalue=" + str(missingvalue) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def rollup(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        ndim=1,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """rollup(ndim=1, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_ROLLUP

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param ndim: number of explicit dimensions that will be transformed in implicit dimensions
        :type ndim: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        newcube = None

        query = "oph_rollup "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if ndim is not None:
            query += "ndim=" + str(ndim) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def split(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        nsplit=2,
        container="-",
        description="-",
        save="yes",
        display=False,
    ):
        """split(nsplit=2, container='-', exec_mode='sync', ncores=1, nthreads=1, schedule=0, description='-', save='yes', display=False) -> Cube or None : wrapper of the operator OPH_SPLIT

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param nsplit: number of output fragments per input fragment
        :type nsplit: int
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None or nsplit is None:
            raise RuntimeError("Cube.client, pid or nsplit is None")
        newcube = None

        query = "oph_split "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if nsplit is not None:
            query += "nsplit=" + str(nsplit) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def subset(
        self,
        ncores=1,
        nthreads=1,
        exec_mode="sync",
        schedule=0,
        subset_dims="none",
        subset_filter="all",
        subset_type="index",
        time_filter="yes",
        offset=0,
        grid="-",
        container="-",
        description="-",
        check_grid="no",
        save="yes",
        display=False,
    ):
        """subset(subset_dims='none', subset_filter='all', container='-', exec_mode='sync', subset_type='index', time_filter='yes', offset=0, grid='-', ncores=1, nthreads=1, schedule=0, description='-',
                  check_grid='no', save='yes', display=False)
             -> Cube or None : wrapper of the operator OPH_SUBSET

        :param ncores: number of cores to use
        :type ncores: int
        :param nthreads: number of threads to use
        :type nthreads: int
        :param exec_mode: async or sync
        :type exec_mode: str
        :param schedule: 0
        :type schedule: int
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters on dimension indexes (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param container: name of the container to be used to store the output cube, by default it is the input container
        :type container: str
        :param subset_type: index|coord
        :type subset_type: str
        :param time_filter: yes|no
        :type time_filter: str
        :param offset: added to the bounds of subset intervals
        :type offset: int
        :param grid: optional argument used to identify the grid of dimensions to be used or the one to be created
        :type grid: str
        :param description: additional description to be associated with the output cube
        :type description: str
        :param check_grid: yes|no
        :type check_grid: str
        :param save: option to enable/disable JSON response saving on the server-side (default is yes)
        :type save: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: new cube or None
        :rtype: Cube or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client pid is None")
        newcube = None

        query = "oph_subset "

        if ncores is not None:
            query += "ncores=" + str(ncores) + ";"
        if exec_mode is not None:
            query += "exec_mode=" + str(exec_mode) + ";"
        if schedule is not None:
            query += "schedule=" + str(schedule) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"
        if subset_type is not None:
            query += "subset_type=" + str(subset_type) + ";"
        if time_filter is not None:
            if subset_type == "index":
                query += "time_filter=no;"
            else:
                query += "time_filter=" + str(time_filter) + ";"
        if offset is not None:
            query += "offset=" + str(offset) + ";"
        if grid is not None:
            query += "grid=" + str(grid) + ";"
        if container is not None:
            query += "container=" + str(container) + ";"
        if description is not None:
            query += "description=" + str(description) + ";"
        if check_grid is not None:
            query += "check_grid=" + str(check_grid) + ";"
        if nthreads is not None:
            query += "nthreads=" + str(nthreads) + ";"
        if save is not None:
            query += "save=" + str(save) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                if Cube.client.cube:
                    newcube = Cube(pid=Cube.client.cube)
        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        else:
            return newcube

    def to_b2drop(
        self,
        cdd=None,
        auth_path="-",
        dst_path="-",
        ncores=1,
        export_metadata="yes",
    ):
        """to_b2drop(cdd=None, auth_path='-', dst_path='-', ncores=1, export_metadata='yes')
          -> None : method that integrates the features of OPH_EXPORTNC2 and OPH_B2DROP operators to upload a cube to B2DROP as a NetCDF file

        :param cdd: absolute path corresponding to the current directory on data repository
        :type cdd: str
        :param auth_path: absolute path to the netrc file containing the B2DROP credentials
        :type auth_path: str
        :param dst_path: path where the file will be uploaded on B2DROP
        :type dst_path: str
        :param ncores: number of cores to use
        :type ncores: int
        :param export_metadata: yes|no
        :type export_metadata: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        try:
            self.exportnc2(
                cdd=cdd,
                force="yes",
                output_path="local",
                export_metadata=export_metadata,
                ncores=ncores,
                display=False,
            )

            file_path = ""
            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

                for response_i in response["response"]:
                    if response_i["objclass"] == "text" and "title" in response_i["objcontent"][0] and response_i["objcontent"][0]["title"] == "Output File":
                        file_path = response_i["objcontent"][0]["message"]
                        break

            if not file_path:
                raise RuntimeError("Unable to export NetCDF file")

            Cube.b2drop(
                action="put",
                auth_path=auth_path,
                src_path=file_path,
                dst_path=dst_path,
                cdd="/",
                display=False,
            )

            Cube.fs(command="rm", dpath=file_path, cdd="/", display=False)

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

    def export_array(
        self,
        show_id="no",
        show_time="no",
        subset_dims=None,
        subset_filter=None,
        time_filter="no",
    ):
        """export_array(show_id='no', show_time='no', subset_dims=None, subset_filter=None, time_filter='no') -> dict or None : return data from an Ophidia datacube into a Python structure

        :param show_id: yes|no
        :type show_id: str
        :param show_time: yes|no
        :type show_time: str
        :param subset_dims: pipe (|) separated list of dimensions on which to apply the subsetting
        :type subset_dims: str
        :param subset_filter: pipe (|) separated list of filters, one per dimension, composed of comma-separated microfilters (e.g. 1,5,10:2:50)
        :type subset_filter: str
        :param time_filter: yes|no
        :type time_filter: str
        :returns: data_values or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        query = "oph_explorecube ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord;limit_filter=0;save=no;"

        if time_filter is not None:
            query += "time_filter=" + str(time_filter) + ";"
        if show_id is not None:
            query += "show_id=" + str(show_id) + ";"
        if show_time is not None:
            query += "show_time=" + str(show_time) + ";"
        if subset_dims is not None:
            query += "subset_dims=" + str(subset_dims) + ";"
        if subset_filter is not None:
            query += "subset_filter=" + str(subset_filter) + ";"

        query += "cube=" + str(self.pid) + ";"

        try:
            if Cube.client.submit(query, display=False) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

        data_values = {}
        data_values["measure"] = {}

        # Get dimensions
        adimCube = True
        try:
            dimensions = []
            for response_i in response["response"]:
                if response_i["objkey"] == "explorecube_dimvalues":
                    data_values["dimension"] = {}
                    adimCube = False

                    for response_j in response_i["objcontent"]:
                        if response_j["title"] and response_j["rowfieldtypes"] and response_j["rowfieldtypes"][1] and response_j["rowvalues"]:
                            curr_dim = {}
                            curr_dim["name"] = response_j["title"]

                            # Append actual values
                            dim_array = []

                            # Special case for time
                            if show_time == "yes" and response_j["title"] == "time":
                                for val in response_j["rowvalues"]:
                                    dims = [s.strip() for s in val[1].split(",")]
                                    for v in dims:
                                        dim_array.append(v)
                            else:
                                for val in response_j["rowvalues"]:
                                    decoded_bin = base64.b64decode(val[1] + "==")
                                    length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][1])
                                    format = _get_unpack_format(length, response_j["rowfieldtypes"][1])
                                    dims = struct.unpack(format, decoded_bin)
                                    for v in dims:
                                        dim_array.append(v)

                            curr_dim["values"] = dim_array
                            dimensions.append(curr_dim)

                        else:
                            raise RuntimeError("Unable to get dimension name or values in response")

                    dim_num = len(dimensions)
                    if dim_num == 0:
                        raise RuntimeError("No dimension found")

                    data_values["dimension"] = dimensions
                    break

        except Exception as e:
            print(_get_linenumber(), "Unable to get dimensions from response:", e)
            return None

        # Read values
        try:
            measures = []
            for response_i in response["response"]:
                if response_i["objkey"] == "explorecube_data":

                    for response_j in response_i["objcontent"]:
                        if response_j["title"] and response_j["rowkeys"] and response_j["rowfieldtypes"] and response_j["rowvalues"]:
                            curr_mes = {}
                            measure_name = ""
                            measure_index = 0

                            if not adimCube:
                                # Check that implicit dimension is just one
                                if dim_num - (len(response_j["rowkeys"]) - 1) / 2.0 > 1:
                                    raise RuntimeError("More than one implicit dimension")

                            for i, t in enumerate(response_j["rowkeys"]):
                                if response_j["title"] == t:
                                    measure_name = t
                                    measure_index = i
                                    break

                            if measure_index == 0:
                                raise RuntimeError("Unable to get measure name in response")

                            curr_mes["name"] = measure_name

                            # Append actual values
                            measure_value = []
                            for val in response_j["rowvalues"]:
                                decoded_bin = base64.b64decode(val[measure_index] + "==")
                                length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][measure_index])
                                format = _get_unpack_format(length, response_j["rowfieldtypes"][measure_index])
                                measure = struct.unpack(format, decoded_bin)
                                curr_line = []
                                for v in measure:
                                    curr_line.append(v)

                                measure_value.append(curr_line)

                            curr_mes["values"] = measure_value
                            measures.append(curr_mes)

                        else:
                            raise RuntimeError("Unable to get measure values in response")

                        break

                    break

            measure_num = len(measures)
            if measure_num == 0:
                raise RuntimeError("No measure found")

            data_values["measure"] = measures

        except Exception as e:
            print(_get_linenumber(), "Unable to get measure from response:", e)
            return None
        else:
            return data_values

    def to_dataset(self):
        """to_dataset() -> xarray.core.dataset.Dataset or None : return data from an Ophidia datacube into a Xarray dataset

        :returns: a 'xarray.core.dataset.Dataset' object or None
        :rtype: <class 'xarray.core.dataset.Dataset'>
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        def _append_with_format(var, frmt):
            """_append_with_format(var, frmt) ->
            numpy.float32|numpy.float64|numpy.int32|numpy.int64 converts a variable
                to the appropriate format according to pyophidia's type
            :param var: the variable to convert
            :type var: int|float|str
            :param frmt: a string representing pyophidias format
            :type frmt: str
            :rtype: <class 'numpy.float32'>|<class 'numpy.float64'>|<class
            'numpy.int32'>|<class 'numpy.int64'>
            """
            _dependency_check("numpy")
            import numpy as np

            if frmt == "float":
                return np.float32(var)
            elif frmt == "double":
                return np.float64(var)
            elif frmt == "int":
                return np.int32(var)
            elif frmt == "long":
                return np.int64(var)
            else:
                return var

        def _scientific_notation(num):
            """_scientific_notation(v) -> str : converts a large number to
            scientific notation
            :params num: our number (int or float)
            :type num: <class 'int'> | <class 'float'>
            :returns: str
            :rtype: <class 'str'>
            """
            from decimal import Decimal

            d = Decimal(eval(str(num)))
            e = "{:.6e}".format(d)
            a = e.split("e")
            b = a[0].replace("0", "")
            return b + "e" + a[1]

        def _add_coordinates(cube, ds, response, meta_info):
            """
            _add_coordinates(cube, dr, response) -> xarray.core.dataset.Dataset,
            int: a function that uses the response from
                the oph_explorecube and adds coordinates to the dataarray object
            :param cube: the cube object
            :type cube:  <class 'PyOphidia.cube.Cube'>
            :param ds: the xarray dataset object
            :type ds:  <class 'xarray.core.dataset.Dataset'>
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: xarray.core.dataset.Dataset,int|None
            :rtype: <class 'xarray.core.dataset.Dataset'>,<class 'int'>|None
            """
            lengths = []
            try:
                for response_i in response["response"]:
                    if "objcontent" in response_i.keys() and "objkey" in response_i.keys():
                        if response_i["objkey"] == "explorecube_dimvalues":
                            for response_j in response_i["objcontent"]:
                                if response_j["title"] and response_j["rowfieldtypes"] and response_j["rowfieldtypes"][1] and response_j["rowvalues"]:
                                    if response_j["title"] == _time_dimension_finder(cube):
                                        temp_array = []
                                        lengths.append(len(response_j["rowvalues"]))
                                        for val in response_j["rowvalues"]:
                                            dims = [s.strip() for s in val[1].split(",")]
                                            temp_array.append(dims[0])
                                        ds[response_j["title"]] = temp_array
                                        ds[response_j["title"]].attrs = _convert_to_metadict(meta_info, filter=response_j["title"])
                                    else:
                                        lengths.append(len(response_j["rowvalues"]))
                                        temp_array = []
                                        for val in response_j["rowvalues"]:
                                            decoded_bin = base64.b64decode(val[1] + "==")
                                            length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][1])
                                            format = _get_unpack_format(length, response_j["rowfieldtypes"][1])
                                            dims = struct.unpack(format, decoded_bin)
                                            temp_array.append(_append_with_format(dims[0], response_j["rowfieldtypes"][1]))
                                        ds[response_j["title"]] = list(temp_array)
                                        ds[response_j["title"]].attrs = _convert_to_metadict(meta_info, filter=response_j["title"])
                                else:
                                    raise RuntimeError("Unable to get dimension name or values in " "response")
                            break
            except Exception as e:
                print(_get_linenumber(), "Unable to get dimensions from response:", e)
                return None
            return ds, lengths

        def _add_measure(cube, ds, response, lengths, meta_info):
            """
            _add_measure(cube, dr, response) -> xarray.core.dataset.Dataset: a
            function that uses the response from
                the oph_explorecube and adds the measure to the dataarray object
            :param cube: the cube object
            :type cube:  <class 'PyOphidia.cube.Cube'>
            :param ds: the xarray dataset object
            :type ds:  <class 'xarray.core.dataset.Dataset'>
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :param lengths: list of the coordinate lengths
            :type lengths:  <class 'list'>
            :returns: xarray.core.dataset.Dataset|None
            :rtype: <class 'xarray.core.dataset.Dataset'>|None
            """
            try:
                for response_i in response["response"]:
                    if "objcontent" in response_i.keys() and "objkey" in response_i.keys():
                        if response_i["objkey"] == "explorecube_data":
                            for response_j in response_i["objcontent"]:
                                if response_j["title"] and response_j["rowkeys"] and response_j["rowfieldtypes"] and response_j["rowvalues"]:
                                    measure_index = 0

                                    for i, t in enumerate(response_j["rowkeys"]):
                                        if response_j["title"] == t:
                                            measure_index = i
                                            break
                                    if measure_index == 0:
                                        raise RuntimeError("Unable to get measure name in response")
                                    values = []
                                    for val in response_j["rowvalues"]:
                                        decoded_bin = base64.b64decode(val[measure_index] + "==")
                                        length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][measure_index])
                                        format = _get_unpack_format(length, response_j["rowfieldtypes"][measure_index])
                                        data_format = response_j["rowfieldtypes"][measure_index]
                                        measure = struct.unpack(format, decoded_bin)
                                        if (type(measure)) is (tuple or list) and len(measure) == 1:
                                            values.append(_append_with_format(measure[0], data_format))
                                        else:
                                            for v in measure:
                                                values.append(_append_with_format(v, data_format))
                                    previous_array = []
                                    for i in range(len(lengths) - 1, -1, -1):
                                        current_array = []
                                        if i == len(lengths) - 1:
                                            for j in range(0, len(values), lengths[i]):
                                                current_array.append(values[j : j + lengths[i]])
                                        else:
                                            for j in range(0, len(previous_array), lengths[i]):
                                                current_array.append(previous_array[j : j + lengths[i]])
                                        previous_array = current_array
                                    measure = previous_array[0]
                                else:
                                    raise RuntimeError("Unable to get measure values in response")
                                break
                            break
                if len(measure) == 0:
                    raise RuntimeError("No measure found")
            except Exception as e:
                print("Unable to get measure from response:", e)
                return None

            sorted_coordinates = []
            for ln in lengths:
                for c in cube.dim_info:
                    if ln == int(c["size"]) and c["name"] not in sorted_coordinates:
                        sorted_coordinates.append(c["name"])
                        break
            ds[cube.measure] = (
                sorted_coordinates,
                measure,
            )
            ds[cube.measure].attrs = _convert_to_metadict(meta_info, filter=response_j["title"])
            ds[cube.measure].data = _convert_missing_value(meta_info, response_j["title"], cube.measure_type, ds[cube.measure].data)
            return ds

        def _get_meta_info(response):
            """
            _get_meta_info(response) -> <class 'list'>: a function that uses the
            response from
                the oph_explorecube and returns metadata information
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: list
            :rtype: <class 'list'>|None
            """
            try:
                meta_list = []
                for obj in response["response"]:
                    if "objcontent" in obj.keys() and "objkey" in obj.keys():
                        if obj["objkey"] == "explorecube_metadata":
                            if ("rowvalues" and "rowkeys") in obj["objcontent"][0].keys():
                                key_indx, value_indx, variable_indx, type_indx = _get_meta_indexes(obj["objcontent"][0]["rowkeys"])
                                for row in obj["objcontent"][0]["rowvalues"]:
                                    key = row[key_indx]
                                    value = row[value_indx]
                                    variable = row[variable_indx]
                                    _type = row[type_indx]
                                    if (_type == "float" or _type == "int") and len(str(value)) > 9:
                                        value = _scientific_notation(value)
                                    meta_list.append({"key": key, "value": value, "variable": variable})
            except Exception as e:
                print("Unable to parse meta info from response:", e)
                return None
            return meta_list

        def _convert_to_metadict(meta_list, filter=""):
            meta_dict = {}
            for d in meta_list:
                if d["variable"] == filter:
                    meta_dict[d["key"]] = d["value"]
            return meta_dict

        def _convert_missing_value(meta_info, measure_name, measure_type, data):
            if measure_type.lower() != "int" and measure_type.lower() != "long":
                try:
                    _dependency_check("numpy")
                    import numpy as np

                    meta = _convert_to_metadict(meta_info, filter=measure_name)
                    missing_val = None
                    for m in meta:
                        if m == "_FillValue" or m == "missing_value":
                            missing_val = meta[m]
                    if missing_val:
                        data[data == float(missing_val)] = np.nan
                except Exception as e:
                    print("Unable to convert missing values:", e)
                    return None
            return data

        def _initiate_xarray_object(cube, meta_info):
            """
            _initiate_xarray_object(cube, meta_info) ->
            xarray.core.dataset.Dataset: a function that initiates the
                xarray.dataset object with the meta information
            :param cube: the cube object
            :type cube:  <class 'PyOphidia.cube.Cube'>
            :param meta_info: meta information dict
            :type meta_info:  <class 'list'>
            :returns: xarray.core.dataset.Dataset|None
            :rtype: <class 'xarray.core.dataset.Dataset'>|None
            """
            coordinates = [c["name"] for c in cube.dim_info]
            if len(coordinates) == 0:
                raise RuntimeError("No coordinates")
            ds = xr.Dataset({cube.measure: ""}, attrs=_convert_to_metadict(meta_info, filter=""))
            return ds

        def _get_meta_indexes(rowkeys):
            """
            _get_meta_indexes(response) -> <class 'int'>, <class 'int'>, <class
            'int'>, <class 'int'>: a function that takes as
                input a list of strings and returns the indexes of the ones that
                match Key and Value
            :param rowkeys: list of strings
            :type rowkeys:  <class 'list'>
            :returns: int, int, int, int
            :rtype: <class 'int'>, <class 'int'>|None
            """
            try:
                return rowkeys.index("Key"), rowkeys.index("Value"), rowkeys.index("Variable"), rowkeys.index("Type")
            except Exception as e:
                print("Unable to parse meta info from response:", e)
                return None

        def _get_dim_indexes(rowkeys):
            """
            _get_dim_indexes(response) -> <class 'int'>, <class 'int'>, <class
            'int'>, <class 'int'>: a function that takes as
                input a list of strings and returns the indexes of the ones that
                match Key and Value
            :param rowkeys: list of strings
            :type rowkeys:  <class 'list'>
            :returns: int, int, int
            :rtype: <class 'int'>, <class 'int'>|None
            """
            try:
                return (
                    rowkeys.index("NAME"),
                    rowkeys.index("TYPE"),
                    rowkeys.index("SIZE"),
                    rowkeys.index("HIERARCHY"),
                    rowkeys.index("CONCEPT LEVEL"),
                    rowkeys.index("ARRAY"),
                    rowkeys.index("LEVEL"),
                    rowkeys.index("LATTICE NAME"),
                )
            except Exception as e:
                print("Unable to parse dim info from response:", e)
                return None

        def _set_measure_info(self, response):
            """
            _get_measure_info(response) -> <class 'list'>: a function that uses the
            response from
                the oph_explorecube and fills cube measure information
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: list
            :rtype: <bool>
            """
            try:
                for obj in response["response"]:
                    if "objcontent" in obj.keys() and "objkey" in obj.keys():
                        if obj["objkey"] == "explorecube_data":
                            if "title" in obj["objcontent"][0].keys():
                                self.measure = obj["objcontent"][0]["title"]
                            if ("rowfieldtypes" and "rowkeys") in obj["objcontent"][0].keys():
                                measure_indx = obj["objcontent"][0]["rowkeys"].index(self.measure)
                                self.measure_type = obj["objcontent"][0]["rowfieldtypes"][measure_indx]
            except Exception as e:
                print("Unable to parse measure info from response:", e)
                return False
            return True

        def _set_dim_info(self, response):
            """
            _get_dim_info(response) -> <class 'list'>: a function that uses the
            response from
                the oph_explorecube and fills cube dim information
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: list
            :rtype: <bool>
            """
            try:
                dim_info = list()
                for obj in response["response"]:
                    if "objcontent" in obj.keys() and "objkey" in obj.keys():
                        if obj["objkey"] == "explorecube_diminfo":
                            if ("rowvalues" and "rowkeys") in obj["objcontent"][0].keys():
                                name_indx, type_indx, size_indx, hier_indx, clev_indx, array_indx, level_indx, lattice_indx = _get_dim_indexes(obj["objcontent"][0]["rowkeys"])
                                for row in obj["objcontent"][0]["rowvalues"]:
                                    element = dict()
                                    element["name"] = row[name_indx]
                                    element["type"] = row[type_indx]
                                    element["size"] = row[size_indx]
                                    element["hierarchy"] = row[hier_indx]
                                    element["concept_level"] = row[clev_indx]
                                    element["array"] = row[array_indx]
                                    element["level"] = row[level_indx]
                                    element["lattice_name"] = row[lattice_indx]
                                    dim_info.append(element)
            except Exception as e:
                print("Unable to parse dim info from response:", e)
                return False
            self.dim_info = dim_info
            return True

        _dependency_check(dependency="xarray")
        import xarray as xr

        query = "oph_explorecube " "ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord;limit_filter=0;show_time=yes;export_metadata=yes;cube={0};".format(self.pid)
        try:
            if Cube.client.submit(query, display=False) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

        try:
            _set_measure_info(self, response)
            _set_dim_info(self, response)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the cube info, error: ", e)
            return None
        try:
            meta_list = _get_meta_info(response)
            ds = _initiate_xarray_object(self, meta_list)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the metadata, error: ", e)
            return None
        try:
            ds, lengths = _add_coordinates(self, ds, response, meta_list)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the coordinates, error: ", e)
            return None
        try:
            ds = _add_measure(self, ds, response, lengths, meta_list)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the measure, error: ", e)
            return None
        return ds

    def to_dataframe(self):
        """to_dataframe() -> pandas.core.frame.DataFrame or None : return data from an Ophidia datacube into a Pandas dataframe

        :returns: a pandas.core.frame.DataFrame object or None
        :rtype: <class 'pandas.core.frame.DataFrame'>
        :raises: RuntimeError
        """

        if Cube.client is None or self.pid is None:
            raise RuntimeError("Cube.client or pid is None")
        response = None

        def _add_coordinates(cube, response):
            """
            _add_coordinates(cube,response) ->
            pandas.core.indexes.multi.MultiIndex: a function that uses the response
                from the oph_explorecube and converts dimensions to pandas
                multiIndex format
            :param cube: the cube object
            :type cube:  <class 'PyOphidia.cube.Cube'>
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: pandas.core.indexes.multi.MultiIndex|None
            :rtype: <class 'pandas.core.indexes.multi.MultiIndex'>|None
            """
            indexes = {}
            try:
                for response_i in response["response"]:
                    if response_i["objkey"] == "explorecube_dimvalues":
                        for response_j in response_i["objcontent"]:
                            if response_j["title"] and response_j["rowfieldtypes"] and response_j["rowfieldtypes"][1] and response_j["rowvalues"]:
                                if response_j["title"] == _time_dimension_finder(cube):
                                    temp_array = []
                                    for val in response_j["rowvalues"]:
                                        dims = [s.strip() for s in val[1].split(",")]
                                        temp_array.append(dims[0])
                                    indexes[response_j["title"]] = temp_array
                                else:
                                    temp_array = []
                                    for val in response_j["rowvalues"]:
                                        decoded_bin = base64.b64decode(val[1] + "==")
                                        length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][1])
                                        format = _get_unpack_format(length, response_j["rowfieldtypes"][1])
                                        dims = struct.unpack(format, decoded_bin)
                                        temp_array.append(dims[0])
                                    indexes[response_j["title"]] = list(temp_array)
                            else:
                                raise RuntimeError("Unable to get dimension name or values in " "response")
                        break
            except Exception as e:
                print(_get_linenumber(), "Unable to get dimensions from response:", e)
                return None
            return pd.MultiIndex.from_product(list(indexes.values()), names=list(indexes.keys()))

        def _add_measure(cube, indexes, response):
            """
            _add_measure(cube, indexes, response) ->
            pandas.core.frame.DataFrame: a function that uses the response from
                the oph_explorecube and creates the pandas.Dataframe
            :param cube: the cube object
            :type cube:  <class 'PyOphidia.cube.Cube'>
            :param indexes: indexes in pandas multiindex format
            :type indexes: <class 'pandas.core.indexes.multi.MultiIndex'>
            :param response: response from pyophidia query
            :type response:  <class 'dict'>
            :returns: pandas.core.frame.DataFrame|None
            :rtype: <class 'pandas.core.frame.DataFrame'>|None
            """
            try:
                for response_i in response["response"]:
                    if response_i["objkey"] == "explorecube_data":
                        for response_j in response_i["objcontent"]:
                            if response_j["title"] and response_j["rowkeys"] and response_j["rowfieldtypes"] and response_j["rowvalues"]:
                                measure_index = 0
                                for i, t in enumerate(response_j["rowkeys"]):
                                    if response_j["title"] == t:
                                        measure_index = i
                                        break
                                if measure_index == 0:
                                    raise RuntimeError("Unable to get measure name in response")
                                values = []
                                for val in response_j["rowvalues"]:
                                    decoded_bin = base64.b64decode(val[measure_index] + "==")
                                    length = _calculate_decoded_length(decoded_bin, response_j["rowfieldtypes"][measure_index])
                                    format = _get_unpack_format(length, response_j["rowfieldtypes"][measure_index])
                                    measure = struct.unpack(format, decoded_bin)
                                    if (type(measure)) is (tuple or list) and len(measure) == 1:
                                        values.append(measure[0])
                                    else:
                                        for v in measure:
                                            values.append(v)
                            else:
                                raise RuntimeError("Unable to get measure values in response")
                            break
                        break
                if len(measure) == 0:
                    raise RuntimeError("No measure found")
            except Exception as e:
                print("Unable to get measure from response:", e)
                return None
            df = pd.DataFrame({cube.measure: values}, index=indexes)
            return df

        _dependency_check(dependency="pandas")
        import pandas as pd

        query = "oph_explorecube " "ncore=1;base64=yes;level=2;show_time=yes;show_index=yes" ";subset_type=coord;" "limit_filter=0;cube={0};".format(self.pid)

        try:
            if Cube.client.submit(query, display=False) is None:
                raise RuntimeError()

            if Cube.client.last_response is not None:
                response = Cube.client.deserialize_response()

        except Exception as e:
            print(_get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()

        try:
            indexes = _add_coordinates(self, response)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the coordinates, error: ", e)
            return None
        try:
            df = _add_measure(self, indexes, response)
        except Exception as e:
            print(_get_linenumber(), "Something is wrong with the measure, error: ", e)
            return None
        return df

    def __str__(self):
        buf = "-" * 30 + "\n"
        buf += "%30s: %s" % ("Cube", self.pid) + "\n"
        buf += "-" * 30 + "\n"
        buf += "%30s: %s" % ("Creation Date", self.creation_date) + "\n"
        buf += "%30s: %s (%s)" % ("Measure (type)", self.measure, self.measure_type) + "\n"
        buf += "%30s: %s" % ("Source file", self.source_file) + "\n"
        buf += "%30s: %s" % ("Level", self.level) + "\n"
        if self.compressed == "yes":
            buf += "%30s: %s (%s)" % ("Size", self.size, "compressed") + "\n"
        else:
            buf += "%30s: %s (%s)" % ("Size", self.size, "not compressed") + "\n"
        buf += "%30s: %s" % ("Num. of elements", self.nelements) + "\n"
        buf += "%30s: %s" % ("Num. of fragments", self.nfragments) + "\n"
        buf += "-" * 30 + "\n"
        buf += "%30s: %s" % ("Num. of hosts", self.hostxcube) + "\n"
        buf += (
            "%30s: %s (%s)"
            % (
                "Num. of fragments/DB (total)",
                self.fragxdb,
                int(self.fragxdb) * int(self.hostxcube),
            )
            + "\n"
        )
        buf += (
            "%30s: %s (%s)"
            % (
                "Num. of rows/fragment (total)",
                self.rowsxfrag,
                int(self.rowsxfrag) * int(self.fragxdb) * int(self.hostxcube),
            )
            + "\n"
        )
        buf += (
            "%30s: %s (%s)"
            % (
                "Num. of elements/row (total)",
                self.elementsxrow,
                int(self.elementsxrow) * int(self.rowsxfrag) * int(self.fragxdb) * int(self.hostxcube),
            )
            + "\n"
        )
        buf += "-" * 127 + "\n"
        buf += (
            "%15s %15s %15s %15s %15s %15s %15s %15s"
            % (
                "Dimension",
                "Data type",
                "Size",
                "Hierarchy",
                "Concept level",
                "Array",
                "Level",
                "Lattice name",
            )
            + "\n"
        )
        buf += "-" * 127 + "\n"
        for dim in self.dim_info:
            buf += (
                "%15s %15s %15s %15s %15s %15s %15s %15s"
                % (
                    dim["name"],
                    dim["type"],
                    dim["size"],
                    dim["hierarchy"],
                    dim["concept_level"],
                    dim["array"],
                    dim["level"],
                    dim["lattice_name"],
                )
                + "\n"
            )
        buf += "-" * 127 + "\n"
        return buf
