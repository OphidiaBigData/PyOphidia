#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2020 CMCC Foundation
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import os
import struct
import sys
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


def _dependency_check(dependency):
    """_dependency_check() -> checks for xarray dependency in user's system
    :param dependency: the dependency to be checked. pandas or xarray
    :type cube:  str
    :returns: NoneType
    :rtype: <class 'NoneType'>
    :raises: RuntimeError, AttributeError
    """
    if dependency == "pandas":
        try:
            import pandas
        except ModuleNotFoundError:
            raise RuntimeError('pandas is not installed')
    elif dependency == "xarray":
        try:
            import xarray
        except ModuleNotFoundError:
            raise RuntimeError('xarray is not installed')
    else:
        raise AttributeError("Dependency variable must be xarray or pandas")


def _time_dimension_finder(cube):
    """
    _time_dimension_finder(cube) -> str: finds the time dimension, if any
    :param cube: the cube object
    :type cube:  <class 'PyOphidia.cube.Cube'>
    :returns: str|None
    :rtype: <class 'str'>
    """
    for c in cube.dim_info:
        if c["hierarchy"].lower() == "oph_time":
            return c["name"]
    return None


def _get_unpack_format(element_num, output_type):
    if output_type == 'float':
        format = str(element_num) + 'f'
    elif output_type == 'double':
        format = str(element_num) + 'd'
    elif output_type == 'int':
        format = str(element_num) + 'i'
    elif output_type == 'long':
        format = str(element_num) + 'l'
    elif output_type == 'short':
        format = str(element_num) + 'h'
    elif output_type == 'char':
        format = str(element_num) + 'c'
    else:
        raise RuntimeError('The value type is not valid')
    return format


def _calculate_decoded_length(decoded_string, output_type):
    if output_type == 'float' or output_type == 'int':
        num = int(float(len(decoded_string)) / float(4))
    elif output_type == 'double' or output_type == 'long':
        num = int(float(len(decoded_string)) / float(8))
    elif output_type == 'short':
        num = int(float(len(decoded_string)) / float(2))
    elif output_type == 'char':
        num = int(float(len(decoded_string)) / float(1))
    else:
        raise RuntimeError('The value type is not valid')
    return num


def convert_to_xarray(cube):
    """convert_to_xarray(cube=cube) -> xarray.core.dataset.Dataset : convert
    a Pyophidia.cube to xarray.dataset
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :returns: a 'xarray.core.dataset.Dataset' object
    :rtype: <class 'Pxarray.core.dataset.Dataset'>
    :raises: RuntimeError
    """

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
        import numpy
        if frmt == "float":
            return numpy.float32(var)
        elif frmt == "double":
            return numpy.float64(var)
        elif frmt == "int":
            return numpy.int32(var)
        elif frmt == "long":
            return numpy.int64(var)
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
        e = format(d, '.6e')
        a = e.split('e')
        b = a[0].replace('0', '')
        return b + 'e' + a[1]

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
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_dimvalues':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j[
                            'rowfieldtypes'] and response_j['rowfieldtypes'][
                            1] and \
                                response_j['rowvalues']:
                            if response_j['title'] == _time_dimension_finder(
                                    cube):
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    dims = [s.strip() for s in
                                            val[1].split(',')]
                                    temp_array.append(dims[0])
                                ds[response_j['title']] = temp_array
                            else:
                                lengths.append(len(response_j['rowvalues']))
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    decoded_bin = base64.b64decode(val[1])
                                    length = _calculate_decoded_length(
                                        decoded_bin,
                                        response_j['rowfieldtypes'][1])
                                    format = _get_unpack_format(length,
                                                                response_j[
                                                                    'rowfieldtypes'][
                                                                    1])
                                    dims = struct.unpack(format, decoded_bin)
                                    temp_array.append(
                                        _append_with_format(dims[0],
                                                            response_j[
                                                                'rowfieldtypes'][
                                                                1]))
                                ds[response_j['title']] = list(temp_array)
                                ds[response_j[
                                    'title']].attrs = convert_to_metadict(
                                    meta_info,
                                    filter=response_j['title'])
                        else:
                            raise RuntimeError(
                                "Unable to get dimension name or values in "
                                "response")
                    break
        except Exception as e:
            print(get_linenumber(), "Unable to get dimensions from response:",
                  e)
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
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_data':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowkeys'] and \
                                response_j['rowfieldtypes'] \
                                and response_j['rowvalues']:
                            measure_index = 0

                            for i, t in enumerate(response_j['rowkeys']):
                                if response_j['title'] == t:
                                    measure_index = i
                                    break
                            if measure_index == 0:
                                raise RuntimeError(
                                    "Unable to get measure name in response")
                            values = []
                            for val in response_j['rowvalues']:
                                decoded_bin = base64.b64decode(
                                    val[measure_index])
                                length = _calculate_decoded_length(decoded_bin,
                                                                   response_j[
                                                                       'rowfieldtypes'][
                                                                       measure_index])
                                format = _get_unpack_format(length, response_j[
                                    'rowfieldtypes'][measure_index])
                                data_format = response_j['rowfieldtypes'][
                                    measure_index]
                                measure = struct.unpack(format, decoded_bin)
                                if (type(measure)) is (tuple or list) and len(
                                        measure) == 1:
                                    values.append(
                                        _append_with_format(measure[0],
                                                            data_format))
                                else:
                                    for v in measure:
                                        values.append(_append_with_format(v,
                                                                          data_format))
                            for i in range(len(lengths) - 1, -1, -1):
                                current_array = []
                                if i == len(lengths) - 1:
                                    for j in range(0, len(values), lengths[i]):
                                        current_array.append(
                                            values[j:j + lengths[i]])
                                else:
                                    for j in range(0, len(previous_array),
                                                   lengths[i]):
                                        current_array.append(
                                            previous_array[j:j + lengths[i]])
                                previous_array = current_array
                            measure = previous_array[0]
                        else:
                            raise RuntimeError(
                                "Unable to get measure values in response")
                        break
                    break
            if len(measure) == 0:
                raise RuntimeError("No measure found")
        except Exception as e:
            print("Unable to get measure from response:", e)
            return None
        sorted_coordinates = []
        for l in lengths:
            for c in cube.dim_info:
                if l == int(c["size"]) and c["name"] not in sorted_coordinates:
                    sorted_coordinates.append(c["name"])
                    break
        ds[cube.measure] = (sorted_coordinates, measure,)
        ds[cube.measure].attrs = convert_to_metadict(meta_info,
                                                     filter=response_j[
                                                         'title'])
        return ds

    def _get_meta_info(response):
        """
        _get_meta_info(response) -> <class 'list'>: a function that uses the
        response from
            the oph_metadata and returns meta information
        :param response: response from pyophidia query
        :type response:  <class 'dict'>
        :returns: list
        :rtype: <class 'list'>|None
        """
        try:
            meta_list = []
            for obj in response["response"]:
                if "objcontent" in obj.keys():
                    if ("rowvalues" and "rowkeys") in obj["objcontent"][
                        0].keys():
                        key_indx, value_indx, variable_indx, type_indx = \
                            _get_indexes(
                                obj["objcontent"][0]["rowkeys"])
                        for row in obj["objcontent"][0]["rowvalues"]:
                            key = row[key_indx]
                            value = row[value_indx]
                            variable = row[variable_indx]
                            _type = row[type_indx]
                            if (_type == "float" or _type == "int") and len(
                                    str(value)) > 9:
                                value = _scientific_notation(value)
                            meta_list.append({"key": key, "value": value,
                                              "variable": variable})
        except Exception as e:
            print("Unable to parse meta info from response:", e)
            return None
        return meta_list

    def convert_to_metadict(meta_list, filter=""):
        meta_dict = {}
        for d in meta_list:
            if d["variable"] == filter:
                meta_dict[d["key"]] = d["value"]
        return meta_dict

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
        ds = xr.Dataset({cube.measure: ""},
                        attrs=convert_to_metadict(meta_info, filter=""))
        return ds

    def _get_indexes(rowkeys):
        """
        _get_indexes(response) -> <class 'int'>, <class 'int'>, <class
        'int'>, <class 'int'>: a function that takes as
            input a list of strings and returns the indexes of the ones that
            match Key and Value
        :param rowkeys: list of strings
        :type rowkeys:  <class 'list'>
        :returns: int, int, int, int
        :rtype: <class 'int'>, <class 'int'>|None
        """
        try:
            return rowkeys.index("Key"), rowkeys.index("Value"), rowkeys.index(
                "Variable"), rowkeys.index("Type")
        except Exception as e:
            print("Unable to parse meta info from response:", e)
            return None

    _dependency_check(dependency="xarray")
    import xarray as xr
    cube.info(display=False)
    pid = cube.pid
    query = 'oph_metadata cube={0}'.format(pid)
    cube.client.submit(query, display=False)
    meta_response = cube.client.deserialize_response()
    meta_list = _get_meta_info(meta_response)
    ds = _initiate_xarray_object(cube, meta_list)
    query = 'oph_explorecube ' \
            'ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord' \
            ';limit_filter=0;show_time=yes;' \
            'cube={0};'.format(pid)
    cube.client.submit(query, display=False)
    response = cube.client.deserialize_response()
    try:
        ds, lengths = _add_coordinates(cube, ds, response, meta_list)
    except Exception as e:
        print(get_linenumber(),
              "Something is wrong with the coordinates, error: ", e)
        return None
    try:
        ds = _add_measure(cube, ds, response, lengths, meta_list)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the measure, error: ",
              e)
        return None
    return ds


def convert_to_dataframe(cube):
    """convert_to_pandas(cube=cube) -> pandas.core.frame.DataFrame : convert
    a Pyophidia.cube to pandas dataframe
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :returns: a pandas.core.frame.DataFrame object
    :rtype: <class 'pandas.core.frame.DataFrame'>
    :raises: RuntimeError
    """

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
        max_size = max([int(dim["size"]) for dim in cube.dim_info])
        indexes = {}
        try:
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_dimvalues':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j[
                            'rowfieldtypes'] and response_j['rowfieldtypes'][
                            1] and \
                                response_j['rowvalues']:
                            if response_j['title'] == _time_dimension_finder(
                                    cube):
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    dims = [s.strip() for s in
                                            val[1].split(',')]
                                    temp_array.append(dims[0])
                                indexes[response_j['title']] = temp_array
                            else:
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    decoded_bin = base64.b64decode(val[1])
                                    length = _calculate_decoded_length(
                                        decoded_bin,
                                        response_j['rowfieldtypes'][1])
                                    format = _get_unpack_format(length,
                                                                response_j[
                                                                    'rowfieldtypes'][
                                                                    1])
                                    dims = struct.unpack(format, decoded_bin)
                                    temp_array.append(dims[0])
                                indexes[response_j['title']] = list(temp_array)
                        else:
                            raise RuntimeError(
                                "Unable to get dimension name or values in "
                                "response")
                    break
        except Exception as e:
            print(get_linenumber(), "Unable to get dimensions from response:",
                  e)
            return None
        return pd.MultiIndex.from_product(list(indexes.values()),
                                          names=list(indexes.keys()))

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
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_data':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowkeys'] and \
                                response_j['rowfieldtypes'] \
                                and response_j['rowvalues']:
                            measure_index = 0
                            for i, t in enumerate(response_j['rowkeys']):
                                if response_j['title'] == t:
                                    measure_index = i
                                    break
                            if measure_index == 0:
                                raise RuntimeError(
                                    "Unable to get measure name in response")
                            values = []
                            for val in response_j['rowvalues']:
                                decoded_bin = base64.b64decode(
                                    val[measure_index])
                                length = _calculate_decoded_length(decoded_bin,
                                                                   response_j[
                                                                       'rowfieldtypes'][
                                                                       measure_index])
                                format = _get_unpack_format(length, response_j[
                                    'rowfieldtypes'][measure_index])
                                measure = struct.unpack(format, decoded_bin)
                                if (type(measure)) is (tuple or list) and len(
                                        measure) == 1:
                                    values.append(measure[0])
                                else:
                                    for v in measure:
                                        values.append(v)
                        else:
                            raise RuntimeError(
                                "Unable to get measure values in response")
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
    cube.info(display=False)
    pid = cube.pid
    query = 'oph_explorecube ' \
            'ncore=1;base64=yes;level=2;show_time=yes;show_index=yes' \
            ';subset_type=coord;' \
            'limit_filter=0;cube={0};'.format(pid)
    cube.client.submit(query, display=False)
    response = cube.client.deserialize_response()
    try:
        indexes = _add_coordinates(cube, response)
    except Exception as e:
        print(get_linenumber(),
              "Something is wrong with the coordinates, error: ", e)
        return None
    try:
        df = _add_measure(cube, indexes, response)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the measure, error: ",
              e)
        return None
    return df
