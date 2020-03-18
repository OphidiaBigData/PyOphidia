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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
import base64
import struct
import PyOphidia.cube as cube
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


def convert_to_xarray(cube):
    """convert_to_xarray(cube=cube) -> xarray.core.dataset.Dataset : convert a Pyophidia.cube to xarray.dataset
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :returns: a 'xarray.core.dataset.Dataset' object
    :rtype: <class 'Pxarray.core.dataset.Dataset'>
    :raises: RuntimeError
    """

    def _time_dimension_finder(cube):
        """
        _time_dimension_finder(cube) -> str: finds the time dimension, if any
        :param cube: the cube object
        :type cube:  <class 'PyOphidia.cube.Cube'>
        :returns: str|None
        :rtype: <class 'str'>
        """
        for c in cube.dim_info:
            if c["type"].lower() == "oph_time":
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

    def _add_coordinates(cube, ds, response, meta_info):
        """
        _add_coordinates(cube, dr, response) -> xarray.core.dataset.Dataset,int: a function that uses the response from
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
                        if response_j['title'] and response_j['rowfieldtypes'] and response_j['rowfieldtypes'][1] and \
                                response_j['rowvalues']:
                            if response_j['title'] == _time_dimension_finder(cube):
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    dims = [s.strip() for s in val[1].split(',')]
                                    temp_array.append(dims[0])
                                ds[response_j['title']] = temp_array
                            else:
                                lengths.append(len(response_j['rowvalues']))
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    decoded_bin = base64.b64decode(val[1])
                                    length = _calculate_decoded_length(decoded_bin, response_j['rowfieldtypes'][1])
                                    format = _get_unpack_format(length, response_j['rowfieldtypes'][1])
                                    dims = struct.unpack(format, decoded_bin)
                                    temp_array.append(dims[0])
                                ds[response_j['title']] = list(temp_array)
                                ds[response_j['title']].attrs = convert_to_metadict(meta_info,
                                                                                    filter=response_j['title'])
                        else:
                            raise RuntimeError("Unable to get dimension name or values in response")
                    break
        except Exception as e:
            print(get_linenumber(), "Unable to get dimensions from response:", e)
            return None
        return ds, lengths

    def _add_measure(cube, ds, response, lengths, meta_info):
        """
        _add_measure(cube, dr, response) -> xarray.core.dataset.Dataset: a function that uses the response from
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
                        if response_j['title'] and response_j['rowkeys'] and response_j['rowfieldtypes'] \
                                and response_j['rowvalues']:
                            measure_index = 0
                            for i, t in enumerate(response_j['rowkeys']):
                                if response_j['title'] == t:
                                    measure_index = i
                                    break
                            if measure_index == 0:
                                raise RuntimeError("Unable to get measure name in response")
                            values = []
                            for val in response_j['rowvalues']:
                                decoded_bin = base64.b64decode(val[measure_index])
                                length = _calculate_decoded_length(decoded_bin,
                                                                   response_j['rowfieldtypes'][measure_index])
                                format = _get_unpack_format(length, response_j['rowfieldtypes'][measure_index])
                                measure = struct.unpack(format, decoded_bin)
                                if (type(measure)) is (tuple or list) and len(measure) == 1:
                                    values.append(measure[0])
                                else:
                                    for v in measure:
                                        values.append(v)
                            for i in range(len(lengths) - 1, -1, -1):
                                current_array = []
                                if i == len(lengths) - 1:
                                    for j in range(0, len(values), lengths[i]):
                                        current_array.append(values[j:j + lengths[i]])
                                else:
                                    for j in range(0, len(previous_array), lengths[i]):
                                        current_array.append(previous_array[j:j + lengths[i]])
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
        for l in lengths:
            for c in cube.dim_info:
                if l == int(c["size"]) and c["name"] not in sorted_coordinates:
                    sorted_coordinates.append(c["name"])
                    break
        ds[cube.measure] = (sorted_coordinates, measure,)
        ds[cube.measure].attrs = convert_to_metadict(meta_info, filter=response_j['title'])
        return ds

    def _get_meta_info(response):
        """
        _get_meta_info(response) -> <class 'list'>: a function that uses the response from
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
                    if ("rowvalues" and "rowkeys") in obj["objcontent"][0].keys():
                        key_indx, value_indx, variable_indx = _get_indexes(obj["objcontent"][0]["rowkeys"])
                        for row in obj["objcontent"][0]["rowvalues"]:
                            key = row[key_indx]
                            value = row[value_indx]
                            variable = row[variable_indx]
                            meta_list.append({"key": key, "value": value, "variable": variable})
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
        _initiate_xarray_object(cube, meta_info) -> xarray.core.dataset.Dataset: a function that initiates the
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
        ds = xr.Dataset({cube.measure: ""}, attrs=convert_to_metadict(meta_info, filter=""))
        return ds

    def _get_indexes(rowkeys):
        """
        _get_indexes(response) -> <class 'int'>, <class 'int'>: a function that takes as input a list of strings and
            returns the indexes of the ones that match Key and Value
        :param rowkeys: list of strings
        :type rowkeys:  <class 'list'>
        :returns: int,int
        :rtype: <class 'int'>, <class 'int'>|None
        """
        try:
            return rowkeys.index("Key"), rowkeys.index("Value"), rowkeys.index("Variable")
        except Exception as e:
            print("Unable to parse meta info from response:", e)
            return None

    import xarray as xr
    cube.info(display=False)
    pid = cube.pid
    query = 'oph_metadata cube={0}'.format(pid)
    cube.client.submit(query, display=False)
    meta_response = cube.client.deserialize_response()
    meta_list = _get_meta_info(meta_response)
    ds = _initiate_xarray_object(cube, meta_list)
    query = 'oph_explorecube ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord;limit_filter=0;cube={0};'. \
        format(pid)
    cube.client.submit(query, display=False)
    response = cube.client.deserialize_response()
    try:
        ds, lengths = _add_coordinates(cube, ds, response, meta_list)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the coordinates, error: ", e)
        return None
    try:
        ds = _add_measure(cube, ds, response, lengths, meta_list)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the measure, error: ", e)
        return None
    return ds

