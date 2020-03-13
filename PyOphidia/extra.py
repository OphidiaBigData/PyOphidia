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


def where(cube=cube, expression="x", if_true=1, if_false=0, ncores=1, nthreads=1,
          description='-', display=False):
    """where(cube=cube, expression="x", if_true=1, if_false=0, ncores=1, nthreads=1,
          description='-') -> Pyophidia.cube : Get a cube object after having run the predicate query
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :param expression: a mathematical equation which represents a way to validate the results we want
    :type expression: str
    :param if_true: the  return value if the expression is true (default is 1)
    :type if_true: str or int or bool
    :param if_false: the  return value if the expression is false (default is 0)
    :type if_false: str or int or bool
    :param ncores: the number of cores that we should use to perform the operation (default is 1)
    :type ncores: int
    :param nthreads: the number of threads that we should use to perform the operation (default is 1)
    :type nthreads: int
    :param description: additional description to be associated with the output container
    :type description: str
    :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
    :type display: bool
    :returns: a 'PyOphidia.cube.Cube' object
    :rtype: <class 'PyOphidia.cube.Cube'>
    :raises: RuntimeError
    """

    def _get_output_type(if_true, input_type):
        """_get_output_type(if_true, input_type) -> str : Get the ophidia output type of the where function
        :param if_true: the type of the positive result
        :type if_true: int or str or bytes
        :param input_type: the Pyophidia input type
        :type input_type: str
        :returns: a string that represents the Pyophidia type of the output variable
        :rtype: str
        :raises: RuntimeError
        """
        if isinstance(if_true, int):
            if input_type == "oph_long":
                return "oph_long"
            return "oph_int"
        elif isinstance(if_true, float):
            if input_type == "oph_double":
                return "oph_double"
            return "oph_float"
        elif isinstance(if_true, bytes):
            return "oph_bytes"
        else:
            raise RuntimeError('given if_true is wrong')

    def _get_input_type(input_type):
        """_get_input_type(input_type) -> str : Get the ophidia input type of the where function
        :param input_type: the Pyophidia input type
        :type input_type: str
        :returns: a string that represents the Pyophidia type of the input variable
        :rtype: str
        :raises: RuntimeError
        """
        if input_type == "float":
            return "oph_float"
        elif input_type == "int":
            return "oph_int"
        elif input_type == "bytes":
            return "oph_bytes"
        elif input_type == "double":
            return "oph_double"
        else:
            raise RuntimeError('given input_type is wrong')

    def _split_expression(expression):
        """_split_expression(expression) -> str, str, str split the expression to 3 parts so it will be easier to use on
        predicate
        :param expression: a string that represents an equation that will be used for the predicate method
        :type expression: str
        :return: measure
        :rtype: str
        :return: measure
        :rtype: str
        :return: final_left_part
        :rtype: str
        :return: right_part
        :rtype: str
        :raises: RuntimeError
        """
        cube.info(display=False)
        comparison_operators = ["=", ">", "!=", ">=", "<", "<="]
        arithmetical_operators = ["+", "-", "/", "*", "**", "%", "//"]
        for comparison_operator in comparison_operators:
            if comparison_operator in expression:
                current_comparison_operator = comparison_operator
                left_part = expression.split(comparison_operator)[0]
                right_part_number = expression.split(comparison_operator)[1]
                break
        try:
            float_right_part_number = float(right_part_number)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
        right_part = current_comparison_operator + "0"
        found_number = False
        for arithmetical_operator in arithmetical_operators:
            if arithmetical_operator in left_part:
                found_number = True
                current_arithmetical_operator = arithmetical_operator
                left_part_number = current_arithmetical_operator + left_part.split(arithmetical_operator)[1]
                break
        if found_number:
            measure = left_part.split(current_arithmetical_operator)[0]
            try:
                float_left_part_number = float(left_part_number)
            except Exception as e:
                print(get_linenumber(), "Something went wrong:", e)
                raise RuntimeError()
            float_combined_number = float_left_part_number + (-1 * float_right_part_number)
            if float_combined_number > 0:
                final_left_part = "x + " + str(float_combined_number)
            else:
                final_left_part = "x " + str(float_combined_number)
        else:
            measure = left_part
            if -1 * float_right_part_number > 0:
                final_left_part = "x + " + str(float_right_part_number)
            else:
                final_left_part = "x " + str(-1 * float_right_part_number)
        return measure, final_left_part, right_part

    cube.info(display=False)
    input_type = _get_input_type(cube.measure_type)
    output_type = _get_output_type(if_true, input_type)
    if not output_type:
        raise RuntimeError('given output_type is wrong')
    if not input_type:
        raise RuntimeError('given input_type is wrong')
    measure, measure_modification, comparison = _split_expression(expression)
    if measure == cube.measure:
        measure = "measure"
    else:
        raise RuntimeError('measure is wrong')
    try:
        results = cube.apply(
            query="oph_predicate('" + input_type + "','" + output_type + "'," + measure + ",'" +
                  measure_modification + "','" + comparison + "','" + str(if_true) + "','" +
                  str(if_false) + "')", ncores=ncores, nthreads=nthreads, description=description,
            display=display
        )
    except Exception as e:
        print(get_linenumber(), "Something went wrong:", e)
        raise RuntimeError()
    return results


def convert_to_xarray(cube):
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

    def _add_coordinates_and_data(cube, dr, response):
        lengths = []
        try:
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_dimvalues':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowfieldtypes'] and response_j['rowfieldtypes'][1] and \
                                response_j['rowvalues']:
                            if response_j['title'] == 'time':
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    dims = [s.strip() for s in val[1].split(',')]
                                    temp_array.append(dims[0])
                                dr[response_j['title']] = temp_array
                            else:
                                lengths.append(len(response_j['rowvalues']))
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    decoded_bin = base64.b64decode(val[1])
                                    length = _calculate_decoded_length(decoded_bin, response_j['rowfieldtypes'][1])
                                    format = _get_unpack_format(length, response_j['rowfieldtypes'][1])
                                    dims = struct.unpack(format, decoded_bin)
                                    temp_array.append(dims[0])
                                dr[response_j['title']] = list(temp_array)
                            last_dim_length = len(response_j['rowvalues'])
                        else:
                            raise RuntimeError("Unable to get dimension name or values in response")
                    break
        except Exception as e:
            print(get_linenumber(), "Unable to get dimensions from response:", e)
            return None
        try:
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_data':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowkeys'] and response_j['rowfieldtypes'] \
                                and response_j['rowvalues']:
                            measure_name = ""
                            measure_index = 0
                            # if not adimCube:
                            #     # Check that implicit dimension is just one
                            #     if len(data_values["dimension"].keys()) - (
                            #             len(response_j['rowkeys']) - 1) / 2.0 > 1:
                            #         raise RuntimeError("More than one implicit dimension")
                            for i, t in enumerate(response_j['rowkeys']):
                                if response_j['title'] == t:
                                    measure_name = t
                                    measure_index = i
                                    break
                            if measure_index == 0:
                                raise RuntimeError("Unable to get measure name in response")
                            values = []
                            last_length = 1
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
                                last_length = len(measure)
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
            # if len(data_values["measure"].keys()) == 0:
            #     raise RuntimeError("No measure found")
        except Exception as e:
            print("Unable to get measure from response:", e)
            return None
        sorted_coordinates = []
        for l in lengths:
            for c in cube.dim_info:
                if l == int(c["size"]) and c["name"] not in sorted_coordinates:
                    sorted_coordinates.append(c["name"])
                    break
        dr[cube.measure] = (sorted_coordinates, measure)
        return dr

    def _get_meta_info(response):
        meta_dict = {}
        for obj in response["response"]:
            if "objcontent" in obj.keys():
                if "rowvalues" in obj["objcontent"][0].keys():
                    for row in obj["objcontent"][0]["rowvalues"]:
                        key = row[2]
                        value = row[4]
                        meta_dict[key] = value
        return meta_dict

    def _initiate_xarray_object(cube, meta_info_dict):
        coordinates = [c["name"] for c in cube.dim_info]
        dr = xr.Dataset({cube.measure: ""}, attrs=meta_info_dict)
        for c in coordinates:
            dr = dr.assign_coords(coords={c: []})
        return dr

    import xarray as xr
    cube.info(display=False)
    pid = cube.pid
    query = 'oph_metadata cube={0}'.format(pid)
    cube.client.submit(query, display=False)
    meta_response = cube.client.deserialize_response()
    meta_info_dict = _get_meta_info(meta_response)
    dr = _initiate_xarray_object(cube, meta_info_dict)

    query = 'oph_explorecube ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord;limit_filter=0;cube={0};'. \
        format(pid)
    cube.client.submit(query, display=False)
    response = cube.client.deserialize_response()
    dr = _add_coordinates_and_data(cube, dr, response)
    return dr


from PyOphidia import cube

cube.Cube.setclient()
rand_cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                      measure='tasmax',
                      import_metadata='yes',
                      imp_dim='time',
                      imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                      ncores=4,
                      description='Max Temps'
                      )
# rand_cube = cube.Cube.randcube(container="mytest", dim="lat|lon|time", dim_size="4|2|1", exp_ndim=2,
#                                    host_partition="main", measure="tos", measure_type="double", nfrag=4, ntuple=2,
#                                    nhost=1)
rand_cube.info(display=False)

#
# print(rand_cube.dim_info)
# quit()
dr = convert_to_xarray(rand_cube)
print(dr)
