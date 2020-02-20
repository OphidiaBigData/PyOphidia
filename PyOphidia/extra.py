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
            if input_type == "oph_short":
                return "oph_short"
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


def add(cube=cube, measure="measure", addend=0, ncores=1, nthreads=1, description='-', display=False):
    """add(cube=cube, measure="measure", addend=0, ncores=1, nthreads=1, description='-',
            display=False) -> Pyophidia.cube : Get a cube object after having run the oph_sum_scalar or oph_sum_array
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :param measure: the measure that will be used to add the addend/s
    :type measure: str
    :param addend: the integer/float or array of integers/floats that will be added to the measure
    :type addend: int or float or list
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

    def _get_types(input, measure_type):
        """_get_input_type(input_type) -> str : Get the ophidia input type of the where function
        :param input: addent's type
        :type input: int or float
        :returns: a string that represents the Pyophidia type of the input variable
        :rtype: str
        :raises: RuntimeError
        """
        if isinstance(input, int):
            if measure_type == "long":
                return "oph_long", "oph_long"
            elif measure_type == "short":
                return "oph_short", "oph_short"
            elif measure_type == "float":
                return "oph_float", "oph_float"
            elif measure_type == "double":
                return "oph_double", "oph_double"
            return "oph_int", "oph_int"
        elif isinstance(input, float):
            if measure_type == "double":
                return "oph_double", "oph_double"
            return "oph_float", "oph_float"
        elif isinstance(input, bytes):
            if measure_type == "bytes":
                return "oph_bytes", "oph_bytes"
            else:
                raise RuntimeError('you cant add bytes to numbers')
        else:
            raise RuntimeError('given input type is wrong')

    cube.info(display=False)
    if isinstance(addend, list):
        if len(addend) != int(cube.elementsxrow):
            raise RuntimeError("Wrong array size")
        input_type, output_type = _get_types(addend[0], cube.measure_type)
        input_type = "|".join([input_type, input_type])
        if measure == cube.measure:
            measure = "measure"
        else:
            raise RuntimeError('measure is wrong')
        try:
            addend_string = ",".join([str(n) for n in addend])
            results = cube.apply(query="oph_sum_array('" + input_type + "','" +
                                       output_type + "',measure, oph_to_bin('','" + input_type.split('|')[0] + "','"
                                       + addend_string + "'))",
                                 check_type='no', ncores=ncores, nthreads=nthreads, description=description,
                                 display=display)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
    elif isinstance(addend, int) or isinstance(addend, float):
        input_type, output_type = _get_types(addend, cube.measure_type)
        if measure == cube.measure:
            measure = "measure"
        else:
            raise RuntimeError('measure is wrong')
        try:
            results = cube.apply(query="oph_sum_scalar('" + input_type + "','" + output_type + "',measure, "
                                       + str(addend) + ")",
                                 check_type='no', ncores=ncores, nthreads=nthreads, description=description,
                                 display=display)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
    else:
        raise RuntimeError('addend type is wrong, must be int, float or list')
    return results


def multiply(cube=cube, measure="measure", multiplier=1, ncores=1, nthreads=1, description='-', display=False):
    """multiply(cube=cube, measure="measure", multiplier=1, ncores=1, nthreads=1, description='-',
            display=False) -> Pyophidia.cube : Get a cube object after having run the oph_mul_scalar or oph_mul_array
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :param measure: the measure that will be used to multiply
    :type measure: str
    :param multiplier: the integer/float or array of integers/floats that will be multiply the measure
    :type multiplier: int or float or list
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

    def _get_types(input, measure_type):
        """_get_input_type(input_type) -> str : Get the ophidia input type of the where function
        :param input: multiplier's type
        :type input: int or float
        :returns: a string that represents the Pyophidia type of the input variable
        :rtype: str
        :raises: RuntimeError
        """
        if isinstance(input, int):
            if measure_type == "long":
                return "oph_long", "oph_long"
            elif measure_type == "short":
                return "oph_short", "oph_short"
            elif measure_type == "float":
                return "oph_float", "oph_float"
            elif measure_type == "double":
                return "oph_double", "oph_double"
            return "oph_int", "oph_int"
        elif isinstance(input, float):
            if measure_type == "double":
                return "oph_double", "oph_double"
            return "oph_float", "oph_float"
        elif isinstance(input, bytes):
            if measure_type == "bytes":
                return "oph_bytes", "oph_bytes"
            else:
                raise RuntimeError('you cant add bytes to numbers')
        else:
            raise RuntimeError('given input type is wrong')

    cube.info(display=False)
    if isinstance(multiplier, list):
        if len(multiplier) != int(cube.elementsxrow):
            raise RuntimeError("Wrong array size")
        input_type, output_type = _get_types(multiplier[0], cube.measure_type)
        input_type = "|".join([input_type, input_type])
        if measure == cube.measure:
            measure = "measure"
        else:
            raise RuntimeError('measure is wrong')
        try:
            addend_string = ",".join([str(n) for n in multiplier])
            results = cube.apply(query="oph_mul_array('" + input_type + "','" +
                                       output_type + "',measure, oph_to_bin('','" + input_type.split('|')[0] + "','"
                                       + addend_string + "'))",
                                 check_type='no', ncores=ncores, nthreads=nthreads, description=description,
                                 display=display)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
    elif isinstance(multiplier, int) or isinstance(multiplier, float):
        input_type, output_type = _get_types(multiplier, cube.measure_type)
        if measure == cube.measure:
            measure = "measure"
        else:
            raise RuntimeError('measure is wrong')
        try:
            results = cube.apply(query="oph_mul_scalar('" + input_type + "','" + output_type + "',measure, "
                                       + str(multiplier) + ")",
                                 check_type='no', ncores=ncores, nthreads=nthreads, description=description,
                                 display=display)
        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            raise RuntimeError()
    else:
        raise RuntimeError('addend type is wrong, must be int, float or list')
    return results


def select(cube=cube, type="coord", dims={}, ncores=1, nthreads=1, description='-',
           display=False, tolerance=0):

    def _perform_checks(type, dims, cube, tolerance):
        if len(dims.keys()) <= 0:
            raise RuntimeError('Empty list of dimensions')
        if type != "coord" and type != "index":
            raise RuntimeError('Wrong type')
        if type == "index" and tolerance != 0:
            raise RuntimeError('Tolerance is incompatible with index type')
        if "time" in dims.keys() and type == "index":
            raise RuntimeError("Can't use time filter with index type")
        cube_dims = [d["name"] for d in cube.dim_info]
        for key in dims.keys():
            if key not in cube_dims:
                raise RuntimeError('Key {0} not in cube dimensions'.format(key))

    def _convert_slice(slice_string):
        if len(slice_string.split(":")) == 2:
            return slice(slice_string.split(":")[0].strip(), slice_string.split(":")[1].strip())
        elif len(slice_string.split(":")) == 3:
            return slice(slice_string.split(":")[0].strip(), slice_string.split(":")[1].strip(),
                         slice_string.split(":")[2].strip())
        else:
            raise RuntimeError('Wrong format in slice')

    def _convert_filter(filters):
        for i in range(0, len(filters)):
            if ":" in filters[i]:
                slice_obj = _convert_slice(filters[i].replace("[", "").replace("]", ""))
                if slice_obj.stop == "-1":
                    slice_obj = slice(slice_obj.start, "end", slice_obj.step)
                if slice_obj.step:
                    filters[i] = ":".join([slice_obj.start, slice_obj.stop, slice_obj.step])
                else:
                    filters[i] = ":".join([slice_obj.start, slice_obj.stop])
        return ",".join(filters)

    def _time_check(datetimes):
        import datetime
        date_formats = ['%Y-%m-%d', '%Y-%m', '%Y', '%Y-%m-%d %H', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for i in range(0, len(datetimes)):
            datetime_bool = False
            if datetimes[i].strip() == "DJF":
                datetime_bool = True
            if datetimes[i].strip() == "MAM":
                datetime_bool = True
            if datetimes[i].strip() == "JJA":
                datetime_bool = True
            if datetimes[i].strip() == "SON":
                datetime_bool = True
            for date_format in date_formats:
                try:
                    datetime.datetime.strptime(datetimes[i], date_format)
                    datetime_bool = True
                except:
                    pass
            if not datetime_bool:
                raise RuntimeError('Wrong date format')
        return ",".join(datetimes)

    time_filter = "no"
    cube.info(display=False)
    _perform_checks(type, dims, cube, tolerance)
    for key in dims:
        if key == "time":
            time_filter = "yes"
            dims[key] = _time_check(dims[key])
        else:
            dims[key] = _convert_filter(dims[key])
    if type == "index":
        return cube.subset(subset_type=type, subset_dims="|".join(dims.keys()),
                           subset_filter="|".join(dims.values()), ncores=ncores,
                           nthreads=nthreads, description=description, display=display, time_filter=time_filter)
    else:
        return cube.subset(subset_type=type, subset_dims="|".join(dims.keys()),
                           subset_filter="|".join(dims.values()), ncores=ncores,
                           nthreads=nthreads, description=description, display=display, offset=tolerance,
                           time_filter=time_filter)


from PyOphidia import cube

cube.Cube.setclient()
cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                 measure='tasmax',
                 import_metadata='yes',
                 imp_dim='time',
                 imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                 ncores=4,
                 description='Max Temps'
                 )


dimensions = {}
dimensions["lat"] = ["[1:5:1]"]
dimensions["lon"] = ["[1:10:1]"]
results = select(cube=cube, type="index", dims=dimensions, ncores=1, nthreads=1, description='-',
                 display=False)
results.info(display=False)
print(results.size)

dimensions = {}
dimensions["lat"] = ["[1:5]"]
dimensions["lon"] = ["[1:10]"]
dimensions["time"] = ["2018", "2016"]
results = select(cube=cube, type="coord", dims=dimensions, ncores=1, nthreads=1, description='-',
                 display=False)
results.info(display=False)
print(results.size)

