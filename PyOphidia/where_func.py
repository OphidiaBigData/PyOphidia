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



