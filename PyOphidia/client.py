#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2018 CMCC Foundation
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
import json
import re
from inspect import currentframe
import PyOphidia.ophsubmit as _ophsubmit
import traceback
import shutil
sys.path.append(os.path.dirname(__file__))


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


class Client():
    """Client(username='', password='', server='', port='11732', token='', read_env=False, api_mode=True) -> obj

    Attributes:
        username: Ophidia username
        password: Ophidia password
        server: Ophidia server address
        port: Ophidia server port (default is 11732)
        session: ID of the current session
        cwd: Current Working Directory
        cdd: Current Data Directory
        base_src_path: Base path for data files
        cube: Last produced cube PID
        host_partition: Name of default host partition
        exec_mode: Execution mode, 'sync' for synchronous mode (default),'async' for asynchronous mode
        ncores: Number of cores for each operation (default is 1)
        last_request: Last submitted query
        last_response: Last response received from the server (JSON string)
        last_jobid: Job ID associated to the last request
        last_return_value: Last return value associated to response
        last_error: Last error value associated to response
        last_exec_time: Last execution time associated to response

    Methods:
        submit(query, display=False) -> self : Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the
            Ophidia server according to all login parameters of the Client and its state.
        get_progress(id=None) -> dict : Get progress of a workflow, either specifying the id or from the last submitted one.
        deserialize_response() -> dict : Return the last_response JSON string attribute as a Python dictionary.
        get_base_path(display=False) -> self : Get base path for data from the Ophidia instance.
        resume_session(display=False) -> self : Resume the last session the user was connected to.
        resume_cdd(display=False) -> self : Resume the last cdd (current data directory) the user was located into.
        resume_cwd(display=False) -> self : Resume the last cwd (current working directory) the user was located into.
        resume_cube(display=False) -> self : Resume the last cube produced by the user.
        wsubmit(workflow,*params) -> self : Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series
            of parameters that will replace $1, $2 etc. in the workflow.
            The workflow will be validated against the Ophidia Workflow JSON Schema.
        wisvalid(workflow) -> bool : Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False.
        pretty_print(response, response_i) -> self : Prints the last_response JSON string attribute as a formatted response
    """

    def __init__(self, username='', password='', server='', port='11732', token='', read_env=False, api_mode=True):
        """Client(username='', password='', server='', port='11732', token='', read_env=False, api_mode=True) -> obj
        :param api_mode: If True, use the class as an API and catch also framework-level errors
        :type api_mode: bool
        :param username: Ophidia username
        :type username: str
        :param password: Ophidia password
        :type password: str
        :param server: Ophidia server address
        :type server: str
        :param port: Ophidia server port (default is 11732)
        :type port: str
        :param token: Ophidia token
        :type token: str
        :param read_env: If True read the client variables from the environment
        :type read_env: bool
        :returns: None
        :rtype: None
        :raises: RuntimeError
        """

        self.api_mode = api_mode
        if read_env is False:
            self.username = username
            self.password = password
            self.server = server
            self.port = port
            access_token = token
        else:
            if username:
                self.username = username
            else:
                self.username = os.environ.get('OPH_USER')
            if password:
                self.password = password
            else:
                self.password = os.environ.get('OPH_PASSWD')
            if server:
                self.server = server
            else:
                self.server = os.environ.get('OPH_SERVER_HOST')
            if port:
                self.port = port
            else:
                self.port = os.environ.get('OPH_SERVER_PORT')
            if token:
                access_token = token
            else:
                access_token = os.environ.get('OPH_TOKEN')

        self.session = ''
        self.cwd = '/'
        self.cdd = '/'
        self.base_src_path = '/'
        self.cube = ''
        self.host_partition = 'test'
        self.exec_mode = 'sync'
        self.ncores = 1
        self.last_request = ''
        self.last_response = ''
        self.last_jobid = ''
        self.last_return_value = 0
        self.last_error = ''
        self.last_exec_time = 0.0

        if not self.username and not self.password and access_token:
            self.password = token
            self.username = "__token__"

        if not self.username or not self.password or not self.server or not self.port:
            raise RuntimeError('one or more login parameters are None')
        try:
            if self.api_mode:
                self.resume_session()
                if self.session is not None and self.session:
                    self.get_base_path()
                    self.resume_cdd()
                    self.resume_cwd()
                    self.resume_cube()
        except Exception as e:
            print(get_linenumber(), "Something went wrong in resuming last session, cwd or cube:", e)
        else:
            if self.api_mode:
                if self.cdd:
                    print("Current cdd is " + self.cdd)
                if self.session:
                    print("Current session is " + self.session)
                if self.cwd:
                    print("Current cwd is " + self.cwd)
                if self.cube:
                    print("The last produced cube is " + self.cube)
        finally:
            pass

    def __del__(self):
        del self.api_mode
        del self.username
        del self.password
        del self.server
        del self.port
        del self.session
        del self.cwd
        del self.cdd
        del self.base_src_path
        del self.cube
        del self.host_partition
        del self.exec_mode
        del self.ncores
        del self.last_request
        del self.last_response
        del self.last_jobid
        del self.last_return_value
        del self.last_error

    def submit(self, query, display=False):
        """submit(query,display=False) -> self : Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server
               according to all login parameters of the Client and its state.
        :param query: query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;'
        :type query: str
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
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
        if self.cdd and 'cdd' not in query:
            query += 'cdd=' + self.cdd + ';'
        if self.cube and 'cube' not in query:
            query += 'cube=' + self.cube + ';'
        if self.host_partition and 'host_partition' not in query:
            query += 'host_partition=' + self.host_partition + ';'
        if self.exec_mode and 'exec_mode' not in query:
            query += 'exec_mode=' + self.exec_mode + ';'
        if self.ncores and 'ncores' not in query:
            query += 'ncores=' + str(self.ncores) + ';'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            if newsession is not None:
                if len(newsession) == 0:
                    self.session = None
                else:
                    if self.session != newsession:
                        self.cwd = '/'
                    self.session = newsession
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Output Cube':
                        self.cube = response_i['objcontent'][0]['message']
                        break
                else:
                    index = 0
                    if 'extra' in response:
                        for response_i in response['extra']['keys']:
                            if response_i == 'cube':
                                self.cube = response['extra']['values'][index]
                                break
                            index += 1

                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Current Working Directory':
                        self.cwd = response_i['objcontent'][0]['message']
                        break

                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Current Data Directory':
                        self.cdd = response_i['objcontent'][0]['message']
                        break

                index = 0
                if 'extra' in response:
                    for response_i in response['extra']['keys']:
                        if response_i == 'execution_time':
                            self.last_exec_time = float(response['extra']['values'][index])
                        elif response_i == 'access_token':
                            self.password = response['extra']['values'][index]
                        index += 1

                if self.api_mode and display is True:
                    self.pretty_print(response_i, response)

        except Exception as e:
            print(get_linenumber(), "Something went wrong in submitting the request:", e)
            return None
        return self

    def get_progress(self, id=None):
        """get_progress(id=None) -> dict : Get progress of a workflow, either specifying the id or from the last submitted one
        :param id: id of the workflow to monitor
        :type id: int
        :returns: workflow progess rate or None
        :rtype: dict or None
        :raises: RuntimeError
        """

        if id is None and self.last_jobid is None:
            raise RuntimeError('no jobid specified')
        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')

        query = 'oph_resume level=0;'
        if id:
            query += 'id=' + str(id) + ';'
        elif self.last_jobid:
            jobid = self.last_jobid.split('?')[1].split('#')[0]
            query += 'id=' + jobid + ';'

        progress_rate = 0
        submission_date = "0000-00-00 00:00:00"
        try:
            if self.submit(query, display=False) is None:
                raise RuntimeError()

            if self.last_response is not None:
                response = self.deserialize_response()

            if response is not None:
                for response_i in response['response']:
                    if response_i['objclass'] == 'grid' and response_i['objcontent'][0]['title'] == 'Workflow Progress Ratio':
                        submission_date = response_i['objcontent'][0]['rowvalues'][0][0]
                        progress_rate = float(response_i['objcontent'][0]['rowvalues'][0][1])
                        break

        except Exception as e:
            print(get_linenumber(), "Something went wrong:", e)
            return None

        return {'submission date': submission_date, 'progress rate': progress_rate}

    def deserialize_response(self):
        """deserialize_response() -> dict : Return the last_response JSON string attribute as a Python dictionary
        :returns: deserialized response or None
        :rtype: dict or None
        """

        if self.last_response is None:
            return None
        return json.loads(self.last_response)

    def pretty_print(self, response, response_i):
        """pretty_print(response, response_i) -> self : Prints the last_response JSON string attribute as a formatted response
        :param response: Python dictionary derived from the last_response JSON string
        :type response: dict
        :param response_i: each of the responses included in the list given by the dictionary key response['response']
        :type response_i: dict
        :returns: self or None
        :rtype: Client or None
        """

        response = self.deserialize_response()
        if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 3):
            from collections import namedtuple
            terminal_size = namedtuple('terminal_size', ['columns', 'lines'])
            sz = terminal_size(120, 10000)
        else:
            sz = shutil.get_terminal_size(fallback=(120, 10000))

        VERTICAL_CHAR = "|"
        HORIZONTAL_CHAR = "-"
        BORDER_CHAR = "="
        JUNCTION_CHAR = "+"

        if response is not None:
            for response_i in response['response']:
                try:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] != 'SUCCESS':
                        print(response_i['objcontent'][0]['title'])
                        title_length = len(response_i['objcontent'][0]['title'])
                        print("-" * title_length)
                        print(response_i['objcontent'][0]['message'])
                        print("\n")

                    if response_i['objclass'] == 'grid':
                        print(response_i['objcontent'][0]['title'])
                        title_length = len(response_i['objcontent'][0]['title'])
                        print(HORIZONTAL_CHAR * title_length)
                        num_columns = len(response_i['objcontent'][0]['rowkeys'])
                        columns = range(num_columns)
                        num_rows = len(response_i['objcontent'][0]['rowvalues'])
                        rows = range(num_rows)
                        max_column_width = []
                        for j in columns:
                            max_column_width.append(j)
                            max_column_width[j] = len(response_i['objcontent'][0]['rowkeys'][j])
                            for i in rows:
                                # Replace tabs with 4 spaces
                                response_i['objcontent'][0]['rowvalues'][i][j] = response_i['objcontent'][0]['rowvalues'][i][j].replace("\t", "    ")
                                if len(response_i['objcontent'][0]['rowvalues'][i][j]) > max_column_width[j]:
                                    # Compute max width based on line breaks
                                    max_column_width[j] = max([len(s) for s in response_i['objcontent'][0]['rowvalues'][i][j].split("\n")])
                        available_width = sz.columns
                        needed_width = sum(i for i in max_column_width) + (num_columns + 1) + (2 * num_columns)
                        while(needed_width > available_width):
                            if response_i['objkey'] == 'explorecube_data':
                                max_column_width[len(max_column_width) - 1] -= 1
                            else:
                                for i in range(len(max_column_width)):
                                    if max_column_width[i] > 1:
                                        max_column_width[i] -= 1
                            needed_width = sum(i for i in max_column_width) + (num_columns + 1) + (2 * num_columns)
                        for j in columns:
                            print(JUNCTION_CHAR + BORDER_CHAR * (max_column_width[j] + 2), end="")
                        print(JUNCTION_CHAR)
                        header_length = []
                        start = []
                        num_rows_per_column = []
                        for j in columns:
                            header_length.append(j)
                            start.append(j)
                            num_rows_per_column.append(j)
                            header_length[j] = len(response_i['objcontent'][0]['rowkeys'][j])
                            start[j] = 0
                            num_rows_per_column[j] = (int)(header_length[j] / max_column_width[j]) + 1

                        maximum_rows = num_rows_per_column[0]
                        for j in columns:
                            if num_rows_per_column[j] > maximum_rows:
                                maximum_rows = num_rows_per_column[j]

                        for x in range(maximum_rows):
                            for j in columns:

                                if start[j] < header_length[j]:
                                    print(VERTICAL_CHAR + " " + response_i['objcontent'][0]['rowkeys'][j][start[j]:start[j] + max_column_width[j]] + " " * ((max_column_width[j] + 2) -
                                          (len(response_i['objcontent'][0]['rowkeys'][j][start[j]:start[j] + max_column_width[j]]) + 1)), end="")
                                    start[j] = start[j] + max_column_width[j]
                                else:
                                    print(VERTICAL_CHAR + " " * (max_column_width[j] + 2), end="")
                            print(VERTICAL_CHAR)
                        for j in columns:
                            print(JUNCTION_CHAR + BORDER_CHAR * (max_column_width[j] + 2), end="")
                        print(JUNCTION_CHAR)
                        text_length = []
                        start = []
                        num_rows_per_column = []
                        maximum_rows = []
                        for i in rows:
                            maximum_rows.append(i)
                            text_length.append(i)
                            start.append(i)
                            num_rows_per_column.append(i)

                            text_length[i] = []
                            start[i] = []
                            num_rows_per_column[i] = []
                            for j in columns:
                                text_length[i].append(j)
                                start[i].append(j)
                                num_rows_per_column[i].append(j)
                                text_length[i][j] = len(response_i['objcontent'][0]['rowvalues'][i][j])
                                start[i][j] = 0
                                # Compute num of rows per column based on line breaks
                                num_rows_per_column[i][j] = sum([(int)(len(s) / (max_column_width[j] + 1)) + 1 for s in response_i['objcontent'][0]['rowvalues'][i][j].split("\n")])
                            maximum_rows[i] = num_rows_per_column[i][0]
                            for j in columns:
                                if maximum_rows[i] < num_rows_per_column[i][j]:
                                    maximum_rows[i] = num_rows_per_column[i][j]
                        for i in rows:
                            rowvalues = response_i['objcontent'][0]['rowvalues'][i]
                            for x in range(maximum_rows[i]):
                                for j in columns:
                                    if start[i][j] < text_length[i][j]:
                                        index = rowvalues[j][start[i][j]:start[i][j] + max_column_width[j]].find("\n")
                                        if index != -1:
                                            # Delete newline char
                                            rowvalues[j] = rowvalues[j][:start[i][j] + index] + rowvalues[j][start[i][j] + index + 1:]
                                            actual_len = start[i][j] + index
                                        else:
                                            actual_len = start[i][j] + max_column_width[j]

                                        print(VERTICAL_CHAR + " " + rowvalues[j][start[i][j]:actual_len] + " " * ((max_column_width[j] + 2) - (len(rowvalues[j][start[i][j]:actual_len]) + 1)), end="")
                                        start[i][j] = actual_len
                                    else:
                                        print(VERTICAL_CHAR + " " * (max_column_width[j] + 2), end="")
                                print(VERTICAL_CHAR)
                            if i != rows[len(rows) - 1]:
                                for j in columns:
                                    print(VERTICAL_CHAR + HORIZONTAL_CHAR * (max_column_width[j] + 2), end="")
                                print(VERTICAL_CHAR)
                            else:
                                for j in columns:
                                    print(JUNCTION_CHAR + BORDER_CHAR * (max_column_width[j] + 2), end="")
                                print(JUNCTION_CHAR)

                    if response_i['objclass'] == 'digraph':
                        print(response_i['objcontent'][0]['title'])
                        title_length = len(response_i['objcontent'][0]['title'])
                        print("-" * title_length)
                        print("Directed Graph DOT string :\n")
                        print("digraph DG {\n")
                        print("\tnode   [shape=box]\n")
                        num_nodevalues = len(response_i['objcontent'][0]['nodevalues'])
                        nodevalues = range(num_nodevalues)
                        for i in nodevalues:
                            print("\t" + str(i) + "\t[label=", end="")
                            num_labels = len(response_i['objcontent'][0]['nodekeys'])
                            labels = range(num_labels)
                            print("\"", end="")
                            for j in labels:
                                print(response_i['objcontent'][0]['nodekeys'][j] + " : ", end="")
                                print(response_i['objcontent'][0]['nodevalues'][i][j] + "  ", end="")
                            print("\"]\n")
                        print("\tedge\n")
                        num_nodelinks = len(response_i['objcontent'][0]['nodelinks'])
                        nodelinks = range(num_nodelinks)
                        for i in nodelinks:
                            if response_i['objcontent'][0]['nodelinks'][i]:
                                for j in range(len(response_i['objcontent'][0]['nodelinks'][i])):
                                    print("\t" + str(i) + "=>" + response_i['objcontent'][0]['nodelinks'][i][j]['node'] +
                                          "\t[label=\"" + response_i['objcontent'][0]['nodelinks'][i][j]['description'], end="")
                                print("\"]\n")
                        print("\n}\n")

                except Exception as e:
                    print(get_linenumber(), "Error in parsing json response:", e)

            print("Execution time: " + str(self.last_exec_time) + " seconds")

        return self

    def get_base_path(self, display=False):
        """get_base_path(display=False) -> self : Get base path for data from the Ophidia instance.
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: self or None
        :rtype: Client or None
        :raises: RuntimeError
        """

        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')
        query = 'operator=oph_get_config;key=OPH_BASE_SRC_PATH;'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objkey'] == 'get_config':
                        self.base_src_path = response_i['objcontent'][0]['rowvalues'][0][1]

                    if self.api_mode and display is True:
                        self.pretty_print(response_i, response)

                    break
        except Exception as e:
            print(get_linenumber(), "Something went wrong in retrieving base data path:", e)
            return None
        return self

    def resume_session(self, display=False):
        """resume_session(display=False) -> self : Resume the last session the user was connected to.
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: self or None
        :rtype: Client or None
        :raises: RuntimeError
        """

        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')
        query = 'operator=oph_get_config;key=OPH_SESSION_ID;'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objkey'] == 'get_config':
                        self.session = response_i['objcontent'][0]['rowvalues'][0][1]

                    if self.api_mode and display is True:
                        self.pretty_print(response_i, response)

                    break
        except Exception as e:
            print(get_linenumber(), "Something went wrong in resuming last session:", e)
            return None
        return self

    def resume_cdd(self, display=False):
        """resume_cdd(display=False) -> self : Resume the last cdd (current data directory) the user was located into.
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: self or None
        :rtype: Client or None
        :raises: RuntimeError
        """

        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')
        query = 'operator=oph_get_config;key=OPH_CDD;'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objkey'] == 'get_config':
                        self.cdd = response_i['objcontent'][0]['rowvalues'][0][1]

                    if self.api_mode and display is True:
                        self.pretty_print(response_i, response)

                    break
        except Exception as e:
            print(get_linenumber(), "Something went wrong in resuming last cdd:", e)
            return None
        return self

    def resume_cwd(self, display=False):
        """resume_cwd(display=False) -> self : Resume the last cwd (current working directory) the user was located into.
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: self or None
        :rtype: Client or None
        :raises: RuntimeError
        """

        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')
        query = 'operator=oph_get_config;key=OPH_CWD;'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objkey'] == 'get_config':
                        self.cwd = response_i['objcontent'][0]['rowvalues'][0][1]

                    if self.api_mode and display is True:
                        self.pretty_print(response_i, response)

                    break
        except Exception as e:
            print(get_linenumber(), "Something went wrong in resuming last cwd:", e)
            return None
        return self

    def resume_cube(self, display=False):
        """resume_cube(display=False) -> self : Resume the last cube produced by the user.
        :param display: option for displaying the response in a "pretty way" using the pretty_print function (default is False)
        :type display: bool
        :returns: self or None
        :rtype: Client or None
        :raises: RuntimeError
        """

        if self.username is None or self.password is None or self.server is None or self.port is None:
            raise RuntimeError('one or more login parameters are None')
        query = 'operator=oph_get_config;key=OPH_DATACUBE;'
        self.last_request = query
        try:
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, query)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objkey'] == 'get_config':
                        self.cube = response_i['objcontent'][0]['rowvalues'][0][1]

                    if self.api_mode and display is True:
                        self.pretty_print(response_i, response)

                    break
        except Exception as e:
            print(get_linenumber(), "Something went wrong in resuming last cube:", e)
            return None
        return self

    def wsubmit(self, workflow, *params):
        """wsubmit(workflow,*params) -> self : Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of
           parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.
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

        if os.path.isfile(workflow):
            try:
                file = open(workflow, 'r')
                buffer = file.read()
                file.close()
                for index, param in enumerate(params, start=1):
                    buffer = buffer.replace('${' + str(index) + '}', str(param))
                    buffer = re.sub('(\$' + str(index) + ')([^0-9]|$)', str(param) + '\g<2>', buffer)
                buffer = re.sub('(\$\{?(\d*)\}?)', '', buffer)
                # Remove comment blocks
                buffer = re.sub(re.compile('/\*.*?\*/|//.*?\n', re.DOTALL), '\n', buffer)
                request = json.loads(buffer)

            except Exception as e:
                print(get_linenumber(), "Something went wrong in reading and/or parsing the file:", e)
                return None
        else:
            try:
                buffer = workflow
                for index, param in enumerate(params, start=1):
                    buffer = buffer.replace('${' + str(index) + '}', str(param))
                    buffer = re.sub('(\$' + str(index) + ')([^0-9]|$)', str(param) + '\g<2>', buffer)
                buffer = re.sub('(\$\{?(\d*)\}?)', '', buffer)
                # Remove comment blocks
                buffer = re.sub(re.compile('/\*.*?\*/|//.*?\n', re.DOTALL), '\n', buffer)
                request = json.loads(buffer)

            except Exception as e:
                print(get_linenumber(), "Something went wrong in parsing the string:", e)
                return None

        if self.session and 'sessionid' not in request:
            request['sessionid'] = self.session
        if self.cwd and 'cwd' not in request:
            request['cwd'] = self.cwd
        if self.cdd and 'cdd' not in request:
            request['cdd'] = self.cdd
        if self.cube and 'cube' not in request:
            request['cube'] = self.cube
        if self.host_partition and 'host_partition' not in request:
            request['host_partition'] = self.host_partition
        if self.exec_mode and 'exec_mode' not in request:
            request['exec_mode'] = self.exec_mode
        if self.ncores and 'ncores' not in request:
            request['ncores'] = str(self.ncores)
        self.last_request = json.dumps(request)
        try:
            err, err_msg = self.wisvalid(self.last_request)
            if not err:
                print("The workflow is not valid: " + str(err_msg))
                return None
            self.last_response, self.last_jobid, newsession, self.last_return_value, self.last_error = _ophsubmit.submit(self.username, self.password, self.server, self.port, self.last_request)
            if self.last_return_value:
                raise RuntimeError(self.last_error)
            if self.api_mode and not self.last_return_value and self.last_error is not None:
                raise RuntimeError(self.last_error)

            if newsession is not None:
                if len(newsession) == 0:
                    self.session = None
                else:
                    self.session = newsession
                    self.cwd = '/'
            response = self.deserialize_response()
            if response is not None:
                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Output Cube':
                        self.cube = response_i['objcontent'][0]['message']
                        break
                else:
                    index = 0
                    if 'extra' in response:
                        for response_i in response['extra']['keys']:
                            if response_i == 'cube':
                                self.cube = response['extra']['values'][index]
                                break
                            index += 1

                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Current Working Directory':
                        self.cwd = response_i['objcontent'][0]['message']
                        break

                for response_i in response['response']:
                    if response_i['objclass'] == 'text' and response_i['objcontent'][0]['title'] == 'Current Data Directory':
                        self.cdd = response_i['objcontent'][0]['message']
                        break

                index = 0
                if 'extra' in response:
                    for response_i in response['extra']['keys']:
                        if response_i == 'execution_time':
                            self.last_exec_time = float(response['extra']['values'][index])
                        elif response_i == 'access_token':
                            self.password = response['extra']['values'][index]
                        elif response_i == 'cwd':
                            self.cwd = response['extra']['values'][index]
                        elif response_i == 'cdd':
                            self.cdd = response['extra']['values'][index]
                        index += 1

                self.pretty_print(response_i, response)

        except Exception as e:
            print(get_linenumber(), "Something went wrong in submitting the request:", e)
            return None
        return self

    def wisvalid(self, workflow):
        """wisvalid(workflow) -> bool : Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False.
        :param workflow: a JSON string or a Python dict containing an Ophidia workflow
        :type workflow: str or dict
        :returns: True or False and validation message
        :rtype: bool
        """

        if workflow is None:
            return False
        w = None

        # Remove comment blocks
        checked_workflow = re.sub(re.compile('/\*.*?\*/|//.*?\n', re.DOTALL), '\n', workflow)
        print(checked_workflow)
        if isinstance(checked_workflow, str):
            try:
                w = json.loads(checked_workflow)
            except:
                return False, "Workflow is not a valid JSON"
        elif isinstance(checked_workflow, dict):
            w = checked_workflow
        else:
            return False, "Workflow is not a valid dictionary"
        if 'name' not in w or not w['name']:
            return False, "Mandatory global argument 'name' is missing"
        if 'author' not in w or not w['author']:
            return False, "Mandatory global argument 'author' is missing"
        if 'abstract' not in w or not w['abstract']:
            return False, "Mandatory global argument 'abstract' is missing"
        if 'on_error' in w:
            try:
                if w['on_error'] != 'skip' and w['on_error'] != 'continue' and w['on_error'] != 'break' and \
                   (w['on_error'][:7] != 'repeat ' or not w['on_error'][7:].isdigit() or int(w['on_error'][7:]) < 0):
                    return False, "Mandatory global argument 'on_error' is not correct"
            except:
                return False, "Mandatory global argument 'on_error' is missing"
        if 'ncores' in w and not w['ncores'].isdigit():
            return False, "Mandatory global argument 'ncores' is missing or is not correct"
        if 'exec_mode' in w and w['exec_mode'] != 'sync' and w['exec_mode'] != 'async':
            return False, "Mandatory global argument 'exec_mode' is missing or is not correct"
        if 'tasks' not in w or not w['tasks']:
            return False, "Workflow task section is missing"
        pattern = re.compile('^[A-Za-z0-9_]+=')
        for task in w['tasks']:
            task_name = ""
            if 'name' not in task or not task['name']:
                return False, "Task 'name' is missing"
            else:
                task_name = str(task['name'])
            if 'operator' not in task or not task['operator']:
                return False, "Task 'operator' is missing in task: " + task_name
            if 'arguments' in task and task['arguments']:
                for argument in task['arguments']:
                    if not pattern.match(argument):
                        return False, "Task argument '" + str(argument) + "' is not valid in task: " + task_name
            if 'dependencies' in task and task['dependencies']:
                for dependency in task['dependencies']:
                    if 'task' not in dependency or not dependency['task']:
                        return False, "Dependency 'task' is missing in task: " + task_name
                    if 'type' in dependency:
                        if dependency['type'] != 'all' and dependency['type'] != 'single' and dependency['type'] != 'embedded':
                            return False, "Dependency 'type' is not correct in task: " + task_name
            if 'on_error' in task:
                try:
                    if task['on_error'] != 'skip' and task['on_error'] != 'continue' and task['on_error'] != 'break' and \
                       (task['on_error'][:7] != 'repeat ' or not task['on_error'][7:].isdigit() or int(task['on_error'][7:]) < 0):
                        return False, "Task 'on_error' is not correct in task: " + task_name
                except:
                    return False, "Task 'on_error' is not correct in task: " + task_name

        for index, task in enumerate(w['tasks']):
            if 'dependencies' in task and task['dependencies']:
                for dependency in task['dependencies']:
                    if dependency['task'] == task['name']:
                        return False, "Task dependency points to same task: " + str(dependency['task'])
                    for index2, task2 in enumerate(w['tasks']):
                        if dependency['task'] == task2['name']:
                            dependency['task_index'] = index2
                            if 'dependents_indexes' not in task2 or not task2['dependents_indexes']:
                                task2['dependents_indexes'] = []
                            task2['dependents_indexes'].append(index)
                            break
                    else:
                        return False, "Task dependency points to not existing task: " + str(dependency['task'])

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
        #   S <- Set of all nodes with no incoming edges
        #   while S is non-empty do
        #       remove a node n from S
        #       for each node m with an edge e from n to m do
        #           remove edge e from the graph
        #           if m has no other incoming edges then
        #               insert m into S
        #   if graph has edges then
        #       return error (graph has at least one cycle)
        #   else
        #       return success (graph has no cycles)

        #   S <- Set of all nodes with no incoming edges
        S = []
        for node in graph:
            if node.in_edges_num == 0:
                S.append(node)

        #   while S is non-empty do
        while len(S) != 0:
            #   remove a node n from S
            n = S.pop()
            assert isinstance(n, WorkflowNode)
            #   for each node m with an edge e from n to m do
            for i in range(len(n.out_edges)):
                if n.out_edges[i] != -1:
                    #   remove edge e from the graph
                    index = n.out_edges[i]
                    n.out_edges[i] = -1
                    n.out_edges_num -= 1
                    for j in range(len(graph[index].in_edges)):
                        if graph[index].in_edges[j] == n.index:
                            graph[index].in_edges[j] = -1
                            graph[index].in_edges_num -= 1
                            #   if m has no other incoming edges then
                            if graph[index].in_edges_num == 0:
                                #   insert m into S
                                S.append(graph[index])
                            break

        for node in graph:
            #   if graph has edges then
            if node.in_edges_num != 0 or node.out_edges_num != 0:
                #   return error (graph has at least one cycle)
                return False, "Workflow is not a DAG"
        #   else return success (graph has no cycles)
        return True, "Workflow is valid"
