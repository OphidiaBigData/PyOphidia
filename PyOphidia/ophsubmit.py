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

import sys
import base64
import re
from xml.dom import minidom
from inspect import currentframe
if sys.version_info < (3, 0):
    import httplib
else:
    import http.client as httplib


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


SOAP_MESSAGE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope
xmlns:ns0 = "urn:oph"
xmlns:ns1 = "http://schemas.xmlsoap.org/soap/envelope/"
xmlns:xsi = "http://www.w3.org/2001/XMLSchema-instance"
xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
<SOAP-ENV:Header/>
<ns1:Body>
<ns0:ophExecuteMain>
<ophExecuteMainRequest>%s</ophExecuteMainRequest>
</ns0:ophExecuteMain>
</ns1:Body>
</SOAP-ENV:Envelope>
"""

OPH_SERVER_OK = 0
OPH_SERVER_UNKNOWN = 1
OPH_SERVER_NULL_POINTER = 2
OPH_SERVER_ERROR = 3
OPH_SERVER_IO_ERROR = 4
OPH_SERVER_AUTH_ERROR = 5
OPH_SERVER_SYSTEM_ERROR = 6
OPH_SERVER_WRONG_PARAMETER_ERROR = 7
OPH_SERVER_NO_RESPONSE = 8

OPH_WORKFLOW_DELIMITER = '?'

WRAPPING_WORKFLOW1 = "{\n  \"name\":\"NAME\",\n  \"author\":\"AUTHOR\",\n  \"abstract\":\"Workflow generated automatically to wrap a command\","
WRAPPING_WORKFLOW2 = "\n  \"sessionid\":\""
WRAPPING_WORKFLOW2_1 = "\","
WRAPPING_WORKFLOW3 = "\n  \"exec_mode\":\""
WRAPPING_WORKFLOW3_1 = "\","
WRAPPING_WORKFLOW4 = "\n  \"callback_url\":\""
WRAPPING_WORKFLOW4_1 = "\","
WRAPPING_WORKFLOW5 = "\n  \"tasks\": [\n    {\n      \"name\":\"Task 0\",\n      \"operator\":\""
WRAPPING_WORKFLOW5_1 = "\",\n      \"arguments\": ["
WRAPPING_WORKFLOW6 = "\"%s\""
WRAPPING_WORKFLOW7 = ",\"%s\""
WRAPPING_WORKFLOW8 = "]\n    }\n  ]\n}"


def submit(username, password, server, port, query):
    try:
        if sys.version_info < (2, 7, 9):
            client = httplib.HTTPS(str(server) + ":" + str(port))
        else:
            import ssl
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            context.verify_mode = ssl.CERT_NONE
            client = httplib.HTTPSConnection(str(server), str(port), context=context)
        client.putrequest("POST", "")
        client.putheader("User-Agent", "Ophidia Python client")
        client.putheader("Content-type", "text/xml; charset=\"UTF-8\"")
    except Exception as e:
        print(get_linenumber(), "Something went wrong in connection setup:", e)
        return (None, None, None, 1, e)
    request = str(query)
    if not request.lstrip(' \n\t').startswith('{'):
        wrapped_query = request.lstrip(' \n\t')
        if not wrapped_query.startswith('operator='):
            if not wrapped_query.startswith('oph_'):
                return (None, None, None, 3, "Invalid request")
        if wrapped_query.startswith('oph_'):
            wrapped_query = 'operator=' + wrapped_query[:wrapped_query.find(' ')] + ';' + wrapped_query[wrapped_query.find(' ') + 1:]
        query_list = re.split(r'(?![^\[]*\]);+', wrapped_query)
        if not query_list:
            return (None, None, None, 3, "Invalid request")
        # operator
        for element in query_list:
            if element:
                element_list = element.split('=', 1)
                if element_list[0] == 'operator':
                    request = WRAPPING_WORKFLOW1.replace('NAME', element_list[1]).replace('AUTHOR', str(username))
                    operator = element_list[1]
                    break
        else:
            return (None, None, None, 3, "Invalid request")
        # sessionid
        for element in query_list:
            if element:
                element_list = element.split('=', 1)
                if element_list[0] == 'sessionid':
                    request += WRAPPING_WORKFLOW2 + element_list[1] + WRAPPING_WORKFLOW2_1
                    break
        # exec_mode
        for element in query_list:
            if element:
                element_list = element.split('=', 1)
                if element_list[0] == 'exec_mode':
                    request += WRAPPING_WORKFLOW3 + element_list[1] + WRAPPING_WORKFLOW3_1
                    break
        # callback_url
        for element in query_list:
            if element:
                element_list = element.split('=', 1)
                if element_list[0] == 'callback_url':
                    request += WRAPPING_WORKFLOW4 + element_list[1] + WRAPPING_WORKFLOW4_1
                    break
        request += WRAPPING_WORKFLOW5 + operator + WRAPPING_WORKFLOW5_1
        # all remaining arguments
        step = 0
        for element in query_list:
            if element:
                element_list = element.split('=', 1)
                if element_list[0] != 'operator' and element_list[0] != 'sessionid' and element_list[0] != 'exec_mode' and element_list[0] != 'callback_url':
                    step += 1
                    if step == 1:
                        request += WRAPPING_WORKFLOW6.replace('%s', element)
                    else:
                        request += WRAPPING_WORKFLOW7.replace('%s', element)
        request += WRAPPING_WORKFLOW8
    try:
        # Escape &, <, > and \n chars for http
        request = request.replace("&", "&amp;")
        request = request.replace("<", "&lt;")
        request = request.replace(">", "&gt;")
        request = request.replace("\n", "&#xA;")
        soapMessage = SOAP_MESSAGE_TEMPLATE % request
        client.putheader("Content-length", "%d" % len(soapMessage))
        client.putheader("SOAPAction", "\"\"")
        user = str(username) + ':' + str(password)

        if sys.version_info < (3, 0):
            auth = 'Basic ' + base64.b64encode(user)
        else:
            auth = 'Basic ' + base64.b64encode(bytes(user, "utf-8")).decode("ISO-8859-1")

        client.putheader('Authorization', auth)
        client.endheaders()

        if sys.version_info < (3, 0):
            client.send(soapMessage)
            if sys.version_info < (2, 7, 9):
                statuscode, statusmessage, header = client.getreply()
                reply = client.getfile().read()
            else:
                _res = client.getresponse()
                statuscode, statusmessage = _res.status, _res.reason
                reply = _res.read()
        else:
            client.send(bytes(soapMessage, "utf-8"))
            _res = client.getresponse()
            statuscode, statusmessage = _res.status, _res.reason
            reply = _res.read()

        if statuscode != 200:
            print(get_linenumber(), "Something went wrong in submitting the request:", statuscode, statusmessage)
            return (None, None, None, 1, statusmessage)

        xmldoc = minidom.parseString(reply)
        response = xmldoc.getElementsByTagName('oph:ophResponse')[0]
        res_error, res_response, res_jobid = None, None, None
        if len(response.getElementsByTagName('jobid')) > 0 and response.getElementsByTagName('jobid')[0].firstChild is not None:
            res_jobid = response.getElementsByTagName('jobid')[0].firstChild.data
        if len(response.getElementsByTagName('error')) > 0 and response.getElementsByTagName('error')[0].firstChild is not None:
            res_error = int(response.getElementsByTagName('error')[0].firstChild.data)
        if len(response.getElementsByTagName('response')) > 0 and response.getElementsByTagName('response')[0].firstChild is not None:
            res_response = response.getElementsByTagName('response')[0].firstChild.data
    except Exception as e:
        print(get_linenumber(), "Something went wrong in submitting the request:", e)
        return (None, None, None, 1, e)
    if res_error is None:
        return (None, None, None, 1, "Invalid response")
    if res_error == OPH_SERVER_OK:
        response, jobid, newsession, return_value, error = None, None, None, 0, None
        if res_response is not None:
            if '"title": "ERROR"' in res_response or ('"title": "Workflow Status"' in res_response and '"message": "OPH_STATUS_ERROR"' in res_response):
                error = "There was an error in one or more tasks"
            if sys.version_info < (3, 0):
                response = str(res_response.encode("ISO-8859-1"))
            else:
                response = str(res_response.encode("ISO-8859-1").decode("UTF-8"))
        if res_jobid is not None:
            if len(res_jobid) != 0:
                jobid = str(res_jobid)
                index = jobid.rfind(OPH_WORKFLOW_DELIMITER)
                if index == -1:
                    newsession = jobid
                    jobid = None
                else:
                    newsession = jobid[:index]
            else:
                newsession = str()
        return (response, jobid, newsession, return_value, error)
    elif res_error == OPH_SERVER_UNKNOWN:
        return (None, None, None, res_error, "Error on serving request: server unknown")
    elif res_error == OPH_SERVER_NULL_POINTER:
        return (None, None, None, res_error, "Error on serving request: server null pointer")
    elif res_error == OPH_SERVER_ERROR:
        return (None, None, None, res_error, "Error on serving request: server error")
    elif res_error == OPH_SERVER_IO_ERROR:
        return (None, None, None, res_error, "Error on serving request: server IO error")
    elif res_error == OPH_SERVER_AUTH_ERROR:
        return (None, None, None, res_error, "Error on serving request: server authentication error")
    elif res_error == OPH_SERVER_SYSTEM_ERROR:
        return (None, None, None, res_error, "Error on serving request: server system error")
    elif res_error == OPH_SERVER_WRONG_PARAMETER_ERROR:
        return (None, None, None, res_error, "Error on serving request: server wrong parameter error")
    elif res_error == OPH_SERVER_NO_RESPONSE:
        return (None, None, None, res_error, "Error on serving request: server no response")
    else:
        return (None, None, None, res_error, "Error on serving request: error undefined")
