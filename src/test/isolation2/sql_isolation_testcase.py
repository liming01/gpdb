"""
Copyright (c) 2004-Present Pivotal Software, Inc.

This program and the accompanying materials are made available under
the terms of the under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pygresql.pg
import os
import subprocess
import re
import multiprocessing
import tempfile
import time
import sys
import socket
from optparse import OptionParser
import traceback
import select

sh_proc = None
v_cnt = 0
poller = None

def is_digit(n):
    try:
        int(n)
        return True
    except ValueError:
        return  False

def load_helper_file(helper_file):
    with open(helper_file) as file:
        return "".join(file.readlines()).strip()


def parse_include_statement(sql):
    include_statement, command = sql.split(None, 1)
    stripped_command = command.strip()

    if stripped_command.endswith(";"):
        return stripped_command.replace(";", "")
    else:
        raise SyntaxError("expected 'include: %s' to end with a semicolon." % stripped_command)

class GlobalShellExecutor(object):
    def __init__(self, output_file='', initfile_prefix=''):
        self.output_file = output_file
        self.initfile_prefix = initfile_prefix
        self.v_cnt = 0
        self.sh_proc = None
    
    def begin(self):
        self.v_cnt = 0
        self.sh_proc = subprocess.Popen(['/bin/bash'], stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Set env var ${NL} because "\n" can not be converted to new line for unknown escaping reason
        cmd = ''' export NL='\n';\n'''
        self.sh_proc.stdin.write(cmd)
        self.sh_proc.stdin.flush()

        self.poller=select.poll()
        self.poller.register(self.sh_proc.stdout,select.POLLIN)

    def terminate(self):
        if self.sh_proc == None:
            return;
        # If write the matchsubs section directly to the output, the generated token id will be compared by gpdiff.pl
        # so here just write all matchsubs section into an auto generated init file when this test case file finished.
        if self.initfile_prefix!=None and len(self.initfile_prefix)>1:
            output_init_file = "%s.ini" % self.initfile_prefix
            cmd = ''' [ ! -z "${MATCHSUBS}" ] && echo "-- start_matchsubs ${NL} ${MATCHSUBS} ${NL}-- end_matchsubs" > %s ''' % output_init_file
            self.exec_gsh(cmd, False)

        self.sh_proc.terminate()
        self.v_cnt = 0
        self.sh_proc = None

    # nonblock read output of global shell process, if error, write info to err_log_file
    def readlines_nonblock(self, sh_cmd):
        lines = []
        while True:
            if self.poller.poll(1):
                line = self.sh_proc.stdout.readline()
                if ("<<quit0>>" in line):
                    break
                elif ("<<quit" in line):
                    print  >>self.output_file, "Error to exec shell %s: %s" % (line.rstrip(), sh_cmd)
                    exit(1)
                lines.append(line)
            else:
                time.sleep(1)
        return lines
    
    # execute global shell cmd in bash deamon, and fetch result without blocking
    def exec_gsh(self, sh_cmd, is_trip_output_end_blanklines):
        if self.sh_proc == None:
            self.begin();
    
        # Add quit flag to return for readlines_nonblock(), and print $? for error tracing.
        cmd = ''' %s; echo "\n<<quit$?>>";\n''' % sh_cmd
        self.sh_proc.stdin.write(cmd)
        self.sh_proc.stdin.flush()
        # print if need to debug this shell cmd
        #print >>self.output_file, "shell cmd: %s" % cmd

        # get the output of shell commmand
        output=self.readlines_nonblock(cmd)
        if is_trip_output_end_blanklines:
            for i in range(len(output)-1, 0, -1):
                if output[i]=='\n':
                    del output[i]
                else:
                    break

        return output


    # execute gobal shell:
    # 1) set input stream -> $RAW_STR
    # 2) execute shell command from input
    # if error, write error message to err_log_file
    def exec_gsh_4_raw_str(self, input, sh_cmd, is_trip_output_end_blanklines):
        self.v_cnt = 1 + self.v_cnt
        escape_in = input.replace('\'',"'\\''")
        # replace env variable to specific one for not to effect each others
        sh_cmd = sh_cmd.replace("$RAW_STR", "$RAW_STR%d"%self.v_cnt)
        sh_cmd = sh_cmd.replace("${RAW_STR}", "${RAW_STR%d}"%self.v_cnt)
        # send shell cmd
        cmd = ''' export RAW_STR%d='%s' && %s''' % (self.v_cnt, escape_in, sh_cmd)
        return self.exec_gsh(cmd, is_trip_output_end_blanklines)

    # extrac shell shell, sql part from one line with format: @header '': SQL
    # return row: (found the header or not?, the extracted shell, the SQL in the left part)
    def extract_sh_cmd(self, header, input_str):
        start=len(header)
        is_start = False
        end=0
        is_trip_comma = False
        res_cmd=""
        res_sql=""

        input_str = input_str.lstrip()
        if not input_str.startswith(header):
            return (False, None, None)

        for i in range(start, len(input_str)):
            if end==0 and input_str[i] == '\'':
                if not is_start:
                    # find shell begin postion
                    is_start = True
                    start = i+1
                    continue
                cnt = 0
                for j in range(i-1, 0, -1):
                    if input_str[j] == '\\':
                        cnt = 1 + cnt
                    else:
                        break
                if cnt%2 == 1:
                    continue
                # find shell end postion
                res_cmd = input_str[start: i]
                end = i
                continue
            if end!=0:
                # skip space until ':'
                if input_str[i] == ' ':
                    continue
                elif input_str[i] == ':':
                    is_trip_comma = True
                    res_sql =  input_str[i+1:]
                    break
        if not is_start or end==0 or not is_trip_comma:
            raise Exception("Invalid format: %v", input_str)
        #unescape \' to ' and \\ to '
        res_cmd = res_cmd.replace('\\\'','\'')
        res_cmd = res_cmd.replace('\\\\','\\')
        return (True, res_cmd, res_sql)

class SQLIsolationExecutor(object):
    # define enum value for connection_mode
    NORMAL_MODE, UTILITY_MODE, RETRIEVE_MODE = range(0, 3)
    def __init__(self, dbname=''):
        self.processes = {}

        # The re.S flag makes the "." in the regex match newlines.
        # When matched against a command in process_command(), all
        # lines in the command are matched and sent as SQL query.
        self.command_pattern = re.compile(r"^(-?\d+|[*])([&\\<\\>URIq]*?)\:(.*)", re.S)
        if dbname:
            self.dbname = dbname
        else:
            self.dbname = os.environ.get('PGDATABASE')

    class SQLConnection(object):
        def __init__(self, out_file, name, connection_mode, dbname):
            self.name = name
            self.connection_mode = connection_mode
            self.out_file = out_file
            self.dbname = dbname

            parent_conn, child_conn = multiprocessing.Pipe(True)
            self.p = multiprocessing.Process(target=self.session_process, args=(child_conn,))   
            self.pipe = parent_conn
            self.has_open = False
            self.p.start()

            # Close "our" copy of the child's handle, so that if the child dies,
            # recv() on the pipe will fail.
            child_conn.close()

            self.out_file = out_file

        def session_process(self, pipe):
            sp = SQLIsolationExecutor.SQLSessionProcess(self.name, 
                self.connection_mode, pipe, self.dbname)
            sp.do()

        def query(self, command, out_sh_cmd, global_sh_executor):
            self.out_file.flush()
            if len(command.strip()) == 0:
                return
            if self.has_open:
                raise Exception("Cannot query command while waiting for results")

            self.pipe.send((command, False))
            r = self.pipe.recv()
            if r is None:
                raise Exception("Execution failed")
            
            if out_sh_cmd!=None:
                new_out=global_sh_executor.exec_gsh_4_raw_str(r.rstrip(), out_sh_cmd,  True)
                for line in new_out:
                    self.out_file.write(line)
                self.out_file.flush()
            else:
               print >>self.out_file, r.rstrip()

        def fork(self, command, blocking):
            print >>self.out_file, " <waiting ...>"
            self.pipe.send((command, True))

            if blocking:
                time.sleep(0.5)
                if self.pipe.poll(0):
                    p = self.pipe.recv()
                    raise Exception("Forked command is not blocking; got output: %s" % p.strip())
            self.has_open = True

        def join(self):
            r = None
            print >>self.out_file, " <... completed>"
            if self.has_open:
                r = self.pipe.recv()
            if r is None:
                raise Exception("Execution failed")
            print >>self.out_file, r.rstrip()
            self.has_open = False

        def stop(self):
            self.pipe.send(("", False))
            self.p.join()
            if self.has_open:
                raise Exception("Should not finish test case while waiting for results")

        def quit(self):
            print >>self.out_file, "... <quitting>"
            self.stop()
        
        def terminate(self):
            self.pipe.close()
            self.p.terminate()

    class SQLSessionProcess(object):
        def __init__(self, name, connection_mode, pipe, dbname):
            """
                Constructor
            """
            self.name = name
            self.connection_mode = connection_mode
            self.pipe = pipe
            self.dbname = dbname
            if self.connection_mode == SQLIsolationExecutor.UTILITY_MODE:
                (hostname, port) = self.get_utility_mode_port(name)
                self.con = self.connectdb(given_dbname=self.dbname,
                                          given_host=hostname,
                                          given_port=port,
                                          given_opt="-c gp_session_role=utility")
            elif self.connection_mode == SQLIsolationExecutor.RETRIEVE_MODE:
                (hostname, port) = self.get_utility_mode_port(name)
                self.con = self.connectdb(given_dbname=self.dbname,
                                          given_host=hostname,
                                          given_port=port,
                                          given_opt="-c gp_session_role=retrieve",
                                          given_passwd="nopasswd")
            else:
                self.con = self.connectdb(self.dbname)

        def connectdb(self, given_dbname, given_host = None, given_port = None, given_opt = None, given_user = None, given_passwd = None):
            con = None
            retry = 1000
            while retry:
                try:
                    if (given_port is None):
                        con = pygresql.pg.connect(host= given_host,
                                          opt= given_opt,
                                          dbname= given_dbname,
                                          user = given_user,
                                          passwd = given_passwd)
                    else:
                        con = pygresql.pg.connect(host= given_host,
                                                  port= given_port,
                                                  opt= given_opt,
                                                  dbname= given_dbname,
                                                  user = given_user,
                                                  passwd = given_passwd)
                    break
                except Exception as e:
                    if (("the database system is starting up" in str(e) or
                         "the database system is in recovery mode" in str(e)) and
                        retry > 1):
                        retry -= 1
                        time.sleep(0.1)
                    else:
                        raise
            return con

        def get_utility_mode_port(self, name):
            """
                Gets the port number/hostname combination of the
                contentid = name and role = primary
            """
            con = self.connectdb(self.dbname)
            r = con.query("SELECT hostname, port FROM gp_segment_configuration WHERE content = %s and role = 'p'" % name).getresult()
            if len(r) == 0:
                raise Exception("Invalid content %s" % name)
            if r[0][0] == socket.gethostname():
                return (None, int(r[0][1]))
            return (r[0][0], int(r[0][1]))

        # Print out a pygresql result set (a Query object, after the query
        # has been executed), in a format that imitates the default
        # formatting of psql. This isn't a perfect imitation: we left-justify
        # all the fields and headers, whereas psql centers the header, and
        # right-justifies numeric fields. But this is close enough, to make
        # gpdiff.pl recognize the result sets as such. (We used to just call
        # str(r), and let PyGreSQL do the formatting. But even though
        # PyGreSQL's default formatting is close to psql's, it's not close
        # enough.)
        def printout_result(self, r):
            widths = []

            # Figure out the widths of each column.
            fields = r.listfields()
            for f in fields:
                widths.append(len(str(f)))

            rset = r.getresult()
            for row in rset:
                colno = 0
                for col in row:
                    if col is None:
                        col = ""
                    widths[colno] = max(widths[colno], len(str(col)))
                    colno = colno + 1

            # Start printing. Header first.
            result = ""
            colno = 0
            for f in fields:
                if colno > 0:
                    result += "|"
                result += " " + f.ljust(widths[colno]) + " "
                colno = colno + 1
            result += "\n"

            # Then the bar ("----+----")
            colno = 0
            for f in fields:
                if colno > 0:
                    result += "+"
                result += "".ljust(widths[colno] + 2, "-")
                colno = colno + 1
            result += "\n"

            # Then the result set itself
            for row in rset:
                colno = 0
                for col in row:
                    if colno > 0:
                        result += "|"
                    if col is None:
                        col = ""
                    result += " " + str(col).ljust(widths[colno]) + " "
                    colno = colno + 1
                result += "\n"

            # Finally, the row count
            if len(rset) == 1:
                result += "(1 row)\n"
            else:
                result += "(" + str(len(rset)) +" rows)\n"

            return result

        def execute_command(self, command):
            """
                Executes a given command
            """
            try:
                r = self.con.query(command)
                if r and type(r) == str:
                    echo_content = command[:-1].partition(" ")[0].upper()
                    return "%s %s" % (echo_content, r)
                elif r:
                    return self.printout_result(r)
                else:
                    echo_content = command[:-1].partition(" ")[0].upper()
                    return echo_content
            except Exception as e:
                return str(e)

        def do(self):
            """
                Process loop.
                Ends when the command None is received
            """
            (c, wait) = self.pipe.recv()
            while c:
                if wait:
                    time.sleep(0.1)
                r = self.execute_command(c)
                self.pipe.send(r)
                r = None

                (c, wait) = self.pipe.recv()


    def get_process(self, out_file, name, connection_mode=NORMAL_MODE, dbname=""):
        """
            Gets or creates the process by the given name
        """
        if len(name) > 0 and not is_digit(name):
            raise Exception("Name should be a number")
        if len(name) > 0 and (connection_mode==SQLIsolationExecutor.NORMAL_MODE) and int(name) >= 1024:
            raise Exception("Session name should be smaller than 1024 unless it is utility or retrieve mode number")

        if not (name, connection_mode) in self.processes:
            if not dbname:
                dbname = self.dbname
            self.processes[(name, connection_mode)] = SQLIsolationExecutor.SQLConnection(out_file, name, connection_mode, dbname)
        return self.processes[(name, connection_mode)]

    def quit_process(self, out_file, name, connection_mode=NORMAL_MODE, dbname=""):
        """
        Quits a process with the given name
        """
        if len(name) > 0 and not is_digit(name):
            raise Exception("Name should be a number")
        if len(name) > 0 and (connection_mode==SQLIsolationExecutor.NORMAL_MODE) and int(name) >= 1024:
            raise Exception("Session name should be smaller than 1024 unless it is utility or retrieve mode number")

        if not (name, connection_mode) in self.processes:
            raise Exception("Sessions not started cannot be quit")

        self.processes[(name, connection_mode)].quit()
        del self.processes[(name, connection_mode)]

    def get_all_primary_contentids(self, dbname):
        """
        Retrieves all primary content IDs (including the master). Intended for
        use by *U queries.
        """
        if not dbname:
            dbname = self.dbname

        con = pygresql.pg.connect(dbname=dbname)
        result = con.query("SELECT content FROM gp_segment_configuration WHERE role = 'p'").getresult()
        if len(result) == 0:
            raise Exception("Invalid gp_segment_configuration contents")
        return [int(content[0]) for content in result]

    def process_command(self, command, output_file, global_sh_executor):
        """
            Processes the given command.
            The command at this point still includes the isolation behavior
            flags, e.g. which session to use.
        """
        process_name = ""
        sql = command
        flag = ""
        dbname = ""
        in_sh_cmd = None
        out_sh_cmd = None
        m = self.command_pattern.match(command)
        if m:
            process_name = m.groups()[0]
            flag = m.groups()[1]
            sql = m.groups()[2]
            sql = sql.lstrip()
            # If db_name is specifed , it should be of the following syntax:
            # 1:@db_name <db_name>: <sql>
            if sql.startswith('@db_name'):
                sql_parts = sql.split(':', 2)
                if not len(sql_parts) == 2:
                    raise Exception("Invalid syntax with dbname, should be of the form 1:@db_name <db_name>: <sql>")
                if not sql_parts[0].startswith('@db_name'):
                    raise Exception("Invalid syntax with dbname, should be of the form 1:@db_name <db_name>: <sql>")
                if not len(sql_parts[0].split()) == 2:
                    raise Exception("Invalid syntax with dbname, should be of the form 1:@db_name <db_name>: <sql>")
                dbname = sql_parts[0].split()[1].strip()
                if not dbname:
                    raise Exception("Invalid syntax with dbname, should be of the form 1:@db_name <db_name>: <sql>")
                sql = sql_parts[1]
            else:
                (found_hd, in_sh_cmd, ex_sql) =  global_sh_executor.extract_sh_cmd('@in_sh', sql)
                if found_hd:
                    sql = ex_sql
                else:
                    (found_hd, out_sh_cmd, ex_sql) = global_sh_executor.extract_sh_cmd('@out_sh', sql)
                    if found_hd:
                        sql = ex_sql
            
            # if set @in_sh or @out_sh
            if in_sh_cmd!=None:
                sqls=global_sh_executor.exec_gsh_4_raw_str(sql, in_sh_cmd, True)
                if(len(sqls)!=1):
                    raise Exception("Invalid shell commmand: %v", sqls)
                else:
                    sql = sqls[0]
            
            if (process_name!="" or flag!=""):
                chg_line = '%s%s: %s'%(process_name, flag, sql)
            else:
                chg_line = sql
            print >>output_file, chg_line.strip()
        else:
            print >>output_file, sql.strip()

        if not flag:
            if sql.startswith('!'):
                sql = sql[1:]

                # Check for execution mode. E.g.
                #     !\retcode path/to/executable --option1 --option2 ...
                #
                # At the moment, we only recognize the \retcode mode, which
                # ignores all program output in the diff (it's still printed)
                # and adds the return code.
                mode = None
                if sql.startswith('\\'):
                    mode, sql = sql.split(None, 1)
                    if mode != '\\retcode':
                        raise Exception('Invalid execution mode: {}'.format(mode))

                cmd_output = subprocess.Popen(sql.strip(), stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
                stdout, _ = cmd_output.communicate()
                print >> output_file
                if mode == '\\retcode':
                    print >> output_file, '-- start_ignore'
                print >> output_file, stdout
                if mode == '\\retcode':
                    print >> output_file, '-- end_ignore'
                    print >> output_file, '(exited with code {})'.format(cmd_output.returncode)
            elif sql.startswith('include:'):
                helper_file = parse_include_statement(sql)

                self.get_process(
                    output_file,
                    process_name,
                    dbname=dbname
                ).query(
                    load_helper_file(helper_file), out_sh_cmd, global_sh_executor
                )
            else:
                self.get_process(output_file, process_name, dbname=dbname).query(sql.strip(), out_sh_cmd, global_sh_executor)
        elif flag == "&":
            self.get_process(output_file, process_name, dbname=dbname).fork(sql.strip(), True)
        elif flag == ">":
            self.get_process(output_file, process_name, dbname=dbname).fork(sql.strip(), False)
        elif flag == "<":
            if len(sql) > 0:
                raise Exception("No query should be given on join")
            self.get_process(output_file, process_name, dbname=dbname).join()
        elif flag == "q":
            if len(sql) > 0:
                raise Exception("No query should be given on quit")
            self.quit_process(output_file, process_name, dbname=dbname)
        elif flag == "U":
            if process_name == '*':
                process_names = [str(content) for content in self.get_all_primary_contentids(dbname)]
            else:
                process_names = [process_name]

            for name in process_names:
                self.get_process(output_file, name, connection_mode=SQLIsolationExecutor.UTILITY_MODE, dbname=dbname).query(sql.strip(), out_sh_cmd)
        elif flag == "U&":
            self.get_process(output_file, process_name, connection_mode=SQLIsolationExecutor.UTILITY_MODE, dbname=dbname).fork(sql.strip(), True)
        elif flag == "U<":
            if len(sql) > 0:
                raise Exception("No query should be given on join")
            self.get_process(output_file, process_name, connection_mode=SQLIsolationExecutor.UTILITY_MODE, dbname=dbname).join()
        elif flag == "Uq":
            if len(sql) > 0:
                raise Exception("No query should be given on quit")
            self.quit_process(output_file, process_name, connection_mode=SQLIsolationExecutor.UTILITY_MODE, dbname=dbname)
        elif flag == "R":
            if process_name == '*':
                process_names = [str(content) for content in self.get_all_primary_contentids(dbname)]
            else:
                process_names = [process_name]

            for name in process_names:
                self.get_process(output_file, name, connection_mode=SQLIsolationExecutor.RETRIEVE_MODE, dbname=dbname).query(sql.strip(), out_sh_cmd, global_sh_executor)
        elif flag == "R&":
            self.get_process(output_file, process_name, connection_mode=SQLIsolationExecutor.RETRIEVE_MODE, dbname=dbname).fork(sql.strip(), True)
        elif flag == "R<":
            if len(sql) > 0:
                raise Exception("No query should be given on join")
            self.get_process(output_file, process_name, connection_mode=SQLIsolationExecutor.RETRIEVE_MODE, dbname=dbname).join()
        elif flag == "Rq":
            if len(sql) > 0:
                raise Exception("No query should be given on quit")
            self.quit_process(output_file, process_name, connection_mode=SQLIsolationExecutor.RETRIEVE_MODE, dbname=dbname)
        else:
            raise Exception("Invalid isolation flag")

    def process_isolation_file(self, sql_file, output_file, initfile_prefix):
        """
            Processes the given sql file and writes the output
            to output file
        """
        shell_executor = GlobalShellExecutor(output_file, initfile_prefix)
        try:
            command = ""

            for line in sql_file:
                #tinctest.logger.info("re.match: %s" %re.match(r"^\d+[q\\<]:$", line))
                if line[0] == "!":
                    command_part = line # shell commands can use -- for multichar options like --include
                else:
                    command_part = line.partition("--")[0] # remove comment from line
                if command_part == "" or command_part == "\n":
                    print >>output_file, line.strip(),
                    print >>output_file
                elif re.match(r".*;\s*$", line) or re.match(r"^\d+[q\\<]:\s*$", line) or re.match(r"^-?\d+[UR][q\\<]:\s*$", line):
                    command += command_part
                    try:
                        self.process_command(command, output_file, shell_executor)
                    except Exception as e:
                        print >>output_file, "FAILED: ", e
                    command = ""
                else:
                    command += command_part

            for process in self.processes.values():
                process.stop()
        except:
            for process in self.processes.values():
                process.terminate()
            shell_executor.terminate()
            raise
        finally:
            for process in self.processes.values():
                process.terminate()
            shell_executor.terminate()

class SQLIsolationTestCase:
    """
        The isolation test case allows a fine grained control of interleaved
        executing transactions. This is mainly used to test isolation behavior.

        [<#>[flag]:] <sql> | ! <shell scripts or command>
        #: either an integer indicating a unique session, or a content-id if
           followed by U (for utility-mode connections) or R (for retrieve-mode
           connection). In 'U' mode or 'R' mode, the
           content-id can alternatively be an asterisk '*' to perform a
           utility-mode query on the master and all primaries.
        flag:
            &: expect blocking behavior
            >: running in background without blocking
            <: join an existing session
            q: quit the given session

            U: connect in utility mode to primary contentid from gp_segment_configuration
            U&: expect blocking behavior in utility mode (does not currently support an asterisk target)
            U<: join an existing utility mode session (does not currently support an asterisk target)
            I: include a file of sql statements (useful for loading reusable functions)

            R|R&|R<: similar to 'U' meaning execept that the connect is in retrieve mode, here don't
               thinking about retrieve mode authentication, just using the normal authentication directly.

        An example is:

        Execute BEGIN in transaction 1
        Execute BEGIN in transaction 2
        Execute INSERT in transaction 2
        Execute SELECT in transaction 1
        Execute COMMIT in transaction 2
        Execute SELECT in transaction 1

        The isolation tests are specified identical to sql-scripts in normal
        SQLTestCases. However, it is possible to prefix a SQL line with
        an tranaction identifier followed by a colon (":").
        The above example would be defined by
        1: BEGIN;
        2: BEGIN;
        2: INSERT INTO a VALUES (1);
        1: SELECT * FROM a;
        2: COMMIT;
        1: SELECT * FROM a;

        Blocking behavior can be tested by forking and joining.
        1: BEGIN;
        2: BEGIN;
        1: DELETE FROM foo WHERE a = 4;
        2&: DELETE FROM foo WHERE a = 4;
        1: COMMIT;
        2<:
        2: COMMIT;

        2& forks the command. It is executed in the background. If the
        command is NOT blocking at this point, it is considered an error.
        2< joins the background command and outputs the result of the   
        command execution.

        Session ids should be smaller than 1024.

        2U: Executes a utility command connected to port 40000. 

        One difference to SQLTestCase is the output of INSERT.
        SQLTestCase would output "INSERT 0 1" if one tuple is inserted.
        SQLIsolationTestCase would output "INSERT 1". As the
        SQLIsolationTestCase needs to have a more fine-grained control
        over the execution order than possible with PSQL, it uses
        the pygresql python library instead.

        Connecting to a specific database:
        1. If you specify a db_name metadata in the sql file, connect to that database in all open sessions.
        2. If you want a specific session to be connected to a specific database , specify the sql as follows:

        1:@db_name testdb: <sql>
        2:@db_name test2db: <sql>
        1: <sql>
        2: <sql>
        etc

        Here session 1 will be connected to testdb and session 2 will be connected to test2db. You can specify @db_name only at the beginning of the session. For eg:, following would error out:

        1:@db_name testdb: <sql>
        2:@db_name test2db: <sql>
        1: @db_name testdb: <sql>
        2: <sql>
        etc

        Quitting sessions:
        By default, all opened sessions will be stopped only at the end of the sql file execution. If you want to explicitly quit a session
        in the middle of the test execution, you can specify a flag 'q' with the session identifier. For eg:

        1:@db_name testdb: <sql>
        2:@db_name test2db: <sql>
        1: <sql>
        2: <sql>
        1q:
        2: <sql>
        3: <sql>
        2q:
        3: <sql>
        2: @db_name test: <sql>

        1q:  ---> Will quit the session established with testdb.
        2q:  ---> Will quit the session established with test2db.

        The subsequent 2: @db_name test: <sql> will open a new session with the database test and execute the sql against that session.

        Shell Execution for SQL or Output:

        @in_sh can be used for executing shell command to change input (i.e. each SQL statement) or get input info;
        @out_sh can be used for executing shell command to change ouput (i.e. the result set printed for each SQL execution)
        or get output info. Just use the env variable ${RAW_STR} to refer to the input/out stream before shell execution,
        and the output of the shell commmand will be used as the SQL exeucted or output printed into results file.

        1: @out_sh ' TOKEN1=` echo "${RAW_STR}" | awk \'NR==3\' | awk \'{print $1}\'` && export MATCHSUBS="${MATCHSUBS}${NL}m/${TOKEN1}/${NL}s/${TOKEN1}/token_id1/${NL}" && echo "${RAW_STR}" ': SELECT token,hostname,status FROM GP_ENDPOINTS WHERE cursorname='c1';
        2R: @in_sh ' echo "${RAW_STR}" | sed "s#@TOKEN1#${TOKEN1}#" ': RETRIEVE ALL FROM "@TOKEN1";

        These 2 sample is to:
        - Sample 1: set env variable ${TOKEN1} to the cell (row 3, col 1) of the result set, and print the raw result. 
          The env var ${MATCHSUBS} is used to store the matchsubs section so that we can store it into initfile when 
          this test case file is finished executing.
        - Sample 2: replaceing "@TOKEN1" by generated token which is fetch in sample1

        Catalog Modification:

        Some tests are easier to write if it's possible to modify a system
        catalog across the *entire* cluster. To perform a utility-mode query on
        all segments and the master, you can use *U commands:

        *U: SET allow_system_table_mods = true;
        *U: UPDATE pg_catalog.<table> SET <column> = <value> WHERE <cond>;

        Since the number of query results returned by a *U command depends on
        the developer's cluster configuration, it can be useful to wrap them in
        a start_/end_ignore block. (Unfortunately, this also hides legitimate
        failures; a better long-term solution is needed.)

        Block/join flags are not currently supported with *U.

        Including files:

        -- example contents for file.sql: create function some_test_function() returning void ...
        include: path/to/some/file.sql;
        select some_helper_function();
    """

    def run_sql_file(self, sql_file, out_file = None, out_dir = None, optimizer = None):
        """
        Given a sql file and an ans file, this adds the specified gucs (self.gucs) to the sql file , runs the sql
        against the test case database (self.db_name) and verifies the output with the ans file.
        If an 'init_file' exists in the same location as the sql_file, this will be used
        while doing gpdiff.
        """
        # Add gucs to the test sql and form the actual sql file to be run
        if not out_dir:
            out_dir = self.get_out_dir()
            
        if not os.path.exists(out_dir):
            TINCSystem.make_dirs(out_dir, ignore_exists_error = True)
            
        if optimizer is None:
            gucs_sql_file = os.path.join(out_dir, os.path.basename(sql_file))
        else:
            # sql file will be <basename>_opt.sql or <basename>_planner.sql based on optimizer
            gucs_sql_file = os.path.join(out_dir, os.path.basename(sql_file).replace('.sql', '_%s.sql' %self._optimizer_suffix(optimizer)))
            
        self._add_gucs_to_sql_file(sql_file, gucs_sql_file, optimizer)
        self.test_artifacts.append(gucs_sql_file)

        
        if not out_file:
            if optimizer is None:
                out_file = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '.out'))
            else:
                # out file will be *_opt.out or *_planner.out based on optimizer
                out_file = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '_%s.out' %self._optimizer_suffix(optimizer)))
        
        self.test_artifacts.append(out_file)
        executor = SQLIsolationExecutor(dbname=self.db_name)
        with open(out_file, "w") as f:
            executor.process_isolation_file(open(sql_file), f, out_file)
            f.flush()   
        
        if out_file[-2:] == '.t':
            out_file = out_file[:-2]

        return out_file

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--dbname", dest="dbname",
                      help="connect to database DBNAME", metavar="DBNAME")
    parser.add_option("--initfile_prefix", dest="initfile_prefix",
                      help="The file path prefix for automatically generated initfile", metavar="INITFILE_PREFIX")
    (options, args) = parser.parse_args()

    executor = SQLIsolationExecutor(dbname=options.dbname)

    executor.process_isolation_file(sys.stdin, sys.stdout, options.initfile_prefix)