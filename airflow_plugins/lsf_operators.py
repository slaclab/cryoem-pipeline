from airflow.plugins_manager import AirflowPlugin

from airflow import configuration
from airflow.models import BaseOperator
# from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator, SSHTempFileContent
# from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.utils.decorators import apply_defaults

from builtins import bytes
import re
from datetime import datetime
import dateutil
from time import sleep

from base64 import b64encode
from select import select


import logging

LOG = logging.getLogger(__name__)


DEFAULT_BSUB='/afs/slac/package/lsf/curr/bin/bsub'
DEFAULT_BJOBS='/afs/slac/package/lsf/curr/bin/bjobs'
DEFAULT_BKILL='/afs/slac/package/lsf/curr/bin/bkill'
DEFAULT_QUEUE_NAME='cryoem-daq'

# copy from airflow/contrib/operators/ssh_operator - need to add env
class SSHOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.
    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ingored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param command: command to execute on remote host. (templated)
    :type command: str
    :param timeout: timeout (in seconds) for executing the command.
    :type timeout: int
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    """

    template_fields = ('command', 'remote_host')
    template_ext = ('.sh',)

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 command=None,
                 timeout=10,
                 do_xcom_push=False,
                 env={},
                 *args,
                 **kwargs):
        super(SSHOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.command = command
        self.timeout = timeout
        self.do_xcom_push = do_xcom_push
        self.env = env

    def execute(self, context):
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id,
                                            timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info("remote_host is provided explicitly. " +
                              "It will replace the remote_host which was defined " +
                              "in ssh_hook or predefined in connection of ssh_conn_id.")
                self.ssh_hook.remote_host = self.remote_host

            if not self.command:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                # Auto apply tty when its required in case of sudo
                get_pty = False
                if self.command.startswith('sudo'):
                    get_pty = True

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(command=self.command,
                                                                get_pty=get_pty,
                                                                timeout=self.timeout,
                                                                environment=self.env
                                                                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or \
                        channel.recv_ready() or \
                        channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.timeout)
                    for c in readq:
                        if c.recv_ready():
                            line = stdout.channel.recv(len(c.in_buffer))
                            line = line
                            agg_stdout += line
                            self.log.info(line.decode('utf-8').strip('\n'))
                        if c.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                            line = line
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8').strip('\n'))
                    if stdout.channel.exit_status_ready()\
                            and not stderr.channel.recv_stderr_ready()\
                            and not stdout.channel.recv_ready():
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status is 0:
                    # returning output if do_xcom_push is set
                    if self.do_xcom_push:
                        enable_pickling = configuration.conf.getboolean(
                            'core', 'enable_xcom_pickling'
                        )
                        if enable_pickling:
                            return agg_stdout
                        else:
                            return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException("error running cmd: {0}, error: {1}"
                                           .format(self.command, error_msg))

        except Exception as e:
            raise AirflowException("SSH operator error: {0}".format(str(e)))

        return True

    def tunnel(self):
        ssh_client = self.ssh_hook.get_conn()
        ssh_client.get_transport()



class BaseSSHOperator(SSHOperator):
    template_fields = ("bash_command", "env",)
    template_ext = (".sh", ".bash",)
    
    def __init__(self, *args, **kwargs):
        super( BaseSSHOperator, self ).__init__( do_xcom_push=True, *args, **kwargs )

    def get_command(self,context):
        return self.bash_command
    
    def execute(self, context):
        self.command = self.get_command(context)
        logging.info("Command: %s" % (self.command,))
        data = super( BaseSSHOperator, self ).execute( context ).decode("utf-8")
        return self.parse_output(context,data)

    def parse_output(self,context,text):
        logging.info("Output:")
        logging.info(text)
        return text
        # for line in iter(text):
        #     line = line.decode().strip()
        

class LSFSubmitOperator(BaseSSHOperator):
    """ Submit a job asynchronously into LSF and return the jobid via xcom return_value """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#0088aa'

    @apply_defaults
    def __init__(self,
                 lsf_script="",
                 ssh_conn_id=None,
                 bsub=DEFAULT_BSUB,
                 bsub_args='',
                 queue_name=DEFAULT_QUEUE_NAME,
                 *args, **kwargs):
        self.bsub = bsub
        self.bsub_args = bsub_args
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        super(LSFSubmitOperator, self).__init__(*args, **kwargs)

    def get_command(self, context):
        name = '%s__%s' % ( context['run_id'], context['task'].task_id )
        return self.bsub + \
            ' -cwd "/tmp" ' + \
            ' -q %s ' % (self.queue_name,) + \
            ' -J %s ' % name.replace('[','_').replace(']','_').replace(' ','_') + \
            ' %s ' % (self.bsub_args,) + \
            " <<-'__LSF_EOF__'\n" + \
            self.lsf_script + "\n" + \
            '__LSF_EOF__\n'

    def parse_output(self,context,text):
        logging.info("LSF Submit Output:")
        logging.info(text)
        for line in text.split('\n'):
            logging.info(line)
            m = re.search( r'^Job \<(?P<jobid>\d+)\> is submitted to queue', line )
            if m:
                d = m.groupdict()
                return d
        return None
#
# class BaseSSHSensor(BaseSSHOperator):
#     """ sensor via executing an ssh command """
#     def __init__(self,
#                  ssh_hook,
#                  bash_command,
#                  xcom_push=False,
#                  poke_interval=10,
#                  timeout=60*60,
#                  soft_fail=False,
#                  env=None,
#                  *args, **kwargs):
#         super(BaseSSHOperator, self).__init__(*args, **kwargs)
#         self.bash_command = bash_command
#         self.env = env
#         self.hook = ssh_hook
#         self.xcom_push = xcom_push
#         self.poke_interval = poke_interval
#         self.soft_fail = soft_fail
#         self.timeout = timeout
#         self.prevent_returncode = None
#
#     def execute(self, context, bash_command_function='get_bash_command'):
#         func = getattr(self, bash_command_function)
#         bash_command = func(context)
#         host = self.hook._host_ref()
#         started_at = datetime.now()
#         with SSHTempFileContent(self.hook,
#                                 bash_command,
#                                 self.task_id) as remote_file_path:
#             logging.info("Temporary script "
#                          "location : {0}:{1}".format(host, remote_file_path))
#
#             while not self.poke_output(self.hook, context, remote_file_path):
#                 if (datetime.now() - started_at).total_seconds() > self.timeout:
#                     if self.soft_fail:
#                         raise AirflowSkipException('Snap. Time is OUT.')
#                     else:
#                         raise AirflowSensorTimeout('Snap. Time is OUT.')
#                 sleep(self.poke_interval)
#             logging.info("Success criteria met. Exiting.")
#
#
#     def poke_output(self, hook, context, remote_file_path):
#
#         sp = hook.Popen(
#             ['-q', 'bash', remote_file_path],
#             stdout=subprocess.PIPE, stderr=STDOUT,
#             env=self.env)
#         self.sp = sp
#
#         result = self.poke(context,sp)
#
#         sp.wait()
#         logging.info("Command exited with "
#                      "return code {0}".format(sp.returncode))
#         # LOG.info("PREVENT RETURNCODE: %s" % (self.prevent_returncode,))
#         if sp.returncode and not self.prevent_returncode:
#             raise AirflowException("Bash command failed: %s" % (sp.returncode,))
#
#         return result
#
#     def poke( self, context, sp ):
#         raise AirflowException('Override me.')
#

class BaseSSHSensor(BaseSSHOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.
    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ingored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param command: command to execute on remote host. (templated)
    :type command: str
    :param timeout: timeout (in seconds) for executing the command.
    :type timeout: int
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    """

    template_fields = ('command', 'remote_host')
    template_ext = ('.sh',)

    @apply_defaults
    def __init__(self,
                 poke_interval=10,
                 timeout=600,
                 *args,
                 **kwargs):
        super(BaseSSHOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout

    def execute(self, context, bash_command_function='get_bash_command'):
        func = getattr(self, bash_command_function)
        self.command = func(context)
        data = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info("ssh_hook is not provided or invalid. " +
                                  "Trying ssh_conn_id to create SSHHook.")
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id,
                                            timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info("remote_host is provided explicitly. " +
                              "It will replace the remote_host which was defined " +
                              "in ssh_hook or predefined in connection of ssh_conn_id.")
                self.ssh_hook.remote_host = self.remote_host

            if not self.command:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                # Auto apply tty when its required in case of sudo
                get_pty = False
                if self.command.startswith('sudo'):
                    get_pty = True

                started_at = datetime.now()
                state = False
                while state == False:
                    state, data = self.poke_command( context, ssh_client, get_pty=get_pty )
                    LOG.info( "STATE: %s, DATA: %s (timeout %s)" % (state,data,self.timeout))
                    if (datetime.now() - started_at).total_seconds() > self.timeout:
                        #if self.soft_fail:
                        #    raise AirflowSkipException('Snap. Time is OUT.')
                        #else:
                        raise AirflowSensorTimeout('Snap. Time is OUT.')
                    sleep(self.poke_interval)
                logging.info("Success criteria met. Exiting.")

        except Exception as e:
            raise AirflowException("SSH operator error: {0}".format(str(e)))

        return data

    def tunnel(self):
        ssh_client = self.ssh_hook.get_conn()
        ssh_client.get_transport()

    def poke_command(self, context, ssh_client, get_pty=False ):

        logging.info("poking... %s" % (self.command,))

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(command=self.command,
                                                        get_pty=get_pty,
                                                        timeout=self.timeout,
                                                        environment=self.env
                                                        )
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b''
        agg_stderr = b''

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        # read from both stdout and stderr
        while not channel.closed or \
                channel.recv_ready() or \
                channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], self.timeout)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    self.log.info(line.decode('utf-8').strip('\n'))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    self.log.warning(line.decode('utf-8').strip('\n'))
            if stdout.channel.exit_status_ready()\
                    and not stderr.channel.recv_stderr_ready()\
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        exit_status = stdout.channel.recv_exit_status()
        if exit_status is 0:

            return self.poke( context, agg_stdout )

        else:
            error_msg = agg_stderr.decode('utf-8')
            raise AirflowException("error running cmd: {0}, error: {1}"
                                   .format(self.command, error_msg))

    def poke( self, context, text ):
        """ return whether command succeeded or not based on the output of text """
        raise AirflowException('Override me.')
        # return state, dict

class LSFJobSensor(BaseSSHSensor):
    """ waits for a the given lsf job to complete and supply results via xcom """
    template_fields = ("jobid",)
    ui_color = '#006699'

    def __init__(self,
                 jobid=None,
                 bjobs=DEFAULT_BJOBS,
                 bkill=DEFAULT_BKILL,
                 *args, **kwargs):
        self.jobid = jobid
        self.bjobs = bjobs
        self.bkill = bkill
        self.prevent_returncode = None
        super(LSFJobSensor, self).__init__(*args, **kwargs)

    def get_bash_command(self, context):
        return self.bjobs + ' -l ' + self.jobid

    def poke( self, context, text ):
        LOG.info('Querying LSF job %s' % (self.jobid,))
        info = {}
        cmd_output = []
        concat = False
        for line in text.decode('utf-8').split('\n'):
            if line.startswith('Job '): # concat
                concat = True
            if concat:
                if len(cmd_output) == 0:
                    cmd_output.append("")
                cmd_output[-1] = cmd_output[-1] + line.lstrip()
                if line.endswith('>'):
                    concat = False
            else:
                cmd_output.append(line.lstrip())
        for line in cmd_output:
            # LOG.info(line)
            if '<DONE>' in line:
                info['status'] = 'DONE'
            elif '<EXIT>' in line:
                info['status'] = 'EXIT'
            elif '<PEND>' in line:
                info['status'] = 'PEND'
            elif ' Submitted from host' in line:
                dt, _ = line.split(': ')
                info['submitted_at'] = dateutil.parser.parse( dt )
            elif ' Started on ' in line:
                m = re.search( '^(?P<dt>.*): Started on .*\<(\d+\*)?(?P<host>.*)\>, Execution Home ', line )
                if m:
                    d = m.groupdict()
                    info['started_at'] = dateutil.parser.parse( d['dt'] )
                    info['host'] = d['host']
            elif ' The CPU time used is ' in line:
                m = re.search( '^(?P<dt>.*)\: .*\. The CPU time used is (?P<duration>.*)\.', line )
                if m:
                    d = m.groupdict()
                    info['finished_at']= dateutil.parser.parse( d['dt'])
                    info['duration'] = d['duration']
                if ' Done successfully. ' in line:
                    info['status'] = 'DONE'

            # upstream failure
            elif 'Dependency condition invalid or never satisfied' in line or '<UNKWN>' in line:
                # TODO run bkill to remove job from queue and report back error
                raise AirflowException('Job dependency condition invalid or never satisfied')

        #LOG.info("FOUND: %s" % (info,))
        if 'status' in info:

            # stupid bjobs may sometimes exit even tho the job exists
            # so if we've previously found the job, don't get bjobs exit
            self.prevent_returncode = True
            if 'submitted_at' and 'started_at' in info:
                info['inertia'] = info['started_at'] - info['submitted_at']
            if 'finished_at' and 'started_at' in info:
                info['runtime'] = info['finished_at'] - info['started_at']
            if not 'started_at' in info and 'duration' in info:
                info['runtime'] = info['duration'].split(' seconds')[0]

            if info['status'] == 'DONE' and 'runtime' in info:
                #LOG.info("PUSHING pickle True, %s" % (info,))
                return True, info
            elif info['status'] == 'EXIT':
                # TODO: bpeek? write std/stderr?
                # context['ti'].xcom_push( key='return_value', value=info )
                raise AirflowException('Job EXITed')

        return False, {}


class LSFOperator(LSFSubmitOperator,LSFJobSensor):
    """ Submit a job into LSF wait for the job to finish """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#006699'

    @apply_defaults
    def __init__(self,
                 lsf_script=None,
                 queue_name=DEFAULT_QUEUE_NAME,
                 bsub=DEFAULT_BSUB,
                 bsub_args='',
                 bjobs=DEFAULT_BJOBS,
                 bkill=DEFAULT_BKILL,
                 poke_interval=10,
                 timeout=60*60,
                 soft_fail=False,
                 *args, **kwargs):
        BaseSSHOperator.__init__( self, *args, **kwargs)
        self.bsub = bsub
        self.bsub_args = bsub_args
        self.bjobs = bjobs
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        self.jobid = None
        self.timeout = timeout
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.prevent_returncode = None

    def get_status_command(self, context):
        return LSFJobSensor.get_bash_command(self,context)

    def execute(self, context):
        logging.info("Executing...")
        d = LSFSubmitOperator.execute(self, context)
        logging.info("GOT: %s" % (d,))
        if not 'jobid' in d:
            raise AirflowException("Could not determine jobid")
        self.jobid = d['jobid']
        return LSFJobSensor.execute(self, context, bash_command_function='get_status_command')




class LSFPlugin(AirflowPlugin):
    name = 'lsf_plugin'
    # operators = [LSFSubmitOperator,LSFJobSensor,LSFOperator]
    operators = [LSFSubmitOperator,LSFJobSensor,LSFOperator,BaseSSHOperator]
