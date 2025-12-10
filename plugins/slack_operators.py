
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.slack_operator import SlackAPIOperator, SlackAPIPostOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from slackclient import SlackClient

import yaml
import ast
import os
import logging

LOG = logging.getLogger(__name__)



class SlackAPIEnsureChannelOperator(SlackAPIOperator):
    template_fields = ('channel',)
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 *args, **kwargs):
        self.method = 'conversations.create'
        self.channel = channel
        super(SlackAPIEnsureChannelOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'name': self.channel,
            #'validate': True,
            'is_private': True,
        }

    def execute(self, context):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        if not rc['ok']:
            if not rc['error'] == 'name_taken':
                logging.error("Slack API call failed ({})".format(rc['error']))
                raise AirflowException("Slack API call failed: ({})".format(rc['error']))
            # get the id
        #rc = sc.api_call('conversations.list', **{ **self.api_params, **{'exclude_archived': 1} } )
        #LOG.info("RC: %s" % (rc,))
        #group = list( filter( lambda d: d['name'] in [ self.channel.lower(), ], rc['channels'] ) )[0]
        LOG.info(f"create {self.channel.lower()}: {rc}")
        
        channels = []
        next_cursor = ""
        while True:
            rc = sc.api_call('conversations.list', exclude_archived=True, limit=1000, types='private_channel', cursor=next_cursor ) 
            if 'channels' in rc:
                channels = channels + rc['channels']
        
            if 'response_metadata' in rc:
                if 'next_cursor' in rc['response_metadata']:
                    next_cursor = rc['response_metadata']['next_cursor']
                    #LOG.info(f"next_cursor: {next_cursor}")
                    #if next_cursor == "":
                    #    LOG.info(f"empty next_cursor: {next_cursor}")
                else:
                    next_cursor = ""
                    #LOG.info(f"no next_cursor in response_metadata: {next_cursor}")
            else:
                next_cursor = ""
                #LOG.info(f"no response_metadata, next_cursor: {next_cursor}")
                
            if next_cursor == "":
                break
        
        LOG.info(f"number of channels: {len(channels)}")
        group = list( filter( lambda d: d['name'] in [ self.channel.lower(), ], channels ) )
        #rc = sc.api_call('conversations.list', exclude_archived=True, limit=1000, types='private_channel' ) 
        #group = list( filter( lambda d: d['name'] in [ self.channel.lower(), ], rc['channels'] ) )
        conv_id = ""
        if len(group) > 0:
            conv_id = group[0]['id']
        LOG.info(f"CONV {conv_id}")
        #mc = sc.api_call('conversations.members', channel=conv_id )
        #members = mc['members']
       # LOG.info(f"MEMBERS: {members}")
        context['task_instance'].xcom_push( key='return_value', value={
            'group_id': conv_id,
            # 'members': members 
        } )

def user_to_slack_id( user, mapping={} ):
    try:
        user = int(user)
        if user in mapping:
            return mapping[user]['slackid']
    except:
        pass
    # is already a slack user id
    #if user.startswith('W'):
    #    return user
    return None

        
class SlackAPIInviteToChannelOperator(SlackAPIOperator):
    template_fields = ('channel','users')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 users=(),
                 usermap_file='/usr/local/airflow/slack_users.yaml',
                 default_users=None,
                 *args, **kwargs):
        self.method = 'conversations.invite'
        self.channel = channel
        self.users = users #ast.literal_eval(users)
        self.default_users = default_users.split(',')
        self.usermap_file = usermap_file
        super(SlackAPIInviteToChannelOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self, channel_id=None ):
        self.api_params = {
            'channel': channel_id,
        } 

    def execute(self, context):
        logging.info(f"self method: {self.method}")
        logging.info(f"self channel: {self.channel}")
        current = context['task_instance'].xcom_pull( task_ids='slack_channel' )
        if not self.api_params:
            self.construct_api_call_params( channel_id=current['group_id'] )
        sc = SlackClient(self.token)
        these = ast.literal_eval( "%s" % (self.users,) )
        prev = these
        mapping = {}
        with open( self.usermap_file, 'r' ) as f:
            mapping = yaml.load( f, Loader=yaml.FullLoader)
        #these = [ user_to_slack_id(u,mapping) for u in these + self.default_users if not u in current['members'] ] 
        these = [ user_to_slack_id(u,mapping) for u in these ]
        logging.info(f"default members: {self.default_users}")

        members = []
        mc = sc.api_call('conversations.members', channel=current['group_id'] )
        if 'members' in mc:
            members = mc['members']
        logging.info(f"current members: {members}")

        for d in self.default_users:
            if not d in members:
                these.append( d )
        unmapped = []
        for n,u in enumerate(these):
            if u == None:
                unmapped.append( { prev[n], these[n] } )
        logging.info("mapped users: %s -> %s" % (prev,these,))
        users = list( filter( None, these ) )
        diff = list( set(users) - set(members) )
        logging.info(f"diff: {diff}")
        if len(diff) > 0:
            rc = sc.api_call('conversations.invite', channel=current['group_id'], users=','.join(diff) )
            #rc = sc.api_call('conversations.invite', channel=current['group_id'], users=','.join(users) )
            if not rc['ok']:
                raise AirflowException( f"Slack API add user failed: {rc}" )
        if len( unmapped ) > 0:
            raise AirflowException( f"Unmapped users {unmapped}" )
        if len(diff) == 0:
            raise AirflowSkipException( f"No actions required" )


class SlackAPIUploadFileOperator(SlackAPIOperator):
    template_fields = ('channel','filepath')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 filepath=None,
                 *args, **kwargs):
        self.method = 'files.upload'
        self.channel = channel
        self.filepath = filepath
        super(SlackAPIUploadFileOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        title = os.path.basename(self.filepath)
        self.api_params = {
            'channels': self.channel,
            'filename': title,
            'title': title,
        }

    def execute(self, **kwargs):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        with open( self.filepath, 'rb' ) as f:
            self.api_params['file'] = f
            rc = sc.api_call(self.method, **self.api_params)
            logging.info("sending: %s" % (self.api_params,))
            if not rc['ok']:
                logging.error("Slack API call failed {}".format(rc['error']))
                raise AirflowException("Slack API call failed: {}".format(rc['error']))




class SlackPlugin(AirflowPlugin):
    name = 'slack_plugin'
    operators = [ SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator ]
