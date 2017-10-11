#!/usr/bin/env python

import os, sys
import pgpubsub
from snap import snap, common
from snap import cli_tools as cli
from code_templates import *
from metaobjects import *
import logging
import jinja2
import json
from cmd import Cmd
from docopt import docopt as docopt_func
from docopt import DocoptExit



SUPPORTED_DB_OPS = ['INSERT', 'UPDATE']

OPERATION_OPTIONS = [{'value': 'INSERT', 'label': 'INSERT'}, {'value': 'UPDATE', 'label': 'UPDATE'}]


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('eavesdroppr')


class NoSuchEventChannel(Exception):
    def __init__(self, channel_id):
        Exception.__init__(self,
                           'No event channel registered under the name "%s". Please check your initfile.' \
                           % channel_id)


class NoSuchEventHandler(Exception):
    def __init__(self, handler_func_name, handler_module):
        Exception.__init__(self,
                           'No event handler function "%s" exists in event handler module "%s".' \
                           % (handler_func_name, handler_module))


class UnsupportedDBOperation(Exception):
    def __init__(self, operation):
        Exception.__init__(self, 'The database operation "%s" is not supported.' % operation)



def docopt_cmd(func):
    """
    This decorator is used to simplify the try/except block and pass the result
    of the docopt parsing to the called action.
    """
    def fn(self, arg):
        try:
            opt = docopt_func(fn.__doc__, arg)

        except DocoptExit as e:
            # The DocoptExit is thrown when the args do not match.
            # We print a message to the user and the usage block.

            print '\nPlease specify one or more valid command parameters.'
            print e
            return

        except SystemExit:
            # The SystemExit exception prints the usage for --help
            # We do not need to do the print here.

            return

        return func(self, opt)

    fn.__name__ = func.__name__
    fn.__doc__ = func.__doc__
    fn.__dict__.update(func.__dict__)
    return fn


def default_proc_name(table_name, operation):
    return '%s_%s_notify' % (table_name, operation.lower())


def default_trigger_name(table_name, operation):
    return 'trg_%s_%s' % (table_name, operation.lower())


def generate_code(event_channel, channel_config, **kwargs):
    operation = channel_config['db_operation']
    if not operation in SUPPORTED_DB_OPS:
        raise eavesdroppr.UnsupportedDBOperation(operation)

    table_name = channel_config['db_table_name']
    db_schema = channel_config.get('db_schema') or 'public'
    procedure_name = channel_config.get('db_proc_name') or default_proc_name(table_name, operation)
    trigger_name = channel_config.get('db_trigger_name') or default_trigger_name(table_name, operation)
    source_fields = channel_config['payload_fields']
    primary_key_field = channel_config['pk_field_name']
    primary_key_type = channel_config['pk_field_type']

    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)
    json_func_template = j2env.from_string(JSON_BUILD_FUNC_TEMPLATE)
    json_func = json_func_template.render(payload_fields=source_fields,
                                          pk_field=primary_key_field)

    if kwargs['procedure']:
        print PROC_TEMPLATE.format(schema=db_schema,
                                   proc_name=procedure_name,
                                   pk_field_name=primary_key_field,
                                   pk_field_type=primary_key_type,
                                   channel_name=event_channel,
                                   json_build_func=json_func)

    elif kwargs['trigger']:
        print TRIGGER_TEMPLATE.format(schema=db_schema,
                                      table_name=table_name,
                                      trigger_name=trigger_name,
                                      db_proc_name=procedure_name,
                                      db_op=operation)


def default_event_handler(event, svc_object_registry):
    print common.jsonpretty(json.loads(event.payload))
    


def listen(channel_id, yaml_config, **kwargs):
    local_env = common.LocalEnvironment('PGSQL_USER', 'PGSQL_PASSWORD')
    local_env.init()

    pgsql_user = local_env.get_variable('PGSQL_USER')
    pgsql_password = local_env.get_variable('PGSQL_PASSWORD')
    db_host = yaml_config['globals']['database_host']
    db_name = yaml_config['globals']['database_name']

    pubsub = pgpubsub.connect(host=db_host,
                              user=pgsql_user,
                              password=pgsql_password,
                              database=db_name)
    handler_module_name = yaml_config['globals']['handler_module']

    project_dir = common.load_config_var(yaml_config['globals']['project_directory'])
    sys.path.append(project_dir)
    handlers = __import__(handler_module_name)
    handler_function_name = yaml_config['channels'][channel_id].get('handler_function') or 'default_handler'

    if handler_function_name != 'default_handler':
        if not hasattr(handlers, handler_function_name):
            raise NoSuchEventHandler(handler_function_name, handler_module_name)

        handler_function = getattr(handlers, handler_function_name)
    else:
        handler_function = default_event_handler

    service_objects = common.ServiceObjectRegistry(snap.initialize_services(yaml_config, logger))

    pubsub.listen(channel_id)
    print 'listening on channel "%s"...' % channel_id
    for event in pubsub.events():
        handler_function(event, service_objects)



class EavesdropCLI(Cmd):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader(*[])
        kwreader.read(**kwargs)
        Cmd.__init__(self)
        self.prompt = '[eavesdrop_cli]> '

        globals = kwreader.get_value('globals') or {}
        self.global_settings = GlobalSettingsMeta(**globals)
        self.channels = kwreader.get_value('channels') or []
        self.service_objects = kwreader.get_value('service_objects') or []

    

    def create_channel(self, channel_name, **kwargs):
        print 'stub create channel function'
        print common.jsonpretty(kwargs)
        return ChannelMeta(channel_name, **kwargs)


    def prompt_for_payload_fields(self, channel_name):
        print '+++ adding payload fields'
        fields = []
        while True:
            new_field = cli.InputPrompt('input field name').show()
            if new_field:
                fields.append(new_field)
                should_continue = cli.InputPrompt('add another [Y/n]?', 'y').show()
                if should_continue == 'y':
                    continue 
            break

        print '+++ payload fields:\n-%s \n+++ added to event channel "%s".' % ('\n-'.join(fields), channel_name)
        return fields


    def do_quit(self, arg):
        print 'eavesdrop interactive mode exiting.'
        raise SystemExit


    do_q = do_quit
    do_exit = do_quit


    @docopt_cmd
    def do_globals(self, arg):
        '''Usage:
                    globals [update]
                    globals set <setting_name>
                    globals set <setting_name> <setting_value>
        '''

        setting_name = arg.get('<setting_name>')
        if arg['update']:
            self.edit_global_settings()
        elif arg['set']:
            value = arg.get('<setting_value>')

            if value is None:
                self.edit_global_setting(setting_name)
            else:
                if not setting_name in self.global_settings.data().keys():
                    print "Available global settings are: "
                    print '\n'.join(['- %s' % (k) for k in self.global_settings.data().keys()])
                    return

                attr_name = 'set_%s' % setting_name
                setter_func = getattr(self.global_settings, attr_name)
                self.global_settings = setter_func(value)

        else:
            self.show_global_settings()
    

    @docopt_cmd
    def do_list(self, arg):
        '''Usage: lschannel'''

        print '+++ Event channels:'
        print '\n'.join([c.name for c in self.channels])
        
    
    @docopt_cmd
    def do_mkchannel(self, arg):
        '''Usage: mkchannel
                  mkchannel <channel_name>
        '''

        if arg.get('<channel_name>'):
            channel_name = arg['<channel_name>']
        else:
            channel_name = cli.InputPrompt('event channel name').show()
            if not channel_name:
                return
            
        while True:
            channel_params = {
                'handler_function': None,
                'schema': None,
                'table_name': None,
                'operation': None,
                'primary_key_field': None,
                'primary_key_type': None,
                'procedure_name': None,
                'trigger_name': None,
                'payload_fields': []
            }
            missing_params = 4 # some of the channel parameters are optional
            
            channel_params['table_name'] = cli.InputPrompt('table name').show()
            if not channel_params['table_name']:
                break
            missing_params -= 1

            channel_params['schema'] = cli.InputPrompt('db schema', 'public').show()

            channel_params['operation'] = cli.MenuPrompt('operation', OPERATION_OPTIONS).show()
            if channel_params['operation'] is None:
                break
            missing_params -= 1

            channel_params['primary_key_field'] = cli.InputPrompt('primary key field', 'id').show()
            if channel_params['primary_key_field'] is None:
                break
            missing_params -= 1

            channel_params['primary_key_type'] = cli.InputPrompt('primary key type', 'bigint').show()
            if channel_params['primary_key_type'] is None:
                break
            missing_params -= 1

            channel_params['payload_fields'] = self.prompt_for_payload_fields(channel_name)

            channel_params['handler_function'] = cli.InputPrompt('handler function').show()
            channel_params['procedure_name'] = cli.InputPrompt('stored procedure name',
                                                               default_proc_name(channel_params['table_name'],
                                                                                 channel_params['operation'])).show()
            channel_params['trigger_name'] = cli.InputPrompt('trigger name',
                                                             default_trigger_name(channel_params['table_name'],
                                                                                  channel_params['operation'])).show()
            
            if not missing_params:
                new_channel = self.create_channel(channel_name, **channel_params)
                self.channels.append(new_channel)
                
                should_continue = cli.InputPrompt('create another channel [Y/n]?', 'y').show()
                if should_continue.lower() == 'y':
                    continue                
            break
        
    
        
    
