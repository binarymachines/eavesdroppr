#!/usr/bin/env python

import os, sys
import pgpubsub
from snap import snap, common
from snap import cli_tools as cli
from code_templates import *
from config_templates import *
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
        raise UnsupportedDBOperation(operation)

    table_name = channel_config['db_table_name']
    db_schema = channel_config.get('db_schema') or 'public'
    procedure_name = channel_config.get('db_proc_name') or default_proc_name(table_name, operation)
    trigger_name = channel_config.get('db_trigger_name') or default_trigger_name(table_name,
                                                                                 operation)
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



class EavesdropConfigWriter(object):

    def __init__(self):
        pass


    def write(self, **kwargs):
        kwreader = common.KeywordArgReader('settings',
                                           'channels')

        kwreader.read(**kwargs)
        j2env = jinja2.Environment()
        template = j2env.from_string(INIT_FILE)
        return template.render(global_settings=kwreader.get_value('settings'),
                               channels=kwreader.get_value('channels') or [],
                               service_objects=kwargs.get('services', []))



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


    def find_channel(self, name):
        result = None
        for s in self.channels:
            if s.name == name:
                result = s
                break

        return result


    def get_channel_index(self, name):
        result = -1
        for i in range(0, len(self.channels)):
            if self.channels[i].name == name:
                result = i
                break
        return result


    def select_channel(self):
        options = [{'value': c.name, 'label': c.name} for c in self.channels]
        return cli.MenuPrompt('select event channel', options).show()


    def create_channel(self, channel_name, **kwargs):
        print 'stub create channel function'
        print common.jsonpretty(kwargs)
        return ChannelMeta(channel_name, **kwargs)


    def edit_channel(self, name):
        print '+++ Updating event channel'
        channel_index = self.get_channel_index(name)
        if channel_index < 0:
            print 'No event channel registered under the name %s.' % name
            return

        current_channel = self.channels[channel_index]
        '''
        channel_name = cli.InputPrompt('change name to', current_channel.name).show()
        self.channels[channel_index] = current_channel.rename(channel_name)
        '''

        while True:
            property_options = [{'value': pn, 'label': pn} for pn in current_channel.property_names()]
            target_property_name = cli.MenuPrompt('event channel property to update', property_options).show()

            if target_property_name == 'handler_function':
                handler_func = cli.InputPrompt('change handler function to',
                                               current_channel.handler_function).show()
                self.channels[channel_index] = current_channel.set_property('handler_function',
                                                                            handler_func)

            elif target_property_name == 'table_name':
                db_table_name = cli.InputPrompt('change table name to',
                                                current_channel.table_name).show()
                self.channels[channel_index] = current_channel.set_property('table_name',
                                                                            db_table_name)

            elif target_property_name == 'operation':
                db_operation = cli.InputPrompt('change operation to',
                                               current_channel.operation).show()
                self.channels[channel_index] = current_channel.set_property('operation',
                                                                            db_operation)

            elif target_property_name == 'primary_key_field':
                pk_field = cli.InputPrompt('change primary key field to',
                                           current_channel.primary_key_field).show()
                self.channels[channel_index] = current_channel.set_property('primary_key_field',
                                                                            pk_field)

            elif target_property_name == 'primary_key_type':
                pk_type = cli.InputPrompt('change primary key type to',
                                          current_channel.primary_key_type).show()
                self.channels[channel_index] = current_channel.set_property('primary_key_type',
                                                                            pk_type)

            elif target_property_name == 'schema':
                db_schema = cli.InputPrompt('change schema to',
                                            current_channel.schema).show()
                self.channels[channel_index] = current_channel.set_property('schema', db_schema)

            elif target_property_name == 'procedure_name':
                procname = cli.InputPrompt('change stored procedure name to',
                                           current_channel.procedure_name).show()
                self.channels[channel_index] = current_channel.set_property('procedure_name',
                                                                            procname)

            elif target_property_name == 'trigger_name':
                trigger = cli.InputPrompt('change trigger name to',
                                          current_channel.trigger_name).show()
                self.channels[channel_index] = current_channel.set_property('trigger_name', trigger)

            should_continue = cli.InputPrompt('edit another property (Y/n)?', 'y').show()
            if should_continue == 'y':
                continue
            break


    def find_service_object(self, name):
        result = None
        for so in self.service_objects:
            if so.name == name:
                result = so
                break
        return result


    def get_service_object_index(self, name):
        result = -1
        for i in range(0, len(self.service_objects)):
            if self.service_objects[i].name == name:
                result = i
                break
        return result


    def select_service_object(self):
        options = [{'value': so.name, 'label': so.name} for so in self.service_objects]
        return cli.MenuPrompt('select service object', options).show()


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

        print '+++ payload fields:\n-%s \n+++ added to event channel "%s".' \
            % ('\n-'.join(fields), channel_name)
        return fields


    def do_quit(self, arg):
        print 'eavesdrop interactive mode exiting.'
        raise SystemExit


    do_q = do_quit
    do_exit = do_quit


    def show_global_settings(self):
        print common.jsonpretty(self.global_settings.data())


    def edit_global_settings(self):
        print '+++ updating eavesdrop settings...'
        settings_menu = []
        defaults = self.global_settings.current_values
        for key, value in defaults.iteritems():
            settings_menu.append({'label': key, 'value': key})

        while True:
            setting_name = cli.MenuPrompt('global setting to update', settings_menu).show()
            if not setting_name:
                break
            setting_value = cli.InputPrompt(setting_name, defaults[setting_name]).show()

            attr_name = 'set_%s' % setting_name
            setter_func = getattr(self.global_settings, attr_name)
            self.global_settings = setter_func(setting_value)

            should_continue = cli.InputPrompt('update another (Y/n)?', 'y').show()
            if should_continue.lower() != 'y':
                break


    def edit_global_setting(self, setting_name):
        if not setting_name in self.global_settings.current_values.keys():
            print "!! No such global setting. Available global settings are: "
            print '\n'.join(['- %s' % (k) for k in self.global_settings.data().keys()])
            return

        defaults = self.global_settings.current_values
        setting_value = cli.InputPrompt(setting_name, defaults[setting_name]).show()
        attr_name = 'set_%s' % setting_name
        setter_func = getattr(self.global_settings, attr_name)
        self.global_settings = setter_func(setting_value)


    def create_service_object_params(self):
        so_params = {}
        while True:
            param_name = cli.InputPrompt('parameter name').show()
            if not param_name:
                break
            param_value = cli.InputPrompt('parameter value').show()
            if not param_value:
                break

            so_params[param_name] = param_value

            should_continue = cli.InputPrompt('add another parameter (Y/n)?', 'y').show()
            if should_continue.lower() != 'y':
                break

        return so_params


    def make_svcobject(self, name):
        print '+++ Register new service object'
        so_name = name or cli.InputPrompt('service object name').show()
        so_classname = cli.InputPrompt('service object class').show()
        so_params = self.create_service_object_params()
        self.service_objects.append(ServiceObjectMeta(so_name, so_classname, **so_params))



    def show_svcobject(self, name):
        index = self.get_service_object_index(name)
        if index < 0:
            print '> No service object registered under the name %s.' % name
            return
        print common.jsonpretty(self.service_objects[index].data())


    def edit_svcobject(self, so_name):
        print '+++ Updating service object'
        so_index = self.get_service_object_index(so_name)
        if so_index < 0:
            print 'No service object registered under the name %s.' % so_name
            return

        current_so = self.service_objects[so_index]
        so_name = cli.InputPrompt('change name to', current_so.name).show()
        self.service_objects[so_index] = current_so.set_name(so_name)
        current_so = self.service_objects[so_index]

        so_classname = cli.InputPrompt('change class to', current_so.classname).show()
        self.service_objects[so_index] = current_so.set_classname(so_classname)
        current_so = self.service_objects[so_index]

        operation = cli.MenuPrompt('select service object operation', CHSO_OPTIONS).show()
        if operation == 'add_params':
            new_params = self.create_service_object_params()
            self.service_objects[so_index] = current_so.add_params(**new_params)
            current_so = self.service_objects[so_index]

        if operation == 'remove_params':
            while True:
                param_menu = [{'label': p['name'], 'value': p['name']} for p in current_so.init_params]
                param_name = cli.MenuPrompt('select param to remove', param_menu).show()
                self.service_objects[so_index] = current_so.remove_param(param_name)
                current_so = self.service_objects[so_index]

                should_continue = cli.InputPrompt('remove another (y/n)?', 'Y').show()
                if should_continue.lower() != 'y':
                    break


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


    def list_channels(self):
        print '+++ Event channels:'
        print '\n'.join([c.name for c in self.channels])


    def show_channel(self, name):
        index = self.get_channel_index(name)
        if index < 0:
            print '> No event channel registered under the name %s.' % name
            return
        print common.jsonpretty(self.channels[index].data())


    def list_svcobjects(self):
        print '+++ Service objects:'
        print '\n'.join([so.name for so in self.service_objects])


    @docopt_cmd
    def do_list(self, arg):
        '''Usage: list (channels | svcobjs )'''

        if arg['channels']:
            self.list_channels()
        elif arg['svcobjs']:
            self.list_svcobjects()



    def complete_list(self, text, line, begidx, endidx):
        LIST_OPTIONS = ('channels', 'svcobjs')
        return [i for i in LIST_OPTIONS if i.startswith(text)]



    def make_channel(self, channel_name):

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


    @docopt_cmd
    def do_make(self, cmd_args):
        '''Usage:
                make (channel | svcobj)
                make channel <name>
                make svcobj <name>
        '''

        object_name = cmd_args.get('<name>')

        if cmd_args['channel']:
            channel_name = object_name or cli.InputPrompt('event channel name').show()
            if not channel_name:
                return
            self.make_channel(channel_name)
        elif cmd_args['svcobj']:

            svcobject_name = object_name or cli.InputPrompt('service object name').show()
            if not svcobject_name:
                return
            self.make_svcobject(svcobject_name)



    def complete_make(self, text, line, begidx, endidx):
        MAKE_OPTIONS = ('channel', 'svcobj')
        return [i for i in MAKE_OPTIONS if i.startswith(text)]


    @docopt_cmd
    def do_edit(self, arg):
        '''Usage:
                    edit (channel | svcobj)
                    edit channel <name>
                    edit svcobj <name>
        '''
        object_name = arg.get('<name>')

        if arg['channel']:
            if not len(self.channels):
                print 'You have not created any Event Channels yet.'
                return
            channel_name = object_name or self.select_channel()
            if not channel_name:
                return
            self.edit_channel(channel_name)

        elif arg['svcobj']:
            if not len(self.service_objects):
                print 'You have not created any ServiceObjects yet.'
                return
            svcobj_name = object_name or self.select_service_object()
            if not svcobj_name:
                return
            self.edit_svcobject(svcobj_name)


    def complete_edit(self, text, line, begidx, endidx):
        EDIT_OPTIONS = ('channel', 'svcobj')
        return [i for i in EDIT_OPTIONS if i.startswith(text)]


    @docopt_cmd
    def do_show(self, cmd_args):
        '''Usage:
                  show (channel | svcobj)
                  show channel <name>
                  show svcobj <name>
        '''

        object_name = cmd_args.get('<name>')

        if cmd_args['channel']:
            if object_name:
                self.show_channel(object_name)
            else:
                print 'Available Event Channels:'
                self.list_channels()
        elif cmd_args['svcobj']:
            if object_name:
                self.show_svcobject(object_name)
            else:
                print 'Available ServiceObjects:'
                self.list_svcobjects()



    def complete_show(self, text, line, begidx, endidx):
        SHOW_OPTIONS = ('transform', 'shape', 'svcobj')
        return [i for i in SHOW_OPTIONS if i.startswith(text)]


    def get_save_condition(self):
        if not len(self.channels):
            return 'to save or preview, you must create at least one channel.'
        return 'ok'


    def yaml_config(self):
        cwriter = EavesdropConfigWriter()
        config = cwriter.write(settings=self.global_settings,
                               channels=self.channels,
                               services=self.service_objects)
        return config


    def do_preview(self, arg):
        '''display current configuration in YAML format'''

        message = self.get_save_condition()
        if message == 'ok':
            print self.yaml_config()
        else:
            print message
