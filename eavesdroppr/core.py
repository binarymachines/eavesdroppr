#!/usr/bin/env python


class NoSuchEventChannel(Exception):
    def __init__(self, channel_id):
        Exception.__init__(self,
                           'No event channel registered under the name "%s". Please check your initfile.' % channel_id)


class NoSuchEventHandler(Exception):
    def __init__(self, handler_func_name, handler_module):
        Exception.__init__(self, 'No event handler function "%s" exists in handler module "%s". Please check your initfile and code modules.' % (handler_func_name, handler_module))


class UnsupportedDBOperation(Exception):
    def __init__(self, operation):
        Exception.__init__(self, 'The database operation "%s" is not supported.' % operation)



def generate_code(channel_name, channel_config, **kwargs):
    operation = channel_config['db_operation']
    if not operation in SUPPORTED_DB_OPS:
        raise eavesdroppr.UnsupportedDBOperation(operation)

    table_name = channel_config['db_table_name']
    db_schema = channel_config.get('db_schema') or 'public'
    proc_name = channel_config.get('db_proc_name') or '%s_%s_notify' % (table_name, operation.lower())
    trigger_name = channel_config.get('db_trigger_name') or 'trg_%s_%s' % (table_name, operation.lower())
    source_fields = channel_config['payload_fields']
    
    j2env = jinja2.Environment()
    template_mgr = common.JinjaTemplateManager(j2env)        
    json_func_template = j2env.from_string(JSON_BUILD_FUNC_TEMPLATE)
    json_func = json_func_template.render(payload_fields=source_fields)
    
    pk_field = channel_config['pk_field_name']
    pk_type = channel_config['pk_field_type']

    if args['--proc']:
        print PROC_TEMPLATE.format(schema=db_schema,
                                   pk_field_name=pk_field,
                                   pk_field_type=pk_type,
                                   channel_name=event_channel,
                                   json_build_func=json_func)

    elif args['--trigger']:
        print TRIGGER_TEMPLATE.format(schema=db_schema,
                                      table_name=table_name,
                                      trigger_name=trigger_name,
                                      db_proc_name=proc_name,
                                      db_op=operation)


def listen(yaml_config, **kwargs):
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

    project_dir = common.load_config_var(yaml_config['globals']['project_dir'])
    sys.path.append(project_dir)
    handlers = __import__(handler_module_name)
    handler_function_name = yaml_config['channels'][channel_id]['handler_function']
    
    if not hasattr(handlers, handler_function_name):
        raise core.NoSuchEventHandler(handler_function_name, handler_module_name)

    handler_function = getattr(handlers, handler_function_name)
    service_objects = common.ServiceObjectRegistry(snap.initialize_services(yaml_config, logger))
    
    pubsub.listen(channel_id)
    print 'listening on channel "%s"...' % channel_id
    for event in pubsub.events():        
        print event.payload
    


        
    
