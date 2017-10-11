#!/usr/bin/env python

from snap import common


REQUIRED_CHANNEL_FIELDS = ['handler_function',
                           'table_name',
                           'operation',
                           'primary_key_field',
                           'primary_key_type',
                           'payload_fields']


REQUIRED_GLOBAL_FIELDS = ['project_directory',
                          'database_host',
                          'database_name',
                          'debug',
                          'handler_module']


class GlobalSettingsMeta(object):
    def __init__(self, **kwargs):
        self._app_name = 'eavesdrop'
        self._debug = kwargs.get('debug') or True
        self._database_host = kwargs.get('database_host') or '127.0.0.1'
        self._database_name = kwargs.get('database_name')
        self._service_module = kwargs.get('service_module') or '%s_services' % self._app_name
        self._handler_module = kwargs.get('handler_module') or '%s_handlers' % self._app_name
        self._project_directory = kwargs.get('project_directory') or  '$%s_HOME' % self._app_name.upper()
        self._logfile = kwargs.get('logfile') or '%s.log' % self._app_name


    @property
    def current_values(self):
        original_attrs = self.__dict__
        attrs = {}
        for key in original_attrs:
            if key != '_app_name':
                attrs[key.lstrip('_')] = original_attrs[key]
        return attrs


    def data(self):
        return self.current_values
    
    


class ChannelMeta(object):
    def __init__(self, name, **kwargs):
        kwreader = common.KeywordArgReader(*REQUIRED_CHANNEL_FIELDS)
        kwreader.read(**kwargs)
        self.name = name
        self._data = kwargs


    @property
    def handler_func(self):
        return self._data['handler_function']


    @property
    def schema(self):
        return self._data.get('schema', 'public')


    @property
    def table_name(self):
        return self._data['table_name']


    @property
    def operation(self):
        return self._data['operation']

    @property
    def primary_key_field(self):
        return self._data['primary_key_field']


    @property
    def primary_key_type(self):
        return self._data['primary_key_type']

        
    @property
    def procedure_name(self):
        return self._data['procedure_name']


    @property
    def trigger_name(self):
        return self._data.get('trigger_name')
    
    
    @property
    def payload_fields(self):
        return self._data['payload_fields']
    
