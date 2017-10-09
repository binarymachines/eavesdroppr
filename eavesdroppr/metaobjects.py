#!/usr/bin/env python

from snap import common


REQUIRED_CHANNEL_FIELDS = ['handler_function',
                           'table_name',
                           'operation',
                           'primary_key_field',
                           'primary_key_type',
                           'payload_fields']


REQUIRED_GLOBAL_FIELDS: ['project_directory',
                         'database_host',
                         'database_name',
                         'debug',
                         'handler_module']

class GlobalsMeta(object):
    def __init__(self, **kwargs):
        


class ChannelMeta(object):
    def __init__(self, name, **kwargs):
        kwreader = common.KeywordArgReader(*REQUIRED_CHANNEL_FIELDS)
        kwreader.read(kwargs)
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
    
