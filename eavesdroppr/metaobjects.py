#!/usr/bin/env python

from snap import common
import copy


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



class ServiceObjectMeta(object):
    def __init__(self, name, class_name, **kwargs):
        self._name = name
        self._classname = class_name
        self._init_params = []
        for param_name, param_value in kwargs.iteritems():
            self._init_params.append({'name': param_name, 'value': param_value})


    @property
    def name(self):
        return self._name


    @property
    def classname(self):
        return self._classname


    @property
    def init_params(self):
        return self._init_params


    def _params_to_dict(self, param_array):
        result = {}
        for p in param_array:
            result[p['name']] = p['value']
        return result


    def find_param_by_name(self, param_name):
        param = None
        for p in self._init_params:
            if p['name'] == param_name:
                param = p
                break
        return param


    def set_name(self, name):
        return ServiceObjectMeta(name, self._classname, **self._params_to_dict(self._init_params))


    def set_classname(self, classname):
        return ServiceObjectMeta(self._name, classname, **self._params_to_dict(self._init_params))


    def add_param(self, name, value):
        new_param_list = copy.deepcopy(self._init_params)
        new_param_list.append({'name': name, 'value': value})
        params = self._params_to_dict(new_param_list)

        return ServiceObjectMeta(self._name, self._classname, **params)


    def add_params(self, **kwargs):
        updated_so = self
        for name, value in kwargs.iteritems():
            updated_so = updated_so.add_param(name, value)
        return updated_so


    def remove_param(self, name):
        param = self.find_param_by_name(name)
        if not param:
            return self

        new_param_list = copy.deepcopy(self._init_params)
        new_param_list.remove(param)
        params = {}
        for p in new_param_list:
            params[p['name']] = p['value']

        return ServiceObjectMeta(self._name, self._classname, **params)


    def data(self):
        result = {'name': self._name,
                  'class': self._classname,
                  'init_params': self._init_params}
        return result



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
        self._data = {}
        for key, value in kwargs.iteritems():
            if key == 'payload_fields':
                self._data[key] = set()
                # here we know that value is actually a collection
                for field in value:
                    self._data[key].add(field)
            else:
                self._data[key] = value


    def rename(self, new_name):
        new_data = copy.deepcopy(self._data)
        return ChannelMeta(new_name, **new_data)


    def set_property(self, name, value):
        new_data = copy.deepcopy(self._data)
        new_data[name] = value
        return ChannelMeta(self.name, **new_data)


    def property_names(self):
        return self._data.keys()


    @property
    def handler_function(self):
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


    def add_payload_fields(self, *fields):
        new_data = copy.deepcopy(self._data)
        for field in fields:
            new_data['payload_fields'].add(field)
        return ChannelMeta(self.name, **new_data)


    def delete_payload_field(self, field):
        new_data = copy.deepcopy(self._data)
        new_data['payload_fields'].discard(field)
        return ChannelMeta(self.name, **new_data)


    def data(self):
        result = {}
        for key, value in self._data.iteritems():
            if key == 'payload_fields':
                result[key] = []
                for f in value:
                    result[key].append(f)
            else:
                result[key] = value

        return result
