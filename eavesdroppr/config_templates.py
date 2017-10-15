#!/usr/bin/env python


INIT_FILE = """
# 
# YAML init file for EavesdropPR listen/notify framework
#
#
globals:
        project_directory: {{ global_settings.data()['project_directory']}}
        database_host: {{ global_settings.data()['database_host'] }}
        database_name: {{ global_settings.data()['database_name'] }}
        debug: {{ global_settings.data()['debug']}}
        logfile: {{ global_settings.data()['logfile'] }}
        handler_module: {{ global_settings.data()['handler_module'] }}
        service_module: {{ global_settings.data()['service_module'] }} 
        

service_objects:
        {% for so in service_objects %}
        {{ so.name }}:
            class:
                {{ so.classname }}
            init_params:
                {% for p in so.init_params %}- name: {{ p['name'] }}
                  value: {{ p['value'] }}
                {% endfor %}
        {% endfor %}


channels:
        {% for ch in channels %}
        {{ch.name}}:
                handler_function: {{ch.handler_function}}
                db_table_name: {{ch.table_name}}
                db_operation: {{ch.operation}}
                pk_field_name: {{ch.primary_key_field}}
                pk_field_type: {{ch.primary_key_type}}
                db_schema: {{ch.schemaa}}
                db_proc_name: {{ch.procedure_name}}
                db_trigger_name: {{ch.trigger_name}}
                payload_fields:
                        {%for field in ch.payload_fields%}- {{field}}                        
                        {% endfor %}
        {% endfor %}
"""