#!/usr/bin/env python

from ecbdatahandler.datahandlers import MedicaoExcel, CombustivelExcel

import os
import configparser

import sqlalchemy
import pandas as pd


class ECBtoSQL:

    def __init__(self, info_file):
        path = os.path.split(info_file)[0]

        config = configparser.ConfigParser()
        config.read(info_file)

        def get_config_split(section, option):
            return config.get(section, option, fallback='').split(', ')

        self.names = get_config_split('general', 'names')

        self.start_date = config.get('global_filters', 'start_date')
        self.end_date = config.get('global_filters', 'end_date')
        self.daterange = pd.date_range(self.start_date, self.end_date)

        self.tags = dict(config['global_tags'])

        self.mysql = dict(config['mysql'])

        self.data_config = {}
        self.data_handlers = {}

        for name in self.names:
            name_config = dict(config[name])
            name_config['daterange'] = self.daterange
            name_type = {
                'medicao': MedicaoExcel,
                'combustivel': CombustivelExcel
            }[name_config['type']]

            self.data_config[name] = name_config
            self.data_handlers[name] = name_type(
                files=[
                    os.path.join(path, f)
                    for f in name_config['files'].split(', ')
                ],
                tags=self.tags,
                tablename=name_config['table']
            )

    def load(self):
        for name, data_wrapper in self.data_handlers.items():
            data_wrapper.load()
            data_wrapper.prepare(self.data_config[name])

    def to_sql(self):
        engine = sqlalchemy.create_engine(
            'mysql+pymysql://{user}:{password}@{server}/{database}'.format(
                **self.mysql
            )
        )
        for data_wrapper in self.data_handlers.values():
            data_wrapper.to_sql(engine)
