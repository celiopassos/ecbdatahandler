#!/usr/bin/env python

from abc import ABC, abstractmethod

import pandas as pd

from ecbdatahandler.helpers import fix_placa, date_to_str, to_sql_string


class ExcelDataHandlerABC(ABC):

    def __init__(self, files, tags, tablename):
        self.files = files
        self.tags = tags
        self.tablename = tablename
        self.df = pd.DataFrame()

    def load(self):
        for pair in self.files:
            filename = pair[:pair.index(':')]
            sheetname = pair[pair.index(':') + 1:]
            self.df = self.df.append(
                pd.read_excel(filename, sheetname=sheetname)
            )

        for tag, value in self.tags.items():
            self.df.insert(0, tag, value)

    @abstractmethod
    def prepare(self, config):
        pass

    def to_sql(self, engine):
        conditions = ' AND '.join(
            "{0} = '{1}'".format(t, v) for t, v in self.tags.items()
        )
        try:
            engine.execute('DELETE FROM {0} WHERE {1}'.format(
                self.tablename, conditions
            ))
        except Exception as e:
            cont = input(
                'Received the following error: \n\n{0}\n\nContinue? '.format(e)
            )
            if cont not in ['Y', 'y', 'yes']:
                exit()

        self.df.to_sql(
            self.tablename,
            engine,
            if_exists='append',
            index=False
        )


class MedicaoExcel(ExcelDataHandlerABC):

    def prepare(self, config):
        rename_map = {col: to_sql_string(col) for col in self.df.columns.values}
        self.df = self.df.rename(columns=rename_map)

        self.df = self.df.set_index('data', drop=False)
        self.df = self.df.loc[config['daterange']]
        self.df = self.df.sort_index()

        self.df['placa'] = self.df['placa'].apply(fix_placa)
        self.df['data'] = self.df['data'].apply(date_to_str)


class CombustivelExcel(ExcelDataHandlerABC):

    def prepare(self, config):
        rename_map = {col: to_sql_string(col) for col in self.df.columns}
        self.df = self.df.rename(columns=rename_map)

        self.df = self.df.set_index('data', drop=False)
        self.df = self.df.sort_index()

        for column in config['not_null'].split(', '):
            self.df = self.df.loc[self.df[column].notnull()]

        self.df['placa'] = self.df['placa'].apply(fix_placa)
        self.df['data'] = self.df['data'].apply(date_to_str)
