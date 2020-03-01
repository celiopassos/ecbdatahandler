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

    def load_sql(self, engine):
        self._sql_df = pd.read_sql_query(
            'SELECT * FROM {} LIMIT 1'.format(self.tablename),
            engine
        )

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

        # delete columns not in database
        self.load_sql(engine)
        self.df = self.df[[col for col in self._sql_df.columns if col in self.df.columns]]

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

        self.df['placa'] = self.df['placa'].apply(fix_placa)
        self.df['data'] = self.df['data'].apply(date_to_str)
