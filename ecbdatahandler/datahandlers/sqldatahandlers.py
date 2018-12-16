#!/usr/bin/env python

import sys

from abc import ABC, abstractmethod

import pandas as pd

from ecbdatahandler.helpers import prompt_yes_no


class SQLDataHandlerABC(ABC):

    def __init__(self, table, filters):
        self.table = table
        self.filters = filters

    def load(self, engine):
        conditions = ' AND '.join(
            "{0} = '{1}'".format(t, v) for t, v in self.filters.items()
        )
        self._df = pd.read_sql_query(
            'SELECT * FROM {0} WHERE {1}'.format(self.table, conditions),
            engine
        )

    @abstractmethod
    def prepare(self, config, packs):
        pass

    @property
    def dataframe(self):
        return self._df.copy()

    @dataframe.setter
    def dataframe(self, value):
        raise TypeError


class MedicaoSQL(SQLDataHandlerABC):

    def __init__(self, table, filters, unid):
        super().__init__(table, filters)
        self.unid = unid
        self.price_column = unid + 'xpuxkm'

    def prepare(self, config, packs):
        for column in config['not_null'].split(', '):
            self._df = self._df.loc[self._df[column].notnull()]

        if self._df.empty:
            print("Warning: no added added from table {}.".format(self.table))
            return

        self._df['data'] = pd.to_datetime(self._df['data'])

        materiais = set(self._df['material'])

        for pack, price in config['price'].items():
            self._df.loc[
                self._df['material'].isin(packs[pack]), self.price_column
            ] = price
            materiais.difference_update(set(packs[pack]))

        if None in materiais:
            price_map = {
                float(k): float(v) for k, v in config['null_price_map'].items()
            }
            self._df.loc[self._df['material'].isnull(), self.price_column] = \
                self._df.loc[
                    self._df['material'].isnull(), self.price_column
                ].map(price_map)
            materiais.discard(None)

        if materiais:
            print(
                'The following materiais did not have their price updated:\n',
                '\n\t{0}\n\n'.format('\n\t'.join(materiais)),
                'In the handling of table {0}.'.format(config['table']),
                sep=''
            )
            cont = prompt_yes_no('Continue?', default='no')
            if not cont:
                sys.exit(1)
            print('')

        self._df['valorizacao'] = (
            pd.to_numeric(self._df[self.unid]) *
            pd.to_numeric(self._df[self.price_column]) *
            pd.to_numeric(self._df['acerto'])
        ).fillna(0.0).round(2)


class CombustivelSQL(SQLDataHandlerABC):

    def prepare(self, config, packs):
        for column in config['not_null'].split(', '):
            self._df = self._df.loc[self._df[column].notnull()]

        if self._df.empty:
            print("Warning: no added added from table {}.".format(self.table))
            return

        self._df['data'] = pd.to_datetime(self._df['data'])

        combustiveis = set(self._df['tipo_de_combustivel'])

        for pack, price in config['price'].items():
            self._df.loc[
                self._df['tipo_de_combustivel'].isin(packs[pack]),
                'preco'
            ] = price
            combustiveis.difference_update(set(packs[pack]))

        if combustiveis:
            print(
                'The following combustiveis did not have their price updated:',
                '\n\t{0}\n'.format('\n\t'.join(combustiveis)),
                'In the handling of table {0}.'.format(config['table'])
            )
            cont = prompt_yes_no('Continue?', default='no')
            if not cont:
                sys.exit(1)

        self._df['total'] = (
            pd.to_numeric(self._df['qtd']) *
            pd.to_numeric(self._df['preco'])
        ).fillna(0.0).round(2)
