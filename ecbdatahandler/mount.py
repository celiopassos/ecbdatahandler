#!/usr/bin/env python

import os
import configparser
import re

import pandas as pd
import pandas.io.formats.excel
import numpy as np
import sqlalchemy

from ecbdatahandler.datahandlers import MedicaoSQLm3, MedicaoSQLton, \
    CombustivelSQL
from ecbdatahandler.helpers import to_sql_string, prompt_yes_no, silent, \
    date_to_str_pt, get_quinzenas


class CA:

    def __init__(self, ca, medicao_df, combustivel_df=pd.DataFrame()):
        self.ca = ca
        self.medicao_df = medicao_df
        self.combustivel_df = combustivel_df

        self.total_carga_bruta = medicao_df['valorizacao'].sum()
        self.total_combustivel = \
            combustivel_df['total'].sum() if not combustivel_df.empty else 0.0
        self.descontado = self.total_carga_bruta - self.total_combustivel
        self.iss = 0.04 * self.descontado if self.descontado > 0 else 0.0
        self.liquido = self.descontado - self.iss

    def export_sheet(self, output_folder, columns, widths):
        medicao_df = self.medicao_df.copy()
        medicao_df = medicao_df.sort_values(by=['data', 'km_inicial'])
        medicao_df['data'] = medicao_df['data'].apply(date_to_str_pt)

        rename_map = {to_sql_string(col): col for col in columns}
        medicao_df = medicao_df.rename(columns=rename_map)
        medicao_df = medicao_df[columns]

        filename = '{}/{}.xlsx'.format(output_folder, self.ca)

        pandas.io.formats.excel.header_style = None
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        medicao_df.to_excel(
            writer,
            sheet_name='Medição',
            index=False,
            float_format='%0.2f'
        )

        wb = writer.book
        ws = writer.sheets['Medição']

        # last_col = string.ascii_uppercase[len(columns) - 1]
        last_row = len(medicao_df.index) + 1

        ws.set_landscape()
        ws.set_paper(9)
        ws.print_area(0, 0, last_row, len(columns) - 1)
        ws.set_margins(left=0.3, right=0.3, top=0.3, bottom=0.3)
        ws.fit_to_pages(1, 0)

        header_format = wb.add_format({
            'font_size': 10,
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'shrink': True,
            'top': True,
            'bottom': True,
            'left': True,
            'right': True,
        })

        ws.set_row(0, None, header_format)
        ws.repeat_rows(0)

        data_format = wb.add_format({
            'font_size': 8,
            'align': 'center',
            'valign': 'vcenter',
            'shrink': True,
            'top': True,
            'bottom': True,
            'left': True,
            'right': True,
        })

        for row_id in range(1, last_row):
            ws.set_row(row_id, None, data_format)

        for col_id in range(0, len(columns) - 1):
            ws.set_column(col_id, col_id, widths[col_id])

        writer.save()

    def export_resumo(self, output_folder, columns):
        filename = '{}/{}.txt'.format(output_folder, self.ca)

        with open(filename, 'w') as resumo:
            resumo.write('{}\n\nPeríodo: {}\n\n'.format(
                self.ca,
                ', '.join(get_quinzenas(self.medicao_df['period'].unique()))
            ))
            resumo.write('TOTAL VALOR CARGA BRUTA: {:.2f}\n\n'.format(
                self.total_carga_bruta
            ))
            if not self.combustivel_df.empty:
                combustivel_df = self.combustivel_df.copy()

                rename_map = {to_sql_string(col): col for col in columns}
                combustivel_df = combustivel_df.rename(columns=rename_map)
                resumo.write(combustivel_df.to_string(
                    columns=columns,
                    index=False,
                    header=True,
                    justify='left',
                    col_space=10
                ))
            resumo.write(
                '\n\nTotal do combustível: R$ {:.2f}'
                '\nDescontado o combustível: R$ {:.2f}'
                '\nISS 4%: R$ {:.2f}'
                '\nTotal a receber: R$ {:.2f}'.format(
                    self.total_combustivel,
                    self.descontado,
                    self.iss,
                    self.liquido
                ))
            resumo.write(
                '\n\nOBS: Se houve gastos adicionais como alimentação ou '
                'borracharia, estes gastos ainda serão descontados.'
            )

        silent('unix2dos {}'.format(filename), silence_stderr=True)

    def stats(self):
        return {
            'ca': self.ca,
            'total_carga_bruta': self.total_carga_bruta,
            'total_combustivel': self.total_combustivel,
            'liquido': self.liquido
        }


class MountSQL:

    def __init__(self, info_file):
        config = configparser.ConfigParser()
        config.read(info_file)

        def get_config_split(section, option):
            return config.get(section, option, fallback='').split(', ')

        def get_config_dict(section, option):
            try:
                return {
                    pair.split(':')[0]: pair.split(':')[1]
                    for pair in get_config_split(section, option)
                }
            except IndexError:
                return {}

        self.data_handlers = {}
        self.data_config = {}
        self.ca_list = []
        self.unproductive = pd.DataFrame()

        self.mysql = dict(config['mysql'])
        self.global_filters = dict(config['global_filters'])
        self.packs = {
            pack: get_config_split('packs', pack) for pack in config['packs']
        }

        self.medicao_names = get_config_split('medicao', 'names')

        self.medicao_columns = []
        self.medicao_widths = []
        for pair in get_config_split('medicao', 'columns'):
            col, width = pair.split(':')
            self.medicao_columns.append(col)
            self.medicao_widths.append(int(width))

        for name in self.medicao_names:
            name_config = dict(config[name])
            name_config['price'] = get_config_dict(name, 'price')
            name_config['rename'] = get_config_dict(name, 'rename')
            name_config['null_price_map'] = get_config_dict(
                name, 'null_price_map'
            )

            name_type = {
                'm3': MedicaoSQLm3,
                'ton': MedicaoSQLton
            }[name_config['type']]

            self.data_config[name] = name_config
            self.data_handlers[name] = name_type(
                table=name_config['table'], filters=self.global_filters
            )

        self.combustivel_names = get_config_split('combustivel', 'names')
        self.combustivel_columns = get_config_split('combustivel', 'columns')

        for name in self.combustivel_names:
            name_config = dict(config[name])
            name_config['price'] = get_config_dict(name, 'price')
            name_config['rename'] = get_config_dict(name, 'rename')
            name_config['null_price_map'] = get_config_dict(
                name, 'null_price_map'
            )

            self.data_config[name] = name_config
            self.data_handlers[name] = CombustivelSQL(
                table=name_config['table'], filters=self.global_filters
            )

    def load(self):
        engine = sqlalchemy.create_engine(
            'mysql+pymysql://{user}:{password}@{server}/{database}'.format(
                **self.mysql
            )
        )
        for name, handler in self.data_handlers.items():
            handler.load(engine)
            handler.prepare(config=self.data_config[name], packs=self.packs)

    def _aggregate(self):
        self.medicao_df = pd.DataFrame()

        for name in self.medicao_names:
            config = self.data_config[name]
            to_append_df = self.data_handlers[name].dataframe

            to_append_df = to_append_df.rename(columns=config['rename'])
            to_append_df['unid'] = self.data_config[name]['type']

            self.medicao_df = self.medicao_df.append(to_append_df)

        self.combustivel_df = pd.DataFrame()

        for name in self.combustivel_names:
            config = self.data_config[name]
            to_append_df = self.data_handlers[name].dataframe

            to_append_df = to_append_df.rename(columns=config['rename'])

            self.combustivel_df = self.combustivel_df.append(to_append_df)

    def _split_ca_sem_combustivel(self):
        medicao_cas = self.medicao_df.sort_values('cod1')['ca'].unique()

        for ca in medicao_cas:
            medicao = self.medicao_df.loc[self.medicao_df['ca'] == ca]
            self.ca_list.append(CA(ca, medicao))

    def _split_ca_com_combustivel(self):
        medicao_cas = self.medicao_df.sort_values('cod1')['ca'].unique()
        ca_placa_map = {
            ca: list(self.medicao_df.loc[self.medicao_df['ca'] == ca, 'placa'])
            for ca in medicao_cas
        }

        medicao_placas = self.medicao_df['placa'].unique()
        combustivel_placas = self.combustivel_df['placa'].unique()

        missing_medicao = sorted(
            list(set(combustivel_placas).difference(medicao_placas))
        )

        # try to find CA in info's first string
        if missing_medicao:
            # iterate over copy, because we're altering it
            for placa in missing_medicao[:]:
                info = self.combustivel_df.loc[
                    self.combustivel_df['placa'] == placa, 'prefixo_marca'
                ].unique()
                placa_ca = re.search('CA-\d+|$', info[0]).group()
                if placa_ca:
                    ca_placa_map.get(placa_ca, []).append(placa)
                    missing_medicao.remove(placa)

        # ask user for CA if still not found
        if missing_medicao:
            print('CAs with medicao: {}.\n'.format(', '.join(medicao_cas)))
            print(
                'Unable to find medicao of the following placas: %s.\n' %
                (', '.join(missing_medicao))
            )

            # iterate over copy, because we're altering it
            for placa in missing_medicao[:]:
                info = self.combustivel_df.loc[
                    self.combustivel_df['placa'] == placa, 'prefixo_marca'
                ].unique()
                print('Info for {}:\n\t{}'.format(placa, '\n '.join(info)))
                question = 'Is there medicao for {}?'.format(placa)
                if prompt_yes_no(question, default='no'):
                    placa_ca = input('Enter CA: ')
                    ca_placa_map.get(placa_ca, []).append(placa)
                    missing_medicao.remove(placa)

        self.unproductive = pd.DataFrame([(
            placa,
            self.combustivel_df.loc[
                self.combustivel_df['placa'] == placa, 'total'
            ].sum()
        ) for placa in missing_medicao], columns=['Placa', 'Total'])

        for ca in medicao_cas:
            ca_medicao = self.medicao_df.loc[
                self.medicao_df['ca'] == ca
            ]
            ca_combustivel = self.combustivel_df.loc[
                self.combustivel_df['placa'].isin(ca_placa_map[ca])
            ]
            if not ca_combustivel.empty:
                self.ca_list.append(CA(ca, ca_medicao, ca_combustivel))
            else:
                self.ca_list.append(CA(ca, ca_medicao))

    def mount(self):
        self._aggregate()

        if self.combustivel_names:
            self._split_ca_com_combustivel()
        else:
            self._split_ca_sem_combustivel()

        folders = {
            'excel': 'CA/Partes diárias - EXCEL',
            'pdf': 'CA/Partes diárias - PDF',
            'txt': 'CA/Resumos'
        }
        for folder in folders.values():
            os.makedirs(folder, exist_ok=True)

        for ca in self.ca_list:
            ca.export_sheet(
                folders['excel'], self.medicao_columns, self.medicao_widths
            )
            ca.export_resumo(folders['txt'], self.combustivel_columns)

        for sheet in os.listdir(folders['excel']):
            command = 'soffice --headless --convert-to ' \
                'pdf:"impress_pdf_Export" --outdir "{}" ' \
                '"{}/{}"'.format(folders['pdf'], folders['excel'], sheet)
            silent(command)

    def export_resumo_geral(self):
        total = self.medicao_df['valorizacao'].sum()
        total_combustivel = self.combustivel_df['total'].sum()

        stats = [ca.stats() for ca in self.ca_list]

        total_ca = sum(
            stat['total_carga_bruta'] for stat in stats
        )
        total_combustivel_ca = sum(
            stat['total_combustivel'] for stat in stats
        )

        liquido_df = self.medicao_df.set_index('cod1')[['ca']].drop_duplicates()
        liquido_df = liquido_df.sort_index()
        liquido_df = liquido_df.rename(columns={'ca': 'CA'})
        liquido_df = liquido_df.set_index('CA', drop=False)
        liquido_df['Total a receber'] = np.nan
        total_liquido = 0.0

        for stat in stats:
            liquido_df.loc[stat['ca'], 'Total a receber'] = stat['liquido']
            total_liquido += stat['liquido']

        with open('Resumo_geral.txt', 'w') as resumo:
            resumo.write(
                'Período: {}\n\n'
                'Total: R$ {:.2f}\n'
                'Total (CA): R$ {:.2f}\n'
                'Total combustível: R$ {:.2f}\n'
                'Total combustível (CA): R$ {:.2f}\n'
                'Total líquido (-4% ISS): R$ {:.2f}\n\n'.format(
                    get_quinzenas([self.global_filters['period']])[0],
                    total,
                    total_ca,
                    total_combustivel,
                    total_combustivel_ca,
                    total_liquido,
                ))
            resumo.write(liquido_df.to_string(
                header=True,
                index=False,
                col_space=10,
                justify='left',
                float_format='%0.2f'
            ))

            if not self.unproductive.empty:
                resumo.write(
                    '\n\nCaminhões que gastaram combustível e não produziram:'
                    '\n\n'
                )
                resumo.write(self.unproductive.to_string(
                    header=True,
                    index=False,
                    col_space=10,
                    justify='left',
                    float_format='%0.2f'
                ))

        silent('unix2dos Resumo_geral.txt', silence_stderr=True)
