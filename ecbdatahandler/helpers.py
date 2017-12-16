#!/usr/bin/env python

import re
import os
import subprocess
import locale

from datetime import datetime


def fix_placa(placa):
    return ''.join(filter(str.isalnum, str(placa)))


def date_to_str(date):
    try:
        return date.strftime('%Y-%m-%d')
    except ValueError:
        return str(date)


def date_to_str_pt(date):
    return date.strftime('%d/%m/%Y')


def to_sql_string(string):
    string = str(string).lower()

    chars_map = {
        'ã': 'a', 'á': 'a', 'à': 'a', 'â': 'a',
        'ẽ': 'e', 'é': 'e', 'è': 'e', 'ê': 'e',
        'ĩ': 'i', 'í': 'i', 'ì': 'i', 'î': 'i',
        'õ': 'o', 'ó': 'o', 'ò': 'o', 'ô': 'o',
        'ũ': 'u', 'ú': 'u', 'ù': 'u', 'û': 'u',
        ' ': '_', 'ç': 'c', 'º': 'o', '<': 'menor_que',
        '¹': '1', '²': '2', '³': '3',
    }
    for k, v in chars_map.items():
        string = string.replace(k, v)

    string = ''.join(c for c in string if c.isalnum() or c in '_')
    string = re.sub('_+', '_', string)

    return string


def prompt_yes_no(question, default="yes"):
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}

    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        choice = input(question + prompt).lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def silent(command, silence_stderr=False):
    with open(os.devnull, "w") as fnull:
        if silence_stderr:
            subprocess.call(command, stdout=fnull, stderr=fnull, shell=True)
        else:
            subprocess.call(command, stdout=fnull, shell=True)


def get_quinzenas(iterable):
    locale.setlocale(locale.LC_TIME, 'pt_BR')
    quinzenas = []
    for pair in iterable:
        quinzena = pair.split(':')[1]
        mes = datetime.strftime(
            datetime.strptime(pair.split(':')[0], '%Y-%m'), '%B, %Y'
        )
        quinzenas.append('{0}ª quinzena de {1}.'.format(quinzena, mes))
    return quinzenas
