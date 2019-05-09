# coding: utf-8

import sys
import logging
from .settings import LOG_LEVEL
from datetime import datetime, timedelta


def singleton(cls, *args, **kwargs):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


def initLogger(level=None):
    '''初始化日志器
    '''
    level = level if level else LOG_LEVEL
    logging.basicConfig(level=level,
                        format='[%(asctime)s|%(levelname)-5s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def log_all_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    '''Log all uncaught exceptions in non-interactive mode.'''
    # ignore KeyboardInterrupt
    if not issubclass(exc_type, KeyboardInterrupt):
        logging.error("", exc_info=(exc_type, exc_value, exc_traceback))

    sys.__excepthook__(exc_type, exc_value, exc_traceback)
    return


class IPythonExceptionHandler:
    instance = None

    def __call__(self, *args, **kwargs):
        if self.instance is None:
            from IPython.core import ultratb
            self.instance = ultratb.FormattedTB(mode='Plain',
                                                color_scheme='Linux', call_pdb=1)
        return self.instance(*args, **kwargs)


def initDefaultExceptionHandler(handler='ipython'):
    if handler == 'ipython':
        sys.excepthook = IPythonExceptionHandler()
    else:
        sys.excepthook = log_all_uncaught_exceptions


def getQuarterOfYears(start_year, end_year):
    result = set()
    y = start_year
    while y <= end_year:
        result.add('%04d0331' % (y))
        result.add('%04d0630' % (y))
        result.add('%04d0930' % (y))
        result.add('%04d1231' % (y))
        y += 1
    return result


def getPeriodOfYears(start_year, end_year):
    return ('%04d0101' % (start_year), '%04d1231' % (end_year))


def getDateRange(start_day, end_day=None):
    if isinstance(start_day, str):
        start_day = datetime.strptime(start_day, "%Y%m%d")

    if isinstance(end_day, str):
        end_day = datetime.strptime(end_day, "%Y%m%d")

    if not end_day:
        current = datetime.now()
        end_day = datetime(current.year, current.month, current.day)
        end_day += timedelta(days=1)

    days = []
    while start_day < end_day:
        days.append(start_day.strftime("%Y%m%d"))
        start_day += timedelta(days=1)
    return days
