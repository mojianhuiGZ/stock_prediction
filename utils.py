# coding: utf-8

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from settings import LOG_LEVEL, LOG_PATH, LOG_FILE


def initializeLogger():
    '''初始化日志器
    '''
    logFile = os.path.join(LOG_PATH, LOG_FILE)
    logFormat = '[%(levelname)-7s] (%(asctime)s %(filename)s line:%(lineno)d)\n%(message)s'

    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    handler = RotatingFileHandler(logFile, mode='a', encoding='utf-8')
    handler.setLevel(LOG_LEVEL)
    handler.setFormatter(logging.Formatter(logFormat, '%Y-%m-%d:%H:%M:%S'))
    logger.addHandler(handler)

    handler = logging.StreamHandler()
    handler.setLevel(LOG_LEVEL)
    handler.setFormatter(logging.Formatter(logFormat, '%Y-%m-%d:%H:%M:%S'))
    logger.addHandler(handler)

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

def initializeDefaultExceptionHandler(handler='ipython'):
    if handler == 'ipython':
        sys.excepthook = IPythonExceptionHandler()
    else:
        sys.excepthook = log_all_uncaught_exceptions
