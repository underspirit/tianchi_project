import os
import logging
import logging.config

project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
logger_conf_path = project_path + '/conf/logging.conf'
logging.config.fileConfig(logger_conf_path)
logger = logging.getLogger('tianchi')

#logger.info('logger start')
