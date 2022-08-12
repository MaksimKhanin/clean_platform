import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import configparser

config = configparser.ConfigParser()
config.read('build.ini')

user = PasswordUser(models.User())
user.username = config['AIRFLOW_SETTINGS']['AIRFLOW_ADMIN_USER']
user.email = config['AIRFLOW_SETTINGS']['AIRFLOW_ADMIN_PASSWORD']
user.superuser = True
user.password = config['AIRFLOW_SETTINGS']['AIRFLOW_ADMIN_PASSWORD']
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()