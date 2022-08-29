from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import sys

user = PasswordUser(models.User())
user.username = sys.argv[1].strip()
user.email = sys.argv[3].strip()
user.superuser = True
user.password = sys.argv[2].strip()
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()