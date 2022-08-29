import re
import sys

def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j.rstrip())
    return text

cfg_name = "airflow.cfg"
new_lines = []
pattern = r'(\$\{.*?\})'

DICT_PARAM = {
    "${POSTGRES_USER}" : sys.argv[1],
    "${POSTGRES_PASSWORD}": sys.argv[2],
    "${POSTGRES_HOST}": sys.argv[3],
    "${POSTGRES_PORT}": sys.argv[4],
    "${AIRFLOW_SECRET_KEY}": sys.argv[5],
}

with open("airflow.cfg") as f:
    lines = f.readlines()
    for line in lines:
        new_line = line
        finded = re.findall(pattern, new_line)
        if len(finded) > 0:
            new_line = replace_all(new_line, DICT_PARAM)
        new_lines.append(new_line)

with open(cfg_name, 'w') as new_config:
    new_config.writelines(new_lines)
