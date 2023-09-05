import os
import random
import string
import time

import pandas as pd
import pymysql
from django.conf import settings
from django.core.management import call_command
from django.db import connection
from django.db import migrations
from django.db import models
from django.db import transaction
from django.shortcuts import render

from ReachRoster.forms import CompanyDetails_Form


def generate_random_name():
    letters = string.ascii_lowercase
    name_length = random.randint(6, 7)
    random_name = ''.join(random.choice(letters) for _ in range(name_length))
    return random_name


def add_company(request):
    if request.method == 'POST':
        django_ = False
        check = request.POST.get('names')
        if check == 'random':
            random_name = generate_random_name()
        else:
            random_name = request.POST.get('table_name')
        form = CompanyDetails_Form(request.POST, request.FILES)
        if form.is_valid():
            form.save()

        csv_file = form.instance.excel_csv_file.file
        try:
            df = pd.read_csv(csv_file.name).fillna('')
        except Exception as e:
            print(f'Pandas Error = {e}')
            try:
                df = pd.read_csv(csv_file.name, encoding='latin-1')
            except Exception as e:
                print(f'Pandas latin Error = {e}')
                return render(request, 'add_csv.html', {'Table_Name': "Not Created"})
        headers = []
        for k in df.items():
            key = k[0]
            headers.append(key)

        if django_:
            """
                Create Table / Model Dynamic Using Django Models
            """
            try:
                create_model_django(random_name, df, headers)
                query = f"SELECT * FROM {database}.DynamicModel_{random_name};"
            except Exception as e:
                print(f'Create Model Django Error = {e}')
                return render(request, 'add_csv.html', {'Table_Name': "Not Created"})

        else:
            """
               Create Table / Model Dynamic Using SQL Query
            """
            try:
                create_model_sql(random_name, df, headers)
                query = f"SELECT * FROM {database}.{random_name};"
            except Exception as e:
                print(f'create Model Sql Error = {e}')
                return render(request, 'add_csv.html', {'Table_Name': "Not Created"})

        return render(request, 'add_csv.html',
                      {'Table_Name': random_name, 'query': query})
    else:
        form = CompanyDetails_Form()
    return render(request, 'add_csv.html', {'form': form})


def create_model_django(random_name, df, headers):
    with transaction.atomic():
        cursor = connection.cursor()
        cursor.execute("DELETE FROM django_migrations WHERE app='testing'")
        cursor.close()
        try:
            migration_folder = os.path.join(settings.BASE_DIR, 'testing', 'migrations')

            for file in os.listdir(migration_folder):
                if file != '__init__.py':
                    try:
                        os.remove(os.path.join(migration_folder, file))
                    except:
                        pass
        except:
            pass
    transaction.commit()
    with transaction.atomic():
        class DynamicModel(models.Model):
            id = models.AutoField(primary_key=True)

            class Meta:
                app_label = 'testing'
                db_table = f'DynamicModel_{random_name}'

        for header in headers:
            field = models.TextField(max_length=16000)
            DynamicModel.add_to_class(header, field)

        migrations.RunPython(lambda apps, schema_editor: DynamicModel.create_table())
    transaction.commit()

    try:
        call_command('makemigrations', 'testing')
    except Exception as e:
        print(f'Makemigrations Error = {e}')
    try:
        call_command('migrate', 'testing')
    except Exception as e:
        print(f'Migrate Error = {e}')

    try:
        time.sleep(2)
        for i, row in df.iterrows():
            kwargs = {}
            for header in headers:
                kwargs[header] = row[header]

            DynamicModel.objects.create(**kwargs)
    except Exception as e:
        print(f'Create Error = {e}')
    with transaction.atomic():
        cursor = connection.cursor()
        cursor.execute("DELETE FROM django_migrations WHERE app='testing'")
        cursor.close()
        try:
            migration_folder = os.path.join(settings.BASE_DIR, 'testing', 'migrations')

            for file in os.listdir(migration_folder):
                if file != '__init__.py':
                    try:
                        os.remove(os.path.join(migration_folder, file))
                    except:
                        pass
        except:
            pass

    transaction.commit()
    print(f'Random Name = {random_name}')

    """
        RENAME TABLE
    """
    cursor = connection.cursor()
    cursor.execute(
        f"ALTER TABLE dynamicmodel_{random_name} RENAME TO {random_name}"
    )
    cursor.close()


def connection_db():
    global database
    host = settings.DATABASES['default']['HOST']
    port = settings.DATABASES['default']['PORT']
    user = settings.DATABASES['default']['USER']
    passwd = settings.DATABASES['default']['PASSWORD']
    database = settings.DATABASES['default']['NAME']
    con = pymysql.connect(host=host, port=port, user=user, passwd=passwd, database=database)
    return con


def create_model_sql(random_name, df, headers):
    conn = connection_db()
    cursor = conn.cursor()

    # Create columns string
    columns = ", ".join([name + " LONGTEXT" for name in headers])

    # Create table
    create_query = f"CREATE TABLE if not exists {random_name} ({columns})"
    cursor.execute(create_query)

    # Insert Data
    try:
        insert_query = f"INSERT IGNORE INTO {random_name} ({', '.join([i for i in headers])}) VALUES ({', '.join(['%s' for i in headers])})"
        temp_data = []
        for i, row in df.iterrows():
            kwargs = []
            for header in headers:
                kwargs.append(row[header])
            data_test = tuple(kwargs)
            temp_data.append(data_test)
        cursor.executemany(insert_query, temp_data)
    except Exception as e:
        print(f'Insert Data Error = {e}')

    conn.commit()
    cursor.close()
    conn.close()


# RESTART SERVER
def start_server():
    import subprocess

    activate_env = os.path.join(settings.BASE_DIR, 'rosterenv\\Scripts\\activate')
    subprocess.Popen(activate_env, shell=True).wait()

    path_manage = os.path.join(settings.BASE_DIR, 'manage.py')
    run_server = f"python {path_manage} runserver"
    subprocess.Popen(run_server, shell=True).wait()
