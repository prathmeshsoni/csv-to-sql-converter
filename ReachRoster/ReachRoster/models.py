from django.contrib.auth.models import User
from django.db import models


class Company_details(models.Model):
    excel_csv_file = models.FileField(upload_to='datas/', blank=True, null=True)

    def __str__(self):
        return self.excel_csv_file
