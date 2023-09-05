from django import forms

from ReachRoster.models import Company_details


class CompanyDetails_Form(forms.ModelForm):
    class Meta:
        model = Company_details
        fields = '__all__'

    excel_csv_file = forms.FileField(
        label='Upload CSV or Excel File',
        widget=forms.ClearableFileInput(attrs={
            'accept': '.csv'}),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.required = False
