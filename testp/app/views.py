from django.shortcuts import render
from django.http import JsonResponse
from .models import List
import csv
from datetime import datetime
from django.db import connection

def file_upload(request):
    if request.method == 'POST':
        file = request.FILES['files']
        create_usr_id = request.POST.get('create_usr_id', '')
        schema = request.POST.get('schema', 'public')
        
        chunk_size = 1000 
        
        timesset = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        table_name = f"{schema}.master_study_list_{timesset}"
        
        with open(file.temporary_file_path(), 'r') as f:
            reader = csv.DictReader(f)
            while True:
                chunk = [row for _, row in zip(range(chunk_size), reader)]
                if not chunk:
                    break
                List.objects.bulk_create(
                    List(**row) for row in chunk
                )
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM {schema}.List WHERE 1=0;
            """)
        return JsonResponse({'message': 'Data saved successfully.'})
    return render(request, "index.html")
