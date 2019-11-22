from datetime import datetime
import csv
import re
# import platform
# import os
# import subprocess

import boto3
import prefect
import requests

# from datateer.tasks.util import get_root_dir

class HtmlToCsvTask(prefect.Task):
    def __init__(self, source_url=None, field_names_pattern=None, row_pattern=None, target_bucket=None, target_key=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_url = source_url
        self.field_names_pattern = field_names_pattern
        self.row_pattern = row_pattern
        self.target_bucket = target_bucket
        self.target_key = target_key
        self.validate()

    
    def run(self):
        html = self.download_html()
        headers = self.extract_field_names(html)
        rows = self.extract_rows(html)
        s3_object_path = self.write_csv_to_s3(headers, rows)
        return f'wrote {len(rows)} rows to {s3_object_path}'
    
    def download_html(self):
        self.logger.info(f'downloading HTML from {self.source_url}')
        r = requests.get(self.source_url)
        html = r.text
        self.logger.info(f'finished downloading HTML ({len(html)} characters)')
        return html


    def extract_field_names(self, html):
        self.logger.info(f'extracting field names from HTML')
        field_names = []
        match = re.search(self.field_names_pattern, html)
        for i in range(0,len(match.groups())):
            field_names.append(match.group(i + 1))
        self.logger.info(f'extracted {len(field_names)} field names')
        return field_names

    
    def extract_rows(self, html):
        self.logger.info(f'extracting fields from HTML')
        rows = re.findall(self.row_pattern, html)
        self.logger.info(f'extracted {len(rows)} rows with {len(rows[0])} fields each')
        return rows
    
    def write_csv_to_s3(self, headers, rows):
        key = self.construct_s3_key()
        target = f'{self.target_bucket}/{key}'
        self.logger.info(f'writing to S3 {target}')
        body = self.construct_csv(headers, rows)
        s3 = boto3.resource('s3')
        object = s3.Object(self.target_bucket, key)
        res = object.put(Body=body)
        self.logger.info(f'finished writing to {target}')
        return target

    def construct_csv(self, headers, rows):
        return ','.join(headers) + '\n' + '\n'.join([','.join(row) for row in rows]) 

    def construct_s3_key(self):
        '''Replace any supported named tokens. Supports {date}'''
        return self.target_key.format(date=datetime.now().strftime("%Y-%m-%d"))

    def validate(self):
        if not self.source_url:
            raise Exception('source_url is required') 
        if not self.field_names_pattern:
            raise Exception('field_names_pattern is required')
        if not self.row_pattern:
            raise Exception('row_pattern is required')
        if not self.target_bucket:
            raise Exception('target_bucket is required')
        if not self.target_key:
            raise Exception('target_key is required')
    