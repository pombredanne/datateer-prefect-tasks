from datetime import datetime
import os

from datateer.tasks.DbtTask import generate_command_list

from datateer.tasks.HtmlToCsvTask import HtmlToCsvTask

HTML = '''
<html>
<body>
<table>
    <tr><th>header 1</th><th>header 2</th></tr>
    <tr><td>row 1 col 1</td><td>row 1 col 2</td></tr>
    <tr><td>row 2 col 1</td><td>row 2 col 2</td></tr>
</table>
</body>
</html>
'''
FIELD_NAMES_PATTERN = '<tr><th>(.*?)<\/th><th>(.*?)<\/th><\/tr>'
ROW_PATTERN = '<tr><td>(.*?)<\/td><td>(.*?)<\/td><\/tr>'
SOURCE_URL = 'https://unifirst7s.avature.net/PublicLists/miNmEldxHgTvu6cmjz7FbXZhBbiu4q14'
TARGET_BUCKET = 'dev-mrp-pipeline-raw'
TARGET_KEY = 'raw-data/ufst/avature/crm_req_active/business_date={date}/{date}.csv'


def instantiate_task():
    return HtmlToCsvTask(source_url=SOURCE_URL, field_names_pattern=FIELD_NAMES_PATTERN, row_pattern=ROW_PATTERN, target_bucket=TARGET_BUCKET, target_key=TARGET_KEY)


def test_extract_field_names():
    task = instantiate_task()
    headers = task.extract_field_names(HTML)
    assert len(headers) == 2
    assert headers[0] == 'header 1'
    assert headers[1] == 'header 2'


def test_extract_rows():
    task = instantiate_task()
    rows = task.extract_rows(HTML)
    assert len(rows) == 2
    assert rows[0][0] == 'row 1 col 1'
    assert rows[0][1] == 'row 1 col 2'
    assert rows[1][0] == 'row 2 col 1'
    assert rows[1][1] == 'row 2 col 2'


def test_download_html():
    task = instantiate_task()
    html = task.download_html()
    assert len(html) > 0
    assert '<table' in html

def test_construct_csv():
    task = instantiate_task()
    headers = ['header1', 'header2']
    rows = [
        ['field1', 'field2'],
        ['field3', 'field4']
    ]
    body = task.construct_csv(headers, rows)
    assert 'header1,header2' in body
    assert 'field1,field2' in body
    assert 'field3,field4' in body

def test_construct_s3_key():
    task = instantiate_task()
    key = task.construct_s3_key()
    assert key == f'raw-data/ufst/avature/crm_req_active/business_date={datetime.now().strftime("%Y-%m-%d")}/{datetime.now().strftime("%Y-%m-%d")}.csv'

def test_run():
    task = instantiate_task()
    task.field_names_pattern = '<tr><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th><th>(.*?)<\/th>.*?<\/tr>'
    task.row_pattern = '<tr><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td><td>(.*?)<\/td>.*?<\/tr>'
    print('env is ' + os.environ['AWS_PROFILE'])
    task.run()

