import json
import urllib.request
import os
import re
import boto3
from datetime import datetime
from urllib.parse import urlparse, parse_qs

client_id = os.environ['NAVER_CLIENT_ID']
client_secret = os.environ['NAVER_CLIENT_SECRET']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('NaverKinQuestions')


def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def format_date(date_string):
    date_obj = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S +0900")
    return date_obj.strftime("%Y년 %m월 %d일 %p %I시 %M분 %S초").replace("AM", "오전").replace("PM", "오후")


def get_timestamp(date_string):
    date_obj = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S +0900")
    return int(date_obj.timestamp())


def extract_id_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    dir_id = query_params.get('dirId', [''])[0]
    doc_id = query_params.get('docId', [''])[0]
    return f"{dir_id}_{doc_id}"


def lambda_handler(event, context):
    encText = urllib.parse.quote("핀다")
    url = f"https://openapi.naver.com/v1/search/kin?query={encText}&display=3&sort=date&adult=1"

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)

    try:
        response = urllib.request.urlopen(request)
        rescode = response.getcode()
        if rescode == 200:
            response_body = response.read()
            search_result = json.loads(response_body.decode('utf-8'))

            last_build_date = search_result.get('lastBuildDate', '')
            formatted_date = format_date(last_build_date)
            timestamp = get_timestamp(last_build_date)

            for item in search_result['items']:
                unique_id = extract_id_from_url(item.get('link', ''))
                table.put_item(
                    Item={
                        'id': unique_id,
                        'url': item.get('link', ''),
                        'last_build_date': formatted_date,
                        'timestamp': timestamp,
                        'title': clean_html(item.get('title', '')),
                        'description': clean_html(item.get('description', '')),
                        'proceed': False,
                        'is_related': None
                    }
                )

            return {
                'statusCode': 200,
                'body': json.dumps(f'처리 완료: {len(search_result["items"])}개의 항목 추가')
            }
        else:
            print(f"Error: Naver API returned status code {rescode}")
            return {
                'statusCode': rescode,
                'body': json.dumps(f"네이버 API 오류 발생: {rescode}")
            }
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"오류 발생: {str(e)}")
        }