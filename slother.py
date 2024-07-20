import logging
import requests
import gzip
import io

class CommonCrawlQuery:
    pass

def get_files(search_term):
    url = 'http://index.commoncrawl.org/CC-MAIN-2024-26-index?url=reddit.com&output=json'

def get_index(index):
    url = 'http://index.commoncrawl.org/'
    response = requests.get(url + 'collinfo.json').json()
    return response


def get_query_results():
    data_url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-26/segments/1718198861173.16/warc/CC-MAIN-20240612140424-20240612170424-00335.warc.gz'
    web_response = requests.get(data_url, timeout=30, stream=True)
    csv_gz_file = web_response.content 
    f = io.BytesIO(csv_gz_file)
    with gzip.GzipFile(fileobj=f) as f:
        for line in f:
            print('got line', line)



class CommonCrawlUrlQuery:
    def __init__(self, archive=None) -> None:
        self.base_data_url = 'https://data.commoncrawl.org'

    def get_file_list(self):
        url = f'{self.base_data_url}/crawl-data/CC-MAIN-2024-26/cc-index.paths.gz'
        files = [x.decode('utf8').replace('\n','') for x in self.read_gzip_resource(url)]
        return files

    def read_file(self, file_path):
        url = f'{self.base_data_url}/{file_path}'
        for result in self.read_gzip_resource(url):
            print(result)

    def read_gzip_resource(self, url, url_params=None):
        web_response = requests.get(
            url, params=url_params, timeout=30, stream=True)
        csv_gz_file = web_response.content 
                                    
        fileobj = io.BytesIO(csv_gz_file)
        with gzip.GzipFile(fileobj=fileobj) as file_lines:
            for line in file_lines:
                yield line

client = CommonCrawlUrlQuery()
file = client.get_file_list()[0]
client.read_file(file)

