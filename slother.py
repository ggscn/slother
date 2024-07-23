from multiprocessing import Pool
from functools import reduce
from operator import concat
from utils import read_gzip_resource
import tldextract
import logging
import requests
import gzip
import io
import ast


class CommonCrawlPageQuery:
    def __init__(self, archive=None) -> None:
        pass

    def get_files(search_term):
        url = 'http://index.commoncrawl.org/CC-MAIN-2024-26-index?url=reddit.com&output=json'

    def get_index(index):
        url = 'http://index.commoncrawl.org/'
        response = requests.get(url + 'collinfo.json').json()
        return response

    def get_query_results():
        data_url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-26/segments/1718198861173.16/warc/CC-MAIN-20240612140424-20240612170424-00335.warc.gz'
        read_gzip_resource(data_url)



class CommonCrawlUrlQuery:
    def __init__(self, archive=None) -> None:
        self.base_data_url = 'https://data.commoncrawl.org'

    def build_queue(self):
        return [f'{self.base_data_url}/{file}' 
            for file in self.get_file_list()]

    def get_file_list(self):
        url = f'{self.base_data_url}/crawl-data/CC-MAIN-2024-26/cc-index.paths.gz'
        return [x.decode('utf8').replace('\n','') 
            for x in read_gzip_resource(url)]

    def read_file(self, file_path):
        url = f'{self.base_data_url}/{file_path}'
        for result in read_gzip_resource(url):
            print(result)

    def multiprocess_queue(self, queue, job, processes=5):
        pool = Pool(processes=processes)
        result = reduce(concat, pool.map(job, queue))
        pool.close()
        print(len(result))

    def run(self, job_config = None):
        job = job_config.job
        queue = self.build_queue()
        self.multiprocess_queue(queue, job)

    


class UniqueURLJob:
    def __init__(self, job_name=None) -> None:
        pass

    def read_file(self, file_url):
        for domain_info in read_gzip_resource(file_url): 
            try:
                domain_info_dict = ast.literal_eval(
                    '{' + domain_info.decode('utf8').replace('\n','').split(' {')[1])
            except Exception as e:
                print(e, domain_info.decode('utf8').replace('\n',''))
            yield domain_info_dict

    def list_all(self, url):
        domains = self.read_file(url)
        return domains

    def extract_domain(self, domain_info):
        domain = None
        try:
            domain = tldextract.extract(
                domain_info['url']).registered_domain
        except Exception as e:
            print(e)
        return domain


    def job(self, url):
        try:
            domains = list(set([ 
                self.extract_domain(x) for x in self.read_file(url)]))
        except Exception as e:
            print(e)
        print(len(domains))
        return domains

job_config = UniqueURLJob()
CommonCrawlUrlQuery().run(job_config)
