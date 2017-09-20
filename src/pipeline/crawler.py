import urllib.parse
import time
import json
import requests
from enum import Enum
from os import listdir
from os.path import isfile, join


class TiaCrawler:

    BASE_URL = 'https://www.techinasia.com/wp-json/techinasia/2.0/posts'

    class DataType(Enum):
        POST = 1
        COMMENT = 2

    def __init__(self, directory, data_type: DataType, post_id=0, sleep_interval=10):
        self.directory = directory
        self.sleep_interval = sleep_interval
        self.data_type = data_type
        self.post_id = post_id

    def iterate_and_crawl(self, start=1):
        page = start
        total_pages = self.__request_and_write(page)
        page += 1

        while page <= total_pages:
            self.__request_and_write(page)
            page += 1

            if page % self.sleep_interval == 0:
                time.sleep(1)

    def __request_and_write(self, page):
        url = self.__create_url(page)
        response = self.__make_request(url)
        self.__write_to_file(self.__get_output_file_name(page), response)
        return self.__get_total_pages(response)

    def __make_request(self, url):
        try:
            r = requests.get(url, headers=self.__headers())
            return r.json()
        except RuntimeError:
            print(f'Unable to send request successfully. Url is {url}')

    def __write_to_file(self, file_name, json_contents):
        with open(self.directory + file_name, 'w') as f:
            json.dump(json_contents, f)

    def __create_url(self, page):
        params = self.__create_parameters(page)

        if self.data_type == self.DataType.POST:
            return self.BASE_URL + '?' + urllib.parse.urlencode(params)
        elif self.data_type == self.DataType.COMMENT:
            return self.BASE_URL + f'/{self.post_id}/comments?' + urllib.parse.urlencode(params)

    def __get_output_file_name(self, page):
        if self.data_type == self.DataType.POST:
            print(f'Crawling page {page}')
            return f'{page}'
        elif self.data_type == self.DataType.COMMENT:
            print(f'Crawling page {page} of post {self.post_id}')
            return f'{self.post_id}_{page}'

    @staticmethod
    def __get_total_pages(json_contents):
        return json_contents['total_pages']

    @staticmethod
    def __create_parameters(page):
        return {'page': page}

    @staticmethod
    def __headers():
        return {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36'}


class TiaDataMunger():

    def __init__(self, input_directory, output_file_name=""):
        self.input_directory = input_directory
        self.output_file_name = output_file_name

    def get_all_post_ids(self):
        post_ids = []
        records = 0
        all_file_counter = 0
        valid_file_counter = 0

        for file in self.__input_file_list():
            all_file_counter += 1

            with open(file) as f:
                json_obj = json.load(f)

                if 'posts' in json_obj:
                    valid_file_counter += 1
                    for item in json_obj['posts']:
                        post_ids.append(item['id'])
                        records += 1

        print(f'{records} post ids have been retrieved from {valid_file_counter} valid json files, out of a total of {all_file_counter} json files')
        return sorted(post_ids)

    def __input_file_list(self):
        files = [join(self.input_directory, f) for f in listdir(self.input_directory) if isfile(join(self.input_directory, f))]
        return sorted(files)


base_dir = "/home/timothy/Projects/assignments/tia_text_analytics/data/raw"


def crawl():
    posts_dir = base_dir + '/posts/'
    # post_crawler = TiaCrawler(posts_dir, TiaCrawler.DataType.POST)
    # post_crawler.iterate_and_crawl()

    munger = TiaDataMunger(posts_dir)
    ids = munger.get_all_post_ids()

    for post_id in ids:
        comments_crawler = TiaCrawler(base_dir + '/comments/', TiaCrawler.DataType.COMMENT, post_id=post_id)
        comments_crawler.iterate_and_crawl()


crawl()
