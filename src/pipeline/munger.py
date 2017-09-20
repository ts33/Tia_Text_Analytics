import json
import csv
from enum import Enum
from os import listdir
from os.path import isfile, join


class JsonToCsv:

    class DataType(Enum):
        POST = "posts"
        COMMENT = "comments"

    def __init__(self, input_directory, output_file_name, data_type: DataType, exclude=list()):
        self.input_directory = input_directory
        self.output_file_name = output_file_name
        self.data_type = data_type
        self.key_list = self._get_top_level_keys(exclude=exclude)

    def _get_top_level_keys(self, exclude=list()):
        key_list = set()

        for file in self._input_file_list():
            with open(file) as f:
                json_obj = json.load(f)
                if self.data_type.value in json_obj:
                    for item in json_obj[self.data_type.value]:
                        key_list.update(item.keys())

        for key in exclude:
            key_list.remove(key)

        key_list.remove('id')
        # push id to be the first column
        return ['id'] + list(key_list)

    def _get_sub_level_keys(self, key):
        key_list = set()

        for file in self._input_file_list():
            with open(file) as f:
                json_obj = json.load(f)
                if self.data_type.value in json_obj:
                    for item in json_obj[self.data_type.value]:
                        if key in item:
                            if isinstance(item[key], list):
                                for element in item[key]:
                                    key_list.update(element.keys())
                            else:
                                key_list.update(item[key].keys())

        return list(key_list)

    @staticmethod
    def _get_sub_level_headers(key, key_list):
        return [f'{key}_{k}' for k in key_list]

    @staticmethod
    def _json_to_arr(json_obj, key_list):
        return [json_obj.get(key, None) for key in key_list]

    def _input_file_list(self):
        files = [join(self.input_directory, f) for f in listdir(self.input_directory) if isfile(join(self.input_directory, f))]
        return sorted(files)


class CommentsJsonToCsv(JsonToCsv):

    def __init__(self, input_directory, output_file_name, exclude=list()):
        super().__init__(input_directory, output_file_name, JsonToCsv.DataType.COMMENT, exclude=exclude)
        self.author_key_list = self._get_sub_level_keys('author')

    def write_data_to_csv(self):
        # print(f'list of keys extracted: {key_list}')
        with open(self.output_file_name, 'w') as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            writer.writerow(self.key_list + self._get_sub_level_headers('author', self.author_key_list))
            self._write_to_csv(writer)

    def _write_to_csv(self, writer):
        records = 0

        for file in self._input_file_list():
            with open(file) as f:
                json_obj = json.load(f)
                if self.data_type.value in json_obj:
                    for item in json_obj[self.data_type.value]:
                        self._write(writer, item)
                        self._extract_children_and_replies(item, writer)
                        records += 1
        print(f'{records} have been printed')

    # recursively extract all children and replies for comments
    def _extract_children_and_replies(self, obj, writer):
        if 'children' in obj:
            for object in obj['children']:
                self._write(writer, object)
                self._extract_children_and_replies(object, writer)
        if 'replies' in obj:
            for object in obj['replies']:
                self._write(writer, object)
                self._extract_children_and_replies(object, writer)

    def _write(self, writer, object):
        standard_values = self._json_to_arr(object, self.key_list)
        author_values = self._json_to_arr(object.get('author', None), self.author_key_list)
        writer.writerow(standard_values + author_values)


class PostsJsonToCsv(JsonToCsv):

    def __init__(self, input_directory, output_file_name, exclude=list()):
        super().__init__(input_directory, output_file_name, JsonToCsv.DataType.POST, exclude=exclude)
        self.author_key_list = self._get_sub_level_keys('author')
        self.seo_key_list = self._get_sub_level_keys('seo')
        self.sponsor_key_list = self._get_sub_level_keys('sponsor')
        # keys for separate files
        self.category_key_list = self._get_sub_level_keys('categories')
        self.company_key_list = self._get_sub_level_keys('companies')
        self.tag_key_list = self._get_sub_level_keys('tags')

    def write_data_to_csv(self):
        # print(f'list of keys extracted: {key_list}')
        with open(self.output_file_name + '.csv', 'w') as posts_csv, \
                open(self.output_file_name + '_categories.csv', 'w') as categories_csv, \
                open(self.output_file_name + '_companies.csv', 'w') as companies_csv, \
                open(self.output_file_name + '_tags.csv', 'w') as tags_csv:

            self.post_writer = csv.writer(posts_csv, quoting=csv.QUOTE_ALL)
            self.category_writer = csv.writer(categories_csv, quoting=csv.QUOTE_ALL)
            self.company_writer = csv.writer(companies_csv, quoting=csv.QUOTE_ALL)
            self.tag_writer = csv.writer(tags_csv, quoting=csv.QUOTE_ALL)

            self.post_writer.writerow(self.key_list +
                            self._get_sub_level_headers('author', self.author_key_list) +
                            self._get_sub_level_headers('seo', self.seo_key_list) +
                            self._get_sub_level_headers('sponsor', self.sponsor_key_list)
                            )
            self.category_writer.writerow(self.category_key_list + ['post_id'])
            self.company_writer.writerow(self.company_key_list + ['post_id'])
            self.tag_writer.writerow(self.tag_key_list + ['post_id'])

            self._write_to_csv()

    def _write_to_csv(self):
        records = 0

        for file in self._input_file_list():
            with open(file) as f:
                json_obj = json.load(f)
                if self.data_type.value in json_obj:
                    for item in json_obj[self.data_type.value]:
                        self._write(item)
                        records += 1
        print(f'{records} have been printed')

    def _write(self, object):
        standard_values = self._json_to_arr(object, self.key_list)
        author_values = self._json_to_arr(object.get('author', None), self.author_key_list)
        seo_values = self._json_to_arr(object.get('seo', None), self.seo_key_list)
        sponsor_values = self._json_to_arr(object.get('sponsor', None), self.sponsor_key_list)
        self.post_writer.writerow(standard_values + author_values + seo_values + sponsor_values)

        self._extract_sub_array(object, 'categories', self.category_writer, self.category_key_list)
        self._extract_sub_array(object, 'companies', self.company_writer, self.company_key_list)
        self._extract_sub_array(object, 'tags', self.tag_writer, self.tag_key_list)

    def _extract_sub_array(self, obj, key, writer, key_list):
        if key in obj:
            for element in obj[key]:
                values = self._json_to_arr(element, key_list)
                writer.writerow(values + [obj['id']])


base_dir = "/home/timothy/Projects/assignments/tia_text_analytics/data"

#   find the keys of the sub-level,
#  write them to an external file, also include the post_id
def munge_posts():
    posts_dir = base_dir + '/raw/posts/'
    output_posts_file = base_dir + '/staging/posts'

    post_munger = PostsJsonToCsv(posts_dir, output_posts_file, exclude=['seo', 'sponsor', 'author', 'categories', 'companies', 'tags'])
    post_munger.write_data_to_csv()


def munge_comments():
    comments_dir = base_dir + '/raw/comments/'
    output_comments_file = base_dir + '/staging/comments.csv'
    output_comments_dedup_file = base_dir + '/staging/comments_dedup.csv'

    comment_munger = CommentsJsonToCsv(comments_dir, output_comments_file, exclude=['children', 'replies', 'author'])
    comment_munger.write_data_to_csv()

    # remove duplicate rows
    with open(output_comments_file, 'r') as f, open(output_comments_dedup_file, 'w') as out_file:
        writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL)
        identifiers = set()
        for row in reader:
            if row[0] not in identifiers:
                writer.writerow(row)
                identifiers.add(row[0])


munge_comments()
munge_posts()
