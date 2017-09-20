import string
import collections
import csv

from nltk import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from pprint import pprint


def process_text(text, stem=True):
    """ Tokenize text and stem words removing punctuation """
    table = str.maketrans("", "", string.punctuation)
    text = text.translate(table)
    tokens = word_tokenize(text)

    if stem:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(t) for t in tokens]

    return tokens


def cluster_texts(texts, clusters=3):
    """ Transform texts to Tf-Idf coordinates and cluster texts using K-Means """
    vectorizer = TfidfVectorizer(tokenizer=process_text,
                                 stop_words=stopwords.words('english'),
                                 max_df=0.5,
                                 min_df=0.1,
                                 lowercase=True)

    tfidf_model = vectorizer.fit_transform(texts)
    km_model = KMeans(n_clusters=clusters)
    km_model.fit(tfidf_model)

    clustering = collections.defaultdict(list)

    for idx, label in enumerate(km_model.labels_):
        clustering[label].append(idx)

    return clustering


if __name__ == "__main__":
    base_dir = "/home/timothy/Projects/assignments/tia_text_analytics/data"
    output_comments_file = base_dir + '/staging/comments_dedup.csv'

    articles = []

    with open(output_comments_file, 'r') as f:
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL)
        headers = next(reader)
        pos = headers.index('excerpt')
        for row in reader:
            articles.append(row[pos])

    clusters = cluster_texts(articles, 7)
    pprint(dict(clusters))
