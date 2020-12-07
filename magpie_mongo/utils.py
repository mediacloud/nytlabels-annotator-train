from __future__ import division

import cPickle as pickle
import os
import random
from collections import Counter, defaultdict

from magpie.base.document import Document


def save_to_disk(path_to_disk, obj, overwrite=False):
    """ Pickle an object to disk """
    dirname = os.path.dirname(path_to_disk)
    if not os.path.exists(dirname):
        raise ValueError("Path " + dirname + " does not exist")

    if not overwrite and os.path.exists(path_to_disk):
        raise ValueError("File " + path_to_disk + "already exists")

    pickle.dump(obj, open(path_to_disk, 'wb'))


def load_from_disk(path_to_disk):
    """ Load a pickle from disk to memory """
    if not os.path.exists(path_to_disk):
        raise ValueError("File " + path_to_disk + " does not exist")

    return pickle.load(open(path_to_disk, 'rb'))


def get_documents(data_dir, as_generator=True, shuffle=False):
    """
    Extract documents from *.txt files in a given directory
    :param data_dir: path to the directory with .txt files
    :param as_generator: flag whether to return a document generator or a list
    :param shuffle: flag whether to return the documents
    in a shuffled vs sorted order

    :return: generator or a list of Document objects
    """
    files = list({filename[:-4] for filename in os.listdir(data_dir) if filename.endswith(".txt")})
    files.sort()
    if shuffle:
        random.shuffle(files)

    generator = (Document(doc_id, os.path.join(data_dir, f + '.txt'))
                 for doc_id, f in enumerate(files))
    return generator if as_generator else list(generator)

def get_documents_from_mongo(ids, mongo_collection, as_generator=True, shuffle=False):
    """
    Extract documents from *.txt files in a given directory
    :param data_dir: path to the directory with .txt files
    :param as_generator: flag whether to return a document generator or a list
    :param shuffle: flag whether to return the documents
    in a shuffled vs sorted order

    :return: generator or a list of Document objects
    """
    print ("get document from mongo!")
    if shuffle:
        random.shuffle(ids)

    docs_step = 500000
    steps_times = len(ids)//docs_step
    steps = [docs_step * i for i in range(steps_times+1)]+[len(ids)]
    cursors = [mongo_collection.find({"_id": {"$in": ids[steps[i-1]:steps[i]]}}) for i in range(1, len(steps))]
    all_docs = (x for c in cursors for x in c)
    generator = (Document(doc_id, None, text=d["full_text"]) for doc_id, d in enumerate(all_docs))
    return generator if as_generator else list(generator)


def get_answers_for_doc(doc_name, data_dir, labels_arr=None, filtered_by=None):
    """
    Read ground_truth answers from a .key file corresponding to the doc_name
    :param doc_name: the name of the document, should end with .txt
    :param data_dir: directory in which the documents and answer files are
    :param filtered_by: whether to filter the answers. Both sets and ontologies
           can be passed as filters

    :return: set of unicodes containing answers for this particular document
    """
    if labels_arr:
        answers = labels_arr
    else:
        filename = os.path.join(data_dir, doc_name[:-4] + '.lab')

        if not os.path.exists(filename):
            raise ValueError("Answer file " + filename + " does not exist")

        with open(filename, 'rb') as f:
            answers = {line.decode('utf-8').rstrip('\n') for line in f}

    if filtered_by:
        answers = {kw for kw in answers if kw in filtered_by}

    return answers
