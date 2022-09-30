import torch
import random
import logging
import sys
import h5py
from sklearn.neighbors import NearestNeighbors
from operator import itemgetter
import argparse
import numpy as np
import os
import time
from pooling_functions import channel_pooling


def set_th_config(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True


# set pytorch configs
set_th_config(1234)
gt = time.time
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[logging.FileHandler("log.log", mode='a'),
                              logging.StreamHandler(sys.stdout)]
                    )


class BruteForceSearch():
    def __init__(self, metric):
        if metric not in ('angular', 'euclidean', 'hamming'):
            raise NotImplementedError(
                "BruteForce doesn't support metric %s" % metric)
        self._metric = metric
        self.name = 'BruteForce()'

    def fit(self, X):
        metric = {'angular': 'cosine', 'euclidean': 'l2',
                  'hamming': 'hamming'}[self._metric]
        print(f'======== Building BruteForce NN ==========')
        t = gt()
        self._nbrs = NearestNeighbors(
            algorithm='brute', metric=metric)
        self._nbrs.fit(X)
        b_t = gt() - t
        print(f'======== BruteForce Build Complete ==========')
        print("Building time:", b_t, "seconds")

    def query(self, v, n):
        print(f'======== Searching k-Nearest Neighbours using BruteForce ==========')
        t = gt()
        positions = list(self._nbrs.kneighbors(
            v, return_distance=False, n_neighbors=n))
        b_t = gt() - t
        print('======== Search Completed ==========')
        print("Query time - Exact:", b_t, "seconds")
        print("Query time - avg time per query:", b_t / len(v), "seconds")
        return positions

    def query_with_distances_and_positions(self, v, n):
        print(f'======== Searching k-Nearest Neighbours using BruteForce ==========')
        t = gt()
        (distances, positions) = self._nbrs.kneighbors(
            v, return_distance=True, n_neighbors=n)
        b_t = gt() - t
        print('======== Search Completed ==========')
        print("Query time - Exact:", b_t, "seconds")
        return list(positions), list(distances)


def load_and_process_text_data(dataset_path, is_test=False, flimit=-1, llimit=-1):
    image_ids = []
    image_features = []
    with open(dataset_path) as f:
        if is_test:
            for x in f:
                data_point = x.split('\t')
                image_name = os.path.basename(data_point[0])
                if image_name.endswith('_1.jpg'):
                    image_ids.append(image_name.replace('.jpg', ''))
                    image_features.append(data_point[1])
            dataset = np.loadtxt(image_features, delimiter=',')
        else:
            for x in f:
                data_point = x.split('\t')
                image_ids.append(os.path.basename(data_point[0].replace('.jpg', '')))
                image_features.append(data_point[1])
            dataset = np.loadtxt(image_features, delimiter=',')

    if flimit != -1:
        dataset = dataset[:flimit]
    elif llimit != -1:
        dataset = dataset[-llimit:]
    print(f"{len(dataset)} datapoints loaded from {dataset_path}")
    return dataset, image_ids


def load_and_process_h5_data(dataset_path, batch_size, from_channels=False, pooling_strategy='sum', sigmoid=False,
                             follow_convnext_architecture=False):
    hf = h5py.File(dataset_path, 'r')
    image_ids = hf.get('image_names')
    image_features = hf.get('image_features')

    if from_channels:
        print(f"Image feature shape {image_features.shape}")
        image_features = channel_pooling(image_features, device, batch_size, pooling_type=pooling_strategy,
                                         sigmoid=sigmoid, follow_convnext_architecture=follow_convnext_architecture)

    image_ids = image_ids[:].tolist()
    image_ids_str = [x.decode("utf-8").replace('.jpg', '') for x in image_ids]

    print(f"{len(image_features)} datapoints loaded from {dataset_path}")
    return image_features[:], image_ids_str


def do_brute_search(train_dataset, test_dataset, k=1000):
    bSearch = BruteForceSearch(metric='angular')
    bSearch.fit(train_dataset)
    positions, distances = bSearch.query_with_distances_and_positions(test_dataset, n=k)
    return positions, distances


def prepare_output_file(train_image_ids, test_image_ids, positions, distances, path_to_save):
    train_ids_4_test_id_list = {}
    train_dist_4_test_id_list = {}
    ranks_list = {}
    assert len(test_image_ids) == len(positions) == len(distances)

    for test_img_id, pos, dist in zip(test_image_ids, positions, distances):
        pos = pos.tolist()
        dist = dist.tolist()
        test_img_id = test_img_id.split('_')[0]
        train_ids_4_test_id = list(itemgetter(*pos)(train_image_ids))
        ranks = [x + 1 for x in range(len(pos))]
        train_ids_4_test_id_list[int(test_img_id)] = train_ids_4_test_id
        train_dist_4_test_id_list[int(test_img_id)] = dist
        ranks_list[int(test_img_id)] = ranks
    with open(path_to_save, 'w') as wfile:
        for index in range(len(test_image_ids)):
            test_img_id = index + 1
            for x, y, z in zip(train_ids_4_test_id_list[test_img_id], ranks_list[test_img_id],
                               train_dist_4_test_id_list[test_img_id]):
                score = 1 - z
                wfile.write(str(test_img_id) + '\t')
                wfile.write('Q0' + '\t')
                wfile.write(str(x) + '\t')
                wfile.write(str(y) + '\t')
                wfile.write(str(score) + '\t')
                wfile.write('CUR' + '\n')

    print(f"Output file has been saved for evaluation in {path_to_save}")


def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'


parser = argparse.ArgumentParser()
# data parameters

parser.add_argument('--path_to_collection_features',
                    type=str,
                    help="path to the processed image features of the collection"
                    )
parser.add_argument('--path_to_query_features',
                    help="path to the processed image features of the query"
                    )
parser.add_argument('--path_to_output_file',
                    help="path to the store the output file for the evaluation"
                    )
parser.add_argument('--from_channels', type=boolean_string,
                    default=True,
                    help="whether to perform the pooling operation from channel features"
                    )
parser.add_argument('--sigmoid', type=boolean_string,
                    default=False,
                    help="whether to use sigmoid activation function"
                    )
parser.add_argument('--follow_convnext_architecture', type=boolean_string,
                    default=False,
                    help="whether to use pre-trained norm-layer weights in case of ConvNeXt feature extractor."

                    )
parser.add_argument('--pooling_strategy', type=str,
                    default='channel_wise_weighting',
                    help="pooling strategy (sum | max | gem | channel_wise_weighting | spatial_wise_weighting)"

                    )
parser.add_argument('--batch_size', type=int,
                    default=1000)

configs = parser.parse_args()
print(configs)

if configs.path_to_collection_features.endswith('.txt'):
    train_dataset, train_image_ids = load_and_process_text_data(configs.path_to_collection_features)
    test_dataset, test_image_ids = load_and_process_text_data(configs.path_to_query_features, is_test=True)
else:
    train_dataset, train_image_ids = load_and_process_h5_data(configs.path_to_collection_features,
                                                              batch_size=configs.batch_size,
                                                              from_channels=configs.from_channels,
                                                              pooling_strategy=configs.pooling_strategy,
                                                              sigmoid=configs.sigmoid,
                                                              follow_convnext_architecture=configs.follow_convnext_architecture)
    test_dataset, test_image_ids = load_and_process_h5_data(configs.path_to_query_features,
                                                            batch_size=configs.batch_size,
                                                            from_channels=configs.from_channels,
                                                            pooling_strategy=configs.pooling_strategy,
                                                            sigmoid=configs.sigmoid,
                                                            follow_convnext_architecture=configs.follow_convnext_architecture)
positions, distances = do_brute_search(train_dataset, test_dataset, k=1000)
prepare_output_file(train_image_ids, test_image_ids, positions, distances, path_to_save=configs.path_to_output_file)
