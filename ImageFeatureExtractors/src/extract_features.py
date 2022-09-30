import torch
import timm
import random
import logging
import sys
from tqdm.auto import tqdm
import glob
import h5py
from PIL import Image
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from timm.data import resolve_data_config
from timm.data.transforms_factory import create_transform

import argparse
import numpy as np
import os
import time


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


class ImageCLEFDataset(Dataset):
    """ImageCLEF dataset."""

    def __init__(self, path_to_images, transform, path_to_image_ids=None, from_query=False):
        """

        """
        self.path_to_images = path_to_images
        self.transform = transform
        if path_to_image_ids is not None:
            file_names = []
            with open(path_to_image_ids, 'r') as rfile:
                for image_base_path in rfile:
                    image_base_path = image_base_path.strip().replace('./', '')
                    file_names.append(os.path.join(self.path_to_images, image_base_path))
            self.file_names = file_names
        else:
            if from_query:
                self.file_names = glob.glob(os.path.join(self.path_to_images, '*_1.jpg'))
            else:
                self.file_names = glob.glob(os.path.join(self.path_to_images, 'PMC*/*.jpg'))
        # self.file_names=self.file_names[:100]

    def __len__(self):
        return len(self.file_names)

    def __getitem__(self, idx):
        try:
            image_reps = Image.open(self.file_names[idx])
            if len(image_reps.getbands()) == 1:
                image_reps = image_reps.convert("RGB")
            image_input = self.transform(image_reps)
            image_name = os.path.basename(self.file_names[idx])
            image_dir = os.path.basename(self.file_names[idx].replace('/' + image_name, ''))
            return {'image_name': image_name, 'image_dir': image_dir, 'image_input': image_input}
        except Exception as e:
            print(e)
            return None


def collate_fn(batch):
    batch = list(filter(lambda x: x is not None, batch))
    return torch.utils.data.dataloader.default_collate(batch)


class ImageFeatureExtraction():
    def __init__(
            self,
            model_name,
            batch_size=128,
            out_indices=None,
            from_query=False
    ):
        self.model_name = model_name
        self.batch_size = batch_size
        self.from_query = from_query
        self.out_indices = out_indices

    def build_model(self):
        logging.info(f' Building {self.model_name} from pre-trained weight')
        if self.out_indices is not None:
            self.model = timm.create_model(model_name=self.model_name, pretrained=True, features_only=True,
                                           out_indices=self.out_indices, num_classes=0)
        else:
            self.model = timm.create_model(model_name=self.model_name, pretrained=True, num_classes=0)

        self.model = self.model.to(device)
        config = resolve_data_config({}, model=self.model)
        self.transform = create_transform(**config)
        logging.info(f' {self.model_name} model loaded')
        self.model = self.model.half()
        o = self.model(torch.randn(1, 3, 224, 224).to(device).half())
        if self.out_indices is not None:
            output = nn.AdaptiveAvgPool2d(1)(o[0]).squeeze(-1).squeeze(-1)
            self.feature_dimension = output.shape[-1]

    def extract_features_using_dataloader(self, path_to_images, path_to_save_features, path_to_image_ids=None):
        imageclef_dataset = ImageCLEFDataset(path_to_images=path_to_images, transform=self.transform,
                                             path_to_image_ids=path_to_image_ids, from_query=self.from_query)
        train_dataloader = DataLoader(imageclef_dataset, batch_size=self.batch_size, shuffle=False,
                                      collate_fn=collate_fn)

        imageNameList = []
        imageDirList = []
        batchImageArray = np.empty([0, self.feature_dimension], float)
        print(f"Data loader length: {len(train_dataloader)}")
        save_interval = len(train_dataloader) / 3
        count = 0
        for batch in tqdm(train_dataloader):
            imageNames = batch['image_name']
            imageDirs = batch['image_dir']
            imageReps = batch['image_input'].to(device).half()
            imageFeatures = self.model(imageReps)
            if self.out_indices is not None:
                imageFeatures = nn.AdaptiveAvgPool2d(1)(imageFeatures[0]).squeeze(-1).squeeze(-1)
            imageFeatures = imageFeatures.cpu().detach().numpy()
            batchImageArray = np.append(batchImageArray, imageFeatures, axis=0)
            imageNameList.extend(imageNames)
            imageDirList.extend(imageDirs)
            count += 1
            if count % len(train_dataloader) == 0:
                hf = h5py.File(path_to_save_features, 'w')
                hf.create_dataset('image_dirs', data=imageDirList)
                hf.create_dataset('image_names', data=imageNameList)
                hf.create_dataset('image_features', data=batchImageArray)
                hf.close()
        hf = h5py.File(path_to_save_features, 'w')
        hf.create_dataset('image_dirs', data=imageDirList)
        hf.create_dataset('image_names', data=imageNameList)
        hf.create_dataset('image_features', data=batchImageArray)
        hf.close()


class ImageFeatureExtractionChannelWise():
    def __init__(
            self,
            model_name,
            batch_size=128,
            from_query=False
    ):
        self.model_name = model_name
        self.batch_size = batch_size
        self.from_query = from_query

    def build_model(self):
        logging.info(f' Building {self.model_name} from pre-trained weight')
        self.model_4_class_weight = timm.create_model(model_name=self.model_name, pretrained=True)
        self.model_4_class_weight = self.model_4_class_weight.to(device)
        self.model_4_class_weight = self.model_4_class_weight.half()
        self.model_4_class_weight.eval()
        self.model = timm.create_model(model_name=self.model_name, features_only=True, pretrained=True)
        config = resolve_data_config({}, model=self.model)
        self.transform = create_transform(**config)
        logging.info(f' {self.model_name} model loaded')
        self.model = self.model.to(device).half()
        self.model.eval()
        outputs = self.model(torch.randn(1, 3, 224, 224).to(device).half())
        shape = None
        for x in outputs:
            shape = x.shape
        self.feature_dimension = shape

    def extract_features_using_dataloader(self, path_to_images, path_to_save_features, path_to_image_ids=None):
        imageclef_dataset = ImageCLEFDataset(path_to_images=path_to_images, transform=self.transform,
                                             path_to_image_ids=path_to_image_ids, from_query=self.from_query)
        train_dataloader = DataLoader(imageclef_dataset, batch_size=self.batch_size, shuffle=False,
                                      collate_fn=collate_fn)

        imageNameList = []
        imageDirList = []
        weights_fc = self.model_4_class_weight.head.fc.state_dict()['weight']
        weights_fc = np.transpose(weights_fc.cpu().numpy(), (1, 0))
        batchImageArray = np.empty([0, self.feature_dimension[1], self.feature_dimension[2], self.feature_dimension[3]],
                                   float)

        print(f"Data loader length: {len(train_dataloader)}")
        save_interval = len(train_dataloader) / 3
        count = 0
        hf = h5py.File(path_to_save_features, 'w')
        hf.create_dataset('weights_classifier', data=weights_fc)

        for batch in tqdm(train_dataloader):
            imageNames = batch['image_name']
            imageDirs = batch['image_dir']
            imageReps = batch['image_input'].to(device).half()

            imageFeatures = self.model(imageReps)
            imageFeatures = imageFeatures[-1].cpu().detach().numpy()

            if count == 0:
                batchImageArray = imageFeatures
            else:
                batchImageArray = np.append(batchImageArray, imageFeatures, axis=0)

            imageNameList.extend(imageNames)
            imageDirList.extend(imageDirs)

            count += 1

            if count == 1:
                hf.create_dataset('image_features', data=batchImageArray, compression="gzip", chunks=True, maxshape=(
                None, self.feature_dimension[1], self.feature_dimension[2], self.feature_dimension[3]))
                batchImageArray = np.empty(
                    [0, self.feature_dimension[1], self.feature_dimension[2], self.feature_dimension[3]], float
                )
            elif count % 10 == 0:
                hf['image_features'].resize((hf['image_features'].shape[0] + batchImageArray.shape[0]), axis=0)
                hf['image_features'][-batchImageArray.shape[0]:] = batchImageArray
                batchImageArray = np.empty(
                    [0, self.feature_dimension[1], self.feature_dimension[2], self.feature_dimension[3]], float
                )

        if batchImageArray.shape[0] != 0:
            hf['image_features'].resize((hf['image_features'].shape[0] + batchImageArray.shape[0]), axis=0)
            hf['image_features'][-batchImageArray.shape[0]:] = batchImageArray
        hf.create_dataset('image_names', data=imageNameList)
        hf.create_dataset('image_dirs', data=imageDirList)
        hf.close()
        print("Feature extraction complete.")

def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'

parser = argparse.ArgumentParser()
parser.add_argument('--image_dir',
                    type=str,
                    help='path to the directory of the ImageCLEF 2011 images')

parser.add_argument('--path_to_image_ids',
                    type=str,
                    help='path to the file having imageIDs')

parser.add_argument('--path_to_save_features',
                    help='path to save ImageCLEF 2011 image features'
                    )

parser.add_argument('--batch_size',
                    type=int,
                    default=32,
                    help='batch size')

parser.add_argument('--out_indices',
                    type=tuple,
                    default=(2,),
                    help='to select feature maps from timm (https://rwightman.github.io/pytorch-image-models/feature_extraction/)')

parser.add_argument('--extract_channel_features',
                    type=boolean_string,
                    default=True,
                    help='whether to extract channel features')

parser.add_argument('--from_query',
                    type=boolean_string,
                    default=True,
                    help='whether to generate features for the query images')

parser.add_argument('--model',
                    type=str,
                    default='convnext_large_in22k',
                    help="pretrained image model name")

configs = parser.parse_args()

if configs.extract_channel_features:
    obj = ImageFeatureExtractionChannelWise(model_name=configs.model, batch_size=configs.batch_size,
                                            from_query=configs.from_query
                                            )
else:
    obj = ImageFeatureExtraction(model_name=configs.model, batch_size=configs.batch_size,
                                 from_query=configs.from_query,
                                 out_indices=configs.out_indices
                                 )

dir_name = os.path.dirname(configs.path_to_save_features)
if not os.path.exists(dir_name):
    os.makedirs(dir_name, exist_ok=True)

obj.build_model()
if configs.from_query:
    obj.extract_features_using_dataloader(configs.image_dir,
                                          configs.path_to_save_features
                                          )
else:
    obj.extract_features_using_dataloader(configs.image_dir,
                                          configs.path_to_save_features,
                                          configs.path_to_image_ids
                                          )
