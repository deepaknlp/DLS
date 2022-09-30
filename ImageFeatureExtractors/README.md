# Medical Image Retrieval via Nearest Neighbor Search on Pre-trained Image Features
These are the experiments reported for ImageCLEF2011 dataset in our paper [Medical Image Retrieval via Nearest Neighbor Search on Pre-trained Image Features](https://arxiv.org/pdf/xxxx.yyyy.pdf)


Please install [Anaconda](https://www.anaconda.com/distribution/) to create a conda environment as follow:
```shell script
conda create -n iclef2011 python=3.8
conda activate iclef2011
pip install --upgrade pip
pip install -r requirements.txt
```

## Data Preparation
1) Download the image features from our [BioNLP server](https://bionlp.nlm.nih.gov/ImageCLEF2011/Features.tar.gz), unzip the file. The zip file contains the image features of CLEF2011 collections and query images.
2) Download the ImageCLEF2011 dataset from our [BioNLP server](https://bionlp.nlm.nih.gov/ImageCLEF2011/Images.tar.gz) , unzip the file. The zip file contains the images of CLEF2011. (**Optional**)
3) Download the ImageCLEF2011 query relevance file from our [BioNLP server](https://bionlp.nlm.nih.gov/ImageCLEF2011/qrel_2011_image_retrieval.txt).
4) Download the TREC eval script from [here](https://trec.nist.gov/trec_eval/trec_eval_latest.tar.gz), unzip the file and place the content in `eval/`

If you want to prepare your image features, please run the following command:

```shell script
cd src
python -u extract_features.py \
--image_dir=/path/to/training/image/directory \
--path_to_save_features=/path/to/save/image/collection/features \
--batch_size=128 \
--model=pre_trained_model_name/from/timm
python -u extract_features.py \
--image_dir=/path/to/query/image/directory \
--path_to_save_features=/path/to/save/query/image/features \
--batch_size=128 \
--from_query=True \
--model=pre_trained_model_name/from/timm
```


## Feature aggregation for ImageCLEF 2011

```shell script
cd src
python -u process_features.py \
--path_to_collection_features=/path/to/saved/image/collection/features \
--path_to_query_features=/path/to/saved/query/image/features
--pooling_strategy=channel_wise_weighting \
--follow_convnext_architecture=True \
--path_to_output_file=/path/to/save/output_file/for/evaluation \
--batch_size=1000
```
## Evaluation

```shell script
cd eval
./trec_eval -c path/to/query/relavance/file /path/to/saved/output_file/obtained/from/feature/aggregation
```