# Medical Image Retrieval via Nearest Neighbor Search on Pre-trained Image Features


## Prerequisites
* Install the latest version of [Java](https://java.com).
* You may need to set your `JAVA_HOME`.
* Create `jar` file using `jar cvf hiD.jar -C hiD/ .`

## Data Preparation
* Download the OpenI image features from our [BioNLP server](https://bionlp.nlm.nih.gov/features/openi/). The directory contains the image features of OpenI datasets.
* Place the `train.txt` and `test.text` feature files from the respective pretrained image model in the `Sources` directory.

## Running the code
* **Building dataset**

```shell script
java -Xmx4G -classpath hiD.jar hiD.data.BuildDataSet ./Sources/test.txt true
java -Xmx4G -classpath hiD.jar hiD.data.BuildDataSet ./Sources/train.text true
```
The script will produce the binary datasets in `DataSets` directory as `test_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs` and `train_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs`


The BuildDataSet program takes the source file name as a parameter.  
A 2nd optional parameter is a boolean flag (defaults to true) that indicates whether the data should be normalized.
Normalization performs two transformations on the data:
  1) subtract out the mean which centers the dataset at the origin.
  2) scale by a factor that makes the overall variance equal the number of dimensions.  

The scaling makes the average variance per component equal to 1, which simplifies analysis and comparison of results for different number of dimensions
The mean and the variance both are included in the binary dataset so the normalization can be undone and the original data recovered

* **Building index**
```shell script
java -Xmx6G -classpath hiD.jar hiD.index.BuildIndex ./DataSets/train_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs <K_INDEX>
```
The script will produce the binary index in `Indexes` directory as `train_<FEATURE_DIMS>D_<DATASET_SIZE>_<K_INDEX>Nr.ndx`

The BuildIndex program requires two parameters: 
1) the dataset file name, and
2) the number of nearest neighbors to include in the index.

If the dataset is not found with the filename, the program replaces path and file type with the expected DataSet directory path and the .vecs filetype.

* **Finding nearest neighbors using BruteSearch**
```shell script
  java -Xmx4G -classpath hiD.jar hiD.search.TimeBruteSearch  ./DataSets/train_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs ./DataSets/test_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs 10 true
```
The TimeBruteSearch program requires four parameters: 
  1) the dataset to be searched file name 
  2) the dataset of test queries file name
  3) the desired number of nearest neighbors to find
  4) an optional boolean parameter (defaults to false) that indicates whether the results should include duplicates

* **Finding nearest neighbors using DenseLinkSearch**
```shell script
  java -Xmx4G -classpath hiD.jar hiD.search.TimeIndexSearch  ./Indexes/train_<FEATURE_DIMS>D_<DATASET_SIZE>_<K_INDEX>Nr.ndx ./DataSets/test_<FEATURE_DIMS>D_<DATASET_SIZE>.vecs <K_SEARCH> true
```
The TimeIndexSearch program requires four parameters: 
  1) the index file name - note the program will look for the appropriate datatset in the expected place
  2) the dataset of test queries file name
  3) the desired number of nearest neighbors to find
  4) an optional boolean parameter (defaults to false) that indicates whether the results should include duplicates

