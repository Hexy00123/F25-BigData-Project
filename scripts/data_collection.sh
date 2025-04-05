url="https://www.kaggle.com/api/v1/datasets/download/elemento/nyc-yellow-taxi-trip-data"
data_dir=$(pwd)/data

echo "INFO: Downloading the data from source..."
curl -L -o "$data_dir/nyc-yellow-taxi-trip-data.zip" "$url"

echo "INFO: Unzipping..."
unzip -p $data_dir/nyc-yellow-taxi-trip-data.zip yellow_tripdata_2016-03.csv > $data_dir/dataset.csv

echo "INFO: Clear files..."
rm $data_dir/nyc-yellow-taxi-trip-data.zip