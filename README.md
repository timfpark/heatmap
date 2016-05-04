## heatmap

Apache Spark application for building heatmaps with progressive levels of detail from a large GPS trace dataset.

### How to Use

1. Spin up Spark cluster via HDInsight or manually.
2. Set environmental variables to configure your environment:
* LOCATION_STORAGE_ACCOUNT: The storage account where the heatmap resultsets will be stored.
* LOCATION_STORAGE_KEY: Storage account key for this storage account.
* LOCATIONS_ROOT: Root of blob for your location data (eg. wasb://locations@mystorageaccount.blob.core.windows.net/ or './locations' to run it locally)
3. $ spark-submit heatmap.py
