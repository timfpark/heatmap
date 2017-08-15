from datetime import date, datetime, timedelta
import json
import math
import calendar
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
from tile import Tile
import time

import datetime

locationsConfig = {
    "Endpoint" : os.environ["LOCATIONS_COSMOSDB_HOST"],
    "Masterkey" : os.environ["LOCATIONS_COSMOSDB_AUTH_KEY"],
    "Database" : "locationsdb",
    "Collection" : "locations"
}

heatmapsConfig = {
    "Endpoint" : os.environ["LOCATIONS_COSMOSDB_HOST"],
    "Masterkey" : os.environ["LOCATIONS_COSMOSDB_AUTH_KEY"],
    "Database" : "locationsdb",
    "Collection" : "heatmaps"
}

#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils

MICROBATCH_FREQUENCY_SECONDS = 15
DETAIL_ZOOM_DELTA = 5
MAX_ZOOM_LEVEL = 16
KEY_SEPERATOR = "|"

KEY_FIELD = 0
VALUE_FIELD = 1

def dataframe_loader(row):
    # "timestamp": calendar.timegm(row["timestamp"].utctimetuple()) * 1000,
    tileId = Tile.tile_id_from_lat_long(row["latitude"], row["longitude"], MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA)
    return {
        "tileId": tileId,
        "timestamp": row["timestamp"],
        "userId": row["userId"],
        "count": 1.0
    }

def build_timespan_label(timespanType, localDate):
    month = str(localDate.month)
    if len(month) == 1:
        month = "0" + month
    day = str(localDate.day)
    if len(day) == 1:
        day = "0" + day
    if timespanType == 'alltime':
        return 'alltime'
    elif timespanType == 'year':
        return str(localDate.year)
    elif timespanType == 'month':
        return str(localDate.year) + "-" + month
    elif timespanType == 'day':
        return str(localDate.year) + "-" + month + "-" + day

def build_tile_composite_key(userId, tileId, timespanLabel):
    return userId + KEY_SEPERATOR + timespanLabel + KEY_SEPERATOR + tileId

def tile_id_timespans_mapper_for_zoom(zoom):
    def tile_id_timespans_mapper(location):
        tileTimespanMappings = []
        oldTile = Tile.tile_from_tile_id(location['tileId'])
        tileId = Tile.tile_id_from_lat_long(oldTile.center_latitude, oldTile.center_longitude, zoom)
        for timespanType in ["alltime"]: # , "year", "month"
            timespanLabel = "alltime" # build_timespan_label(timespanType, location['datetime'])
            userGroups = ['all']
            if not location['userId'][:1] == 'x':
                userGroups.append(location['userId'])
            for userId in userGroups:
                tileTimespanMappings.append((
                    build_tile_composite_key(userId, tileId, timespanLabel),
                    location['count']
                ))
            return tileTimespanMappings
    return tile_id_timespans_mapper

def map_to_resultset(bucket):
    bucketKeyParts = bucket[KEY_FIELD].split(KEY_SEPERATOR)
    userId = bucketKeyParts[0]
    timespanLabel = bucketKeyParts[1]
    tileId = bucketKeyParts[2]
    tile = Tile.tile_from_tile_id(tileId)
    result = {
        "tileId": tileId,
        "count": bucket[VALUE_FIELD]
    }
    resultSetTileId = Tile.tile_id_from_lat_long(tile.center_latitude, tile.center_longitude, tile.zoom - DETAIL_ZOOM_DELTA)
    return (build_tile_composite_key(userId, resultSetTileId, timespanLabel), result)

def heatmap_to_locations(bucket):
    locations = []
    bucketKeyParts = bucket[KEY_FIELD].split(KEY_SEPERATOR)
    userId = bucketKeyParts[0]
    timespan = bucketKeyParts[1]
    heatmap = bucket[VALUE_FIELD]
    for tileId in heatmap:
        locations.append({
            "userId": userId,
            "count": heatmap[tileId],
            "tileId": tileId,
            "timespan": timespan
        })
    return locations

# sed -i '/rootCategory=WARN/ c\log4j.rootCategory=WARN, console' /opt/spark/conf/log4j.properties
# bin/kafka-topics.sh --zookeeper 52.168.90.23:2181 --create --topic locations --partitions 3 --replication-factor 3
# kubectl exec -it spark-master-1604325470-wr1v7 -n spark -- bash
# mkdir develop && cd develop
# git clone http://github.com/timfpark/rhom-data data
# spark-submit --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 --py-files tile.py heatmap.py
# export CASSANDRA_ENDPOINT='cassandra-cassandra.cassandra.svc.cluster.local'
# export KAFKA_ENDPOINT='zookeeper.kafka.svc.cluster.local'
# export LOCATIONS_COSMOSDB_AUTH_KEY='L1SpNwr9BNVAAKgovnr50bZhc8t9HMvyD9sLbxNhJ9kz8Jn0kJIdAFLIdVHgHKe0Ce9dBOSwnF9juiDSAwVUaA==';
# export LOCATIONS_COSMOSDB_HOST='https://rhom-locations-db.documents.azure.com:443/'
# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,datastax:spark-cassandra-connector:2.0.1-s_2.11 --py-files tile.py heatmap.py
# spark-submit --jars azure-cosmosdb-spark-0.0.3-SNAPSHOT.jar,azure-documentdb-1.10.0.jar,json-20140107.jar --py-files tile.py heatmapCosmosDB.py

def build_heatmaps(locations):
    heatmaps = None
    for zoom in range(MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA, DETAIL_ZOOM_DELTA, -1):
        mapper = tile_id_timespans_mapper_for_zoom(zoom)
        countByTileIdTimespanKey = locations.flatMap(mapper).reduceByKey(lambda total1, total2: total1 + total2)
        zoomHeatmap = countByTileIdTimespanKey.map(map_to_resultset).groupByKey().mapValues(list_to_dict)
        if not heatmaps:
            heatmaps = zoomHeatmap
        else:
            heatmaps = heatmaps.union(zoomHeatmap)
        locations = zoomHeatmap.flatMap(heatmap_to_locations)
    return heatmaps

def list_to_dict(heatmapList):
    heatmap = {}
    for entry in heatmapList:
        tileId = entry['tileId']
        count = entry['count']
        heatmap[tileId] = count
    return heatmap

#def heatmap_to_json(heatmap):
#    return json.dumps(heatmap)

def batchMain(sc):
    sqlContext = SQLContext(sc)
    rows = sqlContext.read.format("com.microsoft.azure.cosmosdb.spark").options(**locationsConfig).load()
    locations = rows.rdd.map(dataframe_loader)
    heatmaps = build_heatmaps(locations)
    heatmapDataFrames = sqlContext.createDataFrame(heatmaps, ['id','heatmap'])
    heatmapDataFrames.write.format("com.microsoft.azure.cosmosdb.spark").mode('overwrite').options(**heatmapsConfig).save()

def kafka_json_loader(tuple):
    try:
        location = json.loads(tuple[VALUE_FIELD])
        if location and location['latitude'] and location['longitude']:
            location['tileId'] = Tile.tile_id_from_lat_long(location["latitude"], location["longitude"], MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA)
            location['count'] = 1.0
            return [ location ]
        else:
            print "no location or latitude or longitude"
            return []
    except ValueError, e:
        print "exception: " + str(tuple)
        return []

def merge_partial_heatmap(session, heatmapPartial):
    id = heatmapPartial[KEY_FIELD]
    heatmapPartial = heatmapPartial[VALUE_FIELD]
    mergedHeatmap = {}
    rows = session.execute("SELECT id, heatmap FROM rhom.heatmaps WHERE id='" + id + "'")
    for row in rows:
        mergedHeatmap = json.loads(row[1])
    for tileId in heatmapPartial:
        if tileId in mergedHeatmap:
            mergedHeatmap[tileId] = mergedHeatmap[tileId] + heatmapPartial[tileId]
        else:
            mergedHeatmap[tileId] = heatmapPartial[tileId]
    print "updating " + id
    session.execute("INSERT INTO rhom.heatmaps (id, heatmap) VALUES (%s, %s)", (id, json.dumps(mergedHeatmap)))

def merge_partial_heatmap_rdd(heatmapPartialRDD):
    cluster = Cluster([CASSANDRA_ENDPOINT])
    session = cluster.connect()
    for heatmapPartial in heatmapPartialRDD.collect():
        merge_partial_heatmap(session, heatmapPartial)

def merge_heatmap_partials(heatmapPartials):
    heatmapPartials.foreachRDD(merge_partial_heatmap_rdd)

def streamingMain(sc):
    print "USING STREAMING: " + os.environ["KAFKA_ENDPOINT"]

    sc.stop()
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, MICROBATCH_FREQUENCY_SECONDS)

    rows = KafkaUtils.createStream(ssc, os.environ["KAFKA_ENDPOINT"], 'heatmap-streaming', {"locations": 1})
    locations = rows.flatMap(kafka_json_loader)
    heatmap_partials = build_heatmaps(locations)
    merge_heatmap_partials(heatmap_partials)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    if "USE_STREAMING" in os.environ and os.environ["USE_STREAMING"] == "true":
        streamingMain(sc)
    else:
        batchMain(sc)