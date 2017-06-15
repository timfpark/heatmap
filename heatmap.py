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

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster

CASSANDRA_ENDPOINT = os.environ["CASSANDRA_ENDPOINT"]
DETAIL_ZOOM_DELTA = 5
MAX_ZOOM_LEVEL = 16
BASE_VALUE = 100

KEY_FIELD = 0
VALUE_FIELD = 1

def dataframe_loader(row):
    tileId = Tile.tile_id_from_lat_long(row["latitude"], row["longitude"], MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA)
    return {
        "tileId": tileId,
        "timestamp": calendar.timegm(row["timestamp"].utctimetuple()) * 1000,
        "userId": row["user_id"],
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
    return userId + "/" + timespanLabel + "/" + tileId

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
    bucketKeyParts = bucket[KEY_FIELD].split('/')
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
    bucketKeyParts = bucket[KEY_FIELD].split('/')
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

def apply_base_value(heatmap_value):
    return heatmap_value + BASE_VALUE

def build_heatmaps(locations):
    heatmaps = None
    for zoom in range(MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA, DETAIL_ZOOM_DELTA, -1):
        mapper = tile_id_timespans_mapper_for_zoom(zoom)
        countByTileIdTimespanKey = locations.flatMap(mapper).reduceByKey(lambda total1, total2: total1 + total2).mapValue(apply_base_value)
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

def heatmap_to_json(heatmap):
    return json.dumps(heatmap)

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

MICROBATCH_FREQUENCY_SECONDS = 10

def streamingMain(sc):
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

def batchMain(sc):
    sc.stop()
    conf = SparkConf(True).set("spark.cassandra.connection.host", CASSANDRA_ENDPOINT)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    rows = sqlContext.read.format("org.apache.spark.sql.cassandra")
                          .options(table="locations", keyspace="fortis")
                          .load()
    
    locations = rows.rdd.map(dataframe_loader)
    heatmaps = build_heatmaps(locations)
    heatmapsMaterialized = heatmaps.mapValues(heatmap_to_json)
    heatmapDataFrames = sqlContext.createDataFrame(heatmapsMaterialized, ['id','heatmap'])
    heatmapDataFrames.write.format("org.apache.spark.sql.cassandra")
                           .mode('append')
                           .options(table='heatmaps',keyspace='rhom')
                           .save()

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    if "USE_STREAMING" in os.environ and os.environ["USE_STREAMING"] == "true":
        streamingMain(sc)
    else:
        batchMain(sc)