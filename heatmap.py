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

from cassandra.cluster import Cluster

DETAIL_ZOOM_DELTA = 5
MAX_ZOOM_LEVEL = 16
KEY_SEPERATOR = "|"

KEY_FIELD = 0
VALUE_FIELD = 1

LOCATION_CASSANDRA_ENDPOINT="10.1.0.11"

def dataframe_loader(row):
    # "timestamp": calendar.timegm(row["timestamp"].utctimetuple()) * 1000,
    tileId = Tile.tile_id_from_lat_long(row["latitude"], row["longitude"], MAX_ZOOM_LEVEL + DETAIL_ZOOM_DELTA)
    if row["source"] == "background":
        return []
    else:
        return [{
            "tileId": tileId,
            "timestamp": row["timestamp"],
            "userId": row["user_id"],
            "count": 1.0
        }]

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
                if location['userId'][:3] == 'rt-':
                    userId = "route"
                else:
                    userId = location['userId']
                userGroups.append(userId)
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

def heatmap_to_json(heatmap):
    return json.dumps(heatmap)

def get_rows(sc):
    if LOCATION_CASSANDRA_ENDPOINT:
        sc.stop()
        conf = SparkConf(True).set("spark.cassandra.connection.host", LOCATION_CASSANDRA_ENDPOINT)
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        rows = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="locations", keyspace="rhom").load()
    else:
        sqlContext = SQLContext(sc)
        locationsConfig = {
            "Endpoint" : os.environ["LOCATIONS_COSMOSDB_HOST"],
            "Masterkey" : os.environ["LOCATIONS_COSMOSDB_AUTH_KEY"],
            "Database" : "locationsdb",
            "Collection" : "locations"
        }
        rows = sqlContext.read.format("com.microsoft.azure.cosmosdb.spark").options(**locationsConfig).load()
    return sqlContext, rows

def write_heatmap_dataframes(heatmapDataFrames):
    heatmapDataFrames.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='heatmaps',keyspace='rhom').save()

def batchMain(sc):
    sqlContext, rows = get_rows(sc)
    locations = rows.rdd.flatMap(dataframe_loader)
    heatmaps = build_heatmaps(locations)
    heatmapsMaterialized = heatmaps.mapValues(heatmap_to_json)
    heatmapDataFrames = sqlContext.createDataFrame(heatmapsMaterialized, ['id','heatmap'])
    return write_heatmap_dataframes(heatmapDataFrames)

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    batchMain(sc)