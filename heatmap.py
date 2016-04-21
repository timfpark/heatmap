from azure.storage.blob import BlobService
from datetime import date, datetime, timedelta
import json
import math
import os
import sys
from pyspark import SparkConf, SparkContext
from tile import Tile
import time

MAX_ZOOM_LEVEL = 16

KEY_FIELD = 0
VALUE_FIELD = 1

HEATMAP_CONTAINER = 'heatmaps'

def json_loader(line):
    try:
        location = json.loads(line)
        location['datetime'] = datetime.strptime(location["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        location['timestamp'] = time.mktime(location['datetime'].timetuple())
        return [ location ]
    except ValueError, e:
        return []

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

def tile_id_timespans_mapper(location):
    tileTimespanMappings = []
    tileId = Tile.tile_id_from_lat_long(location['latitude'], location['longitude'], MAX_ZOOM_LEVEL)
    tileIds = Tile.tile_ids_for_all_zoom_levels(tileId)
    for timespanType in ["alltime", "year", "month"]:
        timespanLabel = build_timespan_label(timespanType, location['datetime'])
        for tileId in tileIds:
            tile = Tile.tile_from_tile_id(tileId)
            userGroups = ['all']
            if not location['userId'][:1] == 'x':
                userGroups.append(location['userId'])
            for userId in userGroups:
                tileTimespanMappings.append((
                    build_tile_composite_key(userId, tileId, timespanLabel),
                    1
                ))
    return tileTimespanMappings

def key_by_result_set(entry):
    # Zoom ranges for resultsets: A tile at zoom N will be in zoom level N-RESULTSET_DELTA_LOW to N-RESULTSET_DELTA_HIGH resultsets.
    RESULTSET_DELTA_LOW = 5
    RESULTSET_DELTA_HIGH = 4
    keyFields = entry[KEY_FIELD].split('/')
    if len(keyFields) != 3:
        return []
    userGroup = keyFields[0]
    timespanLabel = keyFields[1]
    tileId = keyFields[2]
    resultSet = []
    tile = Tile.tile_from_tile_id(tileId)
    lowLevel = max(0, tile.zoom - RESULTSET_DELTA_LOW)
    highLevel = max(0, tile.zoom - RESULTSET_DELTA_HIGH)
    if tile.zoom >= MAX_ZOOM_LEVEL-1:
        highLevel = MAX_ZOOM_LEVEL-1
    if tile.zoom == MAX_ZOOM_LEVEL:
        highLevel = MAX_ZOOM_LEVEL
    for zoomLevel in range(lowLevel, highLevel):
        bucketTileId = Tile.tile_id_from_lat_long(tile.center_latitude, tile.center_longitude, zoomLevel)
        key = build_tile_composite_key(userGroup, bucketTileId, timespanLabel)
        print key
        resultSet.append(
            (key, entry)
        )
    return resultSet

def tuple_loader(line):
    return [eval(line)]

def combine_result_sets(resultsetValues):
    combined = {}
    for resultset in list(resultsetValues):
        for tuple in resultset:
            key = tuple[KEY_FIELD]
            if key in combined:
                combined[key] += tuple[VALUE_FIELD]
            else:
                combined[key] = tuple[VALUE_FIELD]
    combinedResultSet = []
    for key in combined:
        combinedResultSet.append(
           (key, combined[key])
        )
    return combinedResultSet

def write_result_set_into_blob_storage(iterator):
    for row in iterator:
        keyParts = row[KEY_FIELD].split('/')
        tileParts = keyParts[2].split('_')
        resultSetZoomLevel = int(tileParts[0])
        tileTimePeriodResultSet = {
            "heatmap": []
        }
        for item in row[VALUE_FIELD]:
            keyParts = item[KEY_FIELD].split('/')
            tileTimePeriodResultSet["heatmap"].append({
                "tc": item[VALUE_FIELD],
                "t": keyParts[2]
            })
        successful = False
        while not successful:
            try:
                blobService.put_blob(HEATMAP_CONTAINER, row[KEY_FIELD], json.dumps(tileTimePeriodResultSet), "BlockBlob", x_ms_blob_cache_control="max-age=3600", x_ms_blob_content_type="application/json")
                successful = True
            except:
                print "error putting heatmap: ", sys.exc_info()[0]
                continue
    yield None

blobService = BlobService(account_name=os.environ["LOCATION_STORAGE_ACCOUNT"], account_key=os.environ["LOCATION_STORAGE_KEY"])

blobService.create_container(HEATMAP_CONTAINER)
blobService.set_container_acl(HEATMAP_CONTAINER, x_ms_blob_public_access='container')

def check_config():
    if not "LOCATION_STORAGE_ACCOUNT" in os.environ:
        print "Required environment variable LOCATION_STORAGE_ACCOUNT missing."

    if not "LOCATION_STORAGE_KEY" in os.environ:
        print "Required environment variable LOCATION_STORAGE_KEY missing."

    if not "LOCATIONS_ROOT" in os.environ:
        print "Required environment variable LOCATIONS_ROOT missing."

def main(sc):
    check_config()

    lines = sc.textFile(os.environ["LOCATIONS_ROOT"])
    locationSlice = lines.flatMap(json_loader)
    tileTimespanAggregates = locationSlice.flatMap(tile_id_timespans_mapper).reduceByKey(lambda total1,total2:total1+total2)

    zoomLevelStats = tileTimespanAggregates.map(zoom_level_stats_mapper).reduceByKey(zoom_level_stats_reducer)
    zoomLevelStats.foreachPartition(write_stats_into_blob_storage)

    resultSets = tileTimespanAggregates.flatMap(key_by_result_set).groupByKey().mapValues(list)
    resultSets.foreachPartition(write_result_set_into_blob_storage)

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    main(sc)