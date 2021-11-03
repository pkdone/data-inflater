#!/usr/bin/python3
##
# MongoDB utility to automate the creation of a new large database collection using data sourced
# from an existing smaller database collection.
#
# Ensure this '.py' script has executable permissions in your OS.
#
# For a full description of the tool and how to invoke it with various options, run:
#  $ ./data-inflater.py -h
#
# Example:
#  $ ./data-inflater --url 'mongodb+srv://usr:pwd@mycluster.abc.mongodb.net/' -s 1000000
#
# Prerequisites: Python 3.8+ and the PyMongo driver - example to install:
#  $ pip3 install --user pymongo
##
import sys
import os
import argparse
import math
import time
from datetime import datetime
from pprint import pprint
from pymongo import MongoClient
from bson.max_key import MaxKey
from multiprocessing import Process


##
# Main function to parse passed-in process before invoking the core processing function
##
def main():
    argparser = argparse.ArgumentParser(
        description="MongoDB utility to inflate the contents of a small collection into a new "
                    "larger collection. Due to using data sourced from an existing collection, "
                    "the resulting collection will contain duplicated documents, but with new "
                    "unique _id's. The ratio of variance in the new collection will approximately "
                    "reflect the ratio of variance in the source collection. It is recommended to "
                    "store at least a few different documents (if not a few hundred/thousand) in "
                    "the source collection you provide.")
    argparser.add_argument("-m", "--url", default=DEFAULT_MONGODB_URL,
                           help=f"MongoDB cluster URL (default: {DEFAULT_MONGODB_URL})")
    argparser.add_argument("-d", "--db", default=DEFAULT_DBNAME,
                           help=f"Database name (default: {DEFAULT_DBNAME})")
    argparser.add_argument("-c", "--coll", default=DEFAULT_SOURCE_COLLNAME,
                           help=f"Source collection name (default: {DEFAULT_SOURCE_COLLNAME})")
    argparser.add_argument("-t", "--target", default=DEFAULT_TARGET_COLLNAME,
                           help=f"Target collection name (default: {DEFAULT_TARGET_COLLNAME})")
    argparser.add_argument("-s", "--size", default=DEFAULT_SIZE, type=int,
                           help=f"Number of documents required (default: {DEFAULT_SIZE})")
    argparser.add_argument("-z", "--compression", default=DEFAULT_COMPRESSION,
                           choices=["snappy", "zstd", "zlib", "none"],
                           help=f"Collection compression to use (default: {DEFAULT_COMPRESSION})")
    argparser.add_argument("-k", "--shardkey", default="",
                           help=f"For sharded clusters, name of field to use as range shard key "
                                f"for the sharded collection or specify a string of comma "
                                f"separated field names for a compound shard key (default is to "
                                f"use hash sharding on '_id'")
    args = argparser.parse_args()

    if args.size < 1:
        sys.exit(f"ERROR: Argument 'args.size' must have a value greater than zero")

    shardKeyElements = []

    if args.shardkey:
        shardKeyElements = [field.strip() for field in args.shardkey.split(',')]

    run(args.url, args.db, args.coll, args.target, args.size, args.compression, shardKeyElements)


##
# Executes the data copying process using intermediate collections to step up the order of
# magnitude of size of data collection
##
def run(url, dbname, collname, target, size, compression, shardKeyFields):
    print(f"\nConnecting to MongoDB using URL '{url}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    start = datetime.now()
    connection = MongoClient(url)
    adminDB = connection["admin"]
    configDB = connection["config"]
    db = connection[dbname]
    originalAmountAvailable = db[collname].count_documents({})
    rangeShardKeySplits = []

    # Try to enable sharding and if can't we know its just a simple replica-set
    try:
        adminDB.command("enableSharding", dbname)
        isClusterSharded = True
    except Exception as e:
        isClusterSharded = False

    # Check there is some source data actually available
    if originalAmountAvailable < 1:
        sys.exit(f"ERROR: Source collection '{dbname}.{collname}' must contain at least one "
                 f"record but it is empty")

    # If sharded with range based shark key see if can get a list of pre-split points
    if isClusterSharded and shardKeyFields:
        rangeShardKeySplits = getRangeShardKeySplitPoints(db, collname, shardKeyFields)

    # Create final collection now in case it's sharded and pre-splitting - want enough time for
    # balancer to spread out the split chunks before collection comes under intense ingestion load
    createCollection(adminDB, db, target, compression, isClusterSharded, shardKeyFields,
                     rangeShardKeySplits, size, True)
    print()

    # See how many magnitudes difference there is. For example, source collection may thousands of"
    # documents but destination needs to be billions (6 order of magnitude difference)
    magnitudesOfDifference = (math.floor(math.log10(size)) -
                              math.ceil(math.log10(originalAmountAvailable)))
    sourceAmountAvailable = originalAmountAvailable
    srcCollName = collname
    tempCollectionsToRemove = []

    # Loop inflating by an order of magnitude each time (if there is that much difference)
    for magnitudeDifference in range(0, magnitudesOfDifference):
        tgtCollName = f"{collname}_{magnitudeDifference}"
        ceilingAmount = (10 ** (math.ceil(math.log10(originalAmountAvailable)) +
                         magnitudeDifference))
        createCollection(adminDB, db, tgtCollName, compression, isClusterSharded, shardKeyFields,
                         rangeShardKeySplits, ceilingAmount, False)
        expandToNewCollection(url, db, srcCollName, tgtCollName, sourceAmountAvailable,
                              ceilingAmount)
        sourceAmountAvailable = ceilingAmount
        tempCollectionsToRemove.append(srcCollName)
        srcCollName = tgtCollName

    # For target collection with range shard key pre-spliting, wait for chunks to be balanced
    if rangeShardKeySplits:
        waitForPresplitChunksToBeBalanced(configDB, dbname, target)

    # Do final inflation level and print summary
    expandToNewCollection(url, db, srcCollName, target, sourceAmountAvailable, size)
    tempCollectionsToRemove.append(srcCollName)
    end = datetime.now()
    print(f"Finished database processing work in {int((end-start).total_seconds())} seconds "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    print(f"\nNow going to gather & print some summary data + remove old temporary collections\n")
    printSummary(db, collname, target, compression)

    # Clean-up any temporary collections that are no longer needed
    if DO_PROPER_RUN:
        if collname in tempCollectionsToRemove:
            tempCollectionsToRemove.remove(collname)  # Ensure will not remove original collection

        removeTempCollections(db, tempCollectionsToRemove)

    print(f"Ended ({datetime.now().strftime(DATE_TIME_FORMAT)})\n")


##
# For a specific order of magnitude expansion, create a new larger source collection using data
# from the source collection
##
def expandToNewCollection(url, db, srcCollName, tgtCollName, srcSize, tgtSize):
    srcColl = db[srcCollName]
    print(f" COPY. Source size: {srcSize}, target size: {tgtSize}, "
          f"source coll: '{srcCollName}', target coll: '{tgtCollName}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})")
    aggBatchSizes = []
    iterations = math.floor(tgtSize / srcSize)
    remainder = tgtSize % srcSize

    for iteration in range(0, iterations):
        aggBatchSizes.append(srcSize)

    if remainder:
        aggBatchSizes.append(remainder)

    if DO_PROPER_RUN:
        print("  |-> ", end="", flush=True)
        spawanAggBatchProcesses(aggBatchSizes, executeCopyAggPipeline, url, db.name, srcCollName,
                                tgtCollName)

    print("\n")


##
# Execute each final aggregation pipeline in own OS process (hence must re-establish MongoClient
# connection for each process as pymongo connections can't be shared across processes)
##
def executeCopyAggPipeline(url, dbName, srcCollName, tgtCollName, limit):
    print("[", end="", flush=True)
    connection = MongoClient(url)
    db = connection[dbName]

    pipeline = [
        {"$unset": [
          "_id"
        ]},
        {"$limit": limit},
        {"$merge": {
          "into": tgtCollName,
          "whenMatched": "fail",
          "whenNotMatched": "insert"
        }}

    ]

    db[srcCollName].aggregate(pipeline)
    print("]", end="", flush=True)


##
# Analyse the original source collection for its natural split of ranges using an aggregation
# $bucketAuto operator.
##
def getRangeShardKeySplitPoints(db, originalCollname, shardKeyFields):
    if not shardKeyFields:
        return

    splitPoints = []

    # Want to split only on first field in shard key (not further fields if compound
    typePipeline = [
      {"$limit": 1},
      {"$project": {
        "_id": 0,
        "type": {"$type": "$" + shardKeyFields[0]},
      }},
    ]

    firstRecord = db[originalCollname].aggregate(typePipeline).next()
    type = firstRecord["type"]

    # Only makes sense to split on specific types (e.g. not boolean which can have only 2 values)
    if type in ["string", "date", "int", "double", "long", "timestamp", "decimal"]:
        splitPointsPipeline = [
          {"$bucketAuto": {
            "groupBy": f"${shardKeyFields[0]}", "buckets": TARGET_SPLIT_POINTS_AMOUNT
          }},

          {"$group": {
            "_id": "",
            "splitsCount": {"$sum": 1},
            "splitPoints": {
                "$push": "$_id.min",
            },
          }},

          {"$unset": [
            "_id",
          ]},
        ]

        firstRecord = db[originalCollname].aggregate(splitPointsPipeline).next()
        listOfPoints = firstRecord["splitPoints"]
        # print(f"splitsCount: {firstRecord['splitsCount']}")

        # Sometimes a list entry may be "None" so remove it
        for val in listOfPoints:
            if val is not None:
                splitPoints.append(val)

    return splitPoints


##
# Create a collection and make it sharded if running against a sharded cluster
##
def createCollection(adminDB, db, collName, compression, isClusterSharded, shardKeyFields,
                     rangeShardKeySplits, indtendedSize, isFinalCollection):
    dropCollection(db, collName)

    doShardCollection = True if (isClusterSharded and (isFinalCollection or
                                 (indtendedSize >= LARGE_COLLN_COUNT_THRESHOLD))) else False

    # Create the collection a specific compression algorithm
    db.create_collection(collName, storageEngine={"wiredTiger":
                         {"configString": f"block_compressor={compression}"}})

    # If collection is to be sharded need to configure shard key + pre-splitting
    if doShardCollection:  # SHARDED collection
        if shardKeyFields:  # RANGE shard key
            shardKeyFieldsText = ""

            for field in shardKeyFields:
                if shardKeyFieldsText:
                    shardKeyFieldsText += ","

                shardKeyFieldsText += field

            keyFieldOrders = {}

            for field in shardKeyFields:
                keyFieldOrders[field] = 1

            # Configure range based shard key which is pre-split
            adminDB.command("shardCollection", f"{db.name}.{collName}", key=keyFieldOrders)

            if rangeShardKeySplits:
                # Define each split point like: {'year': 2014, 'title': MaxKey, 'released': MaxKey}
                # (i.e. for compound shard key only provide split axis on first field in the key)
                for splitPoint in rangeShardKeySplits:
                    isFirst = True
                    middleSplitPoints = {}

                    for field in shardKeyFields:
                        if isFirst:
                            middleSplitPoints[field] = splitPoint
                        else:
                            middleSplitPoints[field] = MaxKey

                        isFirst = False

                    adminDB.command("split", f"{db.name}.{collName}", middle=middleSplitPoints)

                print(f" CREATE. Created collection '{collName}' (compression={compression}) - "
                      f"sharded with range shard key on '{shardKeyFieldsText}' (pre-split)")
            else:
                print(f" CREATE. Created collection '{collName}' (compression={compression}) - "
                      f"sharded with range shard key on '{shardKeyFieldsText}' (NOT pre-split)")
        else:  # HASH shard key
            # Configure hash based shard key which is pre-split
            adminDB.command("shardCollection", f"{db.name}.{collName}", key={"_id": "hashed"},
                            numInitialChunks=96)
            print(f" CREATE. Created collection '{collName}' (compression={compression}) - sharded "
                  f"with hash shard key on '_id' (pre-split)")
    else:  # UNSHARDED collection
        print(f" CREATE. Created collection '{collName}' (compression={compression}) - unsharded")


##
# If the target collection is sharded with a range shard key and has been pre-split, wait for the
# chunks to be balanced before subsequently doing inserts, to help maximise subsequent performance.
##
def waitForPresplitChunksToBeBalanced(configDB, dbName, collName):
    collectionIsImbalanced = True
    shownSleepNotice = False
    waitTimeSecs = 0
    lastChunkCountDifference = -1
    finalConvergenceAttemps = 0
    startTime = datetime.now()

    while collectionIsImbalanced and (waitTimeSecs < MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS):
        pipeline = [
          {"$match": {
            "ns": f"{dbName}.{collName}",
          }},

          {"$group": {
            "_id": "$shard",
            "chunksCount": {"$sum": 1},
          }},

          {"$set": {
            "shard": "$_id",
            "_id": "$$REMOVE",
          }},
        ]

        shardsMetadata = configDB["chunks"].aggregate(pipeline)
        chunkCounts = []

        for shardMetadata in shardsMetadata:
            # print(f"shard: {shardMetadata['shard']}, chunksCount: {shardMetadata['chunksCount']}")
            chunkCounts.append(shardMetadata["chunksCount"])

        chunkCounts.sort()
        lastChunkCountDifference = chunkCounts[-1] - chunkCounts[0]

        if lastChunkCountDifference <= BALANCED_CHUNKS_MAX_DIFFERENCE:
            # If still more than 2 difference, keep trying to get more convergence for a short time
            if (lastChunkCountDifference >= 2) and (finalConvergenceAttemps <= 2):
                finalConvergenceAttemps += 1
            else:
                collectionIsImbalanced = False
                break

        if not shownSleepNotice:
            print(f" WAITING. Waiting for the range key pre-split chunks to balance in the sharded"
                  f" collection '{collName}' - this may take a few minutes")

        time.sleep(BALANCE_CHECK_SLEEP_SECS)
        shownSleepNotice = True
        waitTimeSecs = (datetime.now() - startTime).total_seconds()

    if collectionIsImbalanced:
        print(f" WARNING: Exceeded maximum threshold ({MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS} "
              f"secs) waiting for sharded collection to evenly balance (current difference: "
              f"{lastChunkCountDifference}) - subsequent cluster performance may be degraded "
              f"for a while")
    else:
        print(f" BALANCED. Sharded collection with range key pre-split chunks is now "
              f"balanced (max chunk count difference across all shards is: "
              f"{lastChunkCountDifference})")


##
# Drop all temporary collections used
##
def removeTempCollections(db, collectionNames):
    for coll in collectionNames:
        dropCollection(db, coll)


##
# Drop collection
##
def dropCollection(db, coll):
    print(f" DROP. Removing existing collection: '{coll}'")
    db.drop_collection(coll)


##
# Print summary of source and target collection statistics
##
def printSummary(db, srcCollName, tgtCollName, compression):
    print("Original collection statistics:")
    printCollectionData(db, srcCollName)
    print("\nFinal collection statistics:")
    printCollectionData(db, tgtCollName)
    print(f" Compression used: {compression}\n")


##
# Print summary stats for a collection
##
def printCollectionData(db, collName):
    collstats = db.command("collstats", collName)
    print(f" Colleton name: {collName}")
    print(f" Sharded collection: {'sharded' in collstats}")
    print(f" Average object size: {int(collstats['avgObjSize'])}")
    print(f" Docs amount: {db[collName].count_documents({})}")
    print(f" Docs size (uncompressed): {collstats['size']}")
    print(f" Index size (with prefix compression): {collstats['totalIndexSize']}")
    print(f" Data size (index + uncompressed docs): "
          f"{collstats['size'] + collstats['totalIndexSize']}")
    print(f" Stored data size (docs+indexes compressed): {collstats['totalSize']}")


##
# Spawns multiple process, each running an aggregation in parallel against a batch records from a
# source collection.
#
# The 'funcToParallelise' argument should have the following signature:
#     myfunc(*args, limit)
# E.g.:
#     myfunc(url, dbName, srcCollName, tgtCollName, limit)
##
def spawanAggBatchProcesses(aggBatchSizes, funcToParallelise, *args):
    processesList = []

    # Create a set of OS processes to perform each of the aggregations in the batch in parallel
    for batchSize in aggBatchSizes:
        process = Process(target=wrapperProcessWithKeyboardException, args=(funcToParallelise,
                          *args, batchSize))
        processesList.append(process)

    try:
        # Start all processes
        for process in processesList:
            process.start()

        # Wait for all processes to finish
        for process in processesList:
            process.join()
    except KeyboardInterrupt:
        print(f"\nKeyboard interrupted received\n")
        shutdown()


##
# For a newly spawned process, wraps a business function with the catch of a keyboard interrupt to
# then immediately ends the process when the exception occurs without spitting out verbiage.
##
def wrapperProcessWithKeyboardException(*args):
    try:
        args[0](*(args[1:]))
    except KeyboardInterrupt:
        os._exit(0)


##
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script.
##
def shutdown():
    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(0)


# Constants
DO_PROPER_RUN = True
LARGE_COLLN_COUNT_THRESHOLD = 100_000_000
TARGET_SPLIT_POINTS_AMOUNT = 512
BALANCED_CHUNKS_MAX_DIFFERENCE = 8
MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS = 600
BALANCE_CHECK_SLEEP_SECS = 5
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_MONGODB_URL = "mongodb://localhost:27017"
DEFAULT_DBNAME = "sample_mflix"
DEFAULT_SOURCE_COLLNAME = "movies"
DEFAULT_TARGET_COLLNAME = "movies_big"
DEFAULT_COMPRESSION = "snappy"
DEFAULT_SIZE = 100_000_000


##
# Main
##
if __name__ == "__main__":
    main()
