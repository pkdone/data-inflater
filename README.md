# MongoDB Data Inflater ("data-inflater")

The _data-inflater_ tool is a MongoDB utility to automate the creation of a new large database collection using data sourced from an existing smaller database collection.

By default, the utility will use the [Atlas](https://www.mongodb.com/atlas) 'sample data set' database collection `sample_mflix.movies` as the source collection. However, most users will provide parameters to the utility to specify the use of their own database and source collection. If you do want to use the Atlas sample data set, see the [sample data manual page](https://docs.atlas.mongodb.com/sample-data/) for more information.

The _data-inflater_ utility issues multiple concurrent aggregation processes, each copying batches of records in parallel for increased performance. The resulting collection will contain documents with duplicated data but with new unique `_id` field values. The variance ratio of data in the new collection will approximately reflect the variance ratio of the source collection. Therefore, you should ensure you have supplied at least a few different documents (if not a few hundred or thousand) in the source collection.

If you are running a sharded cluster, the utility will ensure the target collection is sharded with a shard key, and where it can, it will pre-split the chunks to avoid subsequent needless balancer overhead. For example, if you specify the `--shardkey` parameter for this utility to reference a field (e.g. `product_name`) as the range based shard key, before creating the target collection, the utility will introspect the spread of values for the shard key field (e.g. `product_name`). The utility will then create [pre-split chunks](https://docs.mongodb.com/manual/tutorial/create-chunks-in-sharded-cluster/) in the new empty target collection before any data is copied to it, to maximise performance.

## How To Run

In a running MongoDB cluster (self-managed or running in Atlas), ensure you have created and populated a source collection with at least one sample record in it (ideally more with varying values for the fields across the different documents to reflect the shape and variance you desire).

Ensure Python3 (version 3.8 or greater) and the MongoDB Python Driver ([PyMongo](https://docs.mongodb.com/drivers/pymongo/)) are already installed on your workstation. Example to install _PyMongo_:

```console
pip3 install --user pymongo
```

Ensure the `.py` script is executable and then execute the following to view the utility's _help instructions_ and the full list of parameters that you can provide:

```console
./data-inflater.py -h
```

Execute the following to connect to a locally running __single server__ database (default port) to copy and expand the data from an existing source collection, `mydb.mySrcColl`, to an a new collection, `mydb.myDestColl`, which will contain 1 million records:

```console
./data-inflater.py --url 'mongodb://localhost:27017' -d 'mydb' -c 'mySrcColl' -t 'myDestColl' -s 1000000
```

Execute the following to connect to an __Atlas cluster__ (ensure you've already loaded the [Atlas sample data set](https://docs.atlas.mongodb.com/sample-data/)), to inflate the data from the source `movies` collection to the new `movies_big` collection, which will contain 100 million records (note, first change the URL _username_, _password_ and _hostname_ shown, to match the URL of your Atlas cluster):

```console
./data-inflater.py --url 'mongodb+srv://usr:pwd@mycluster.abcd.mongodb.net/'
```

