# MongoDB Data Inflater (a.k.a. data-inflater)

A MongoDB utility to automate the creation of a new large database collection using data sourced from an existing smaller database collection.

By default, the utility will try to use the [Atlas](https://www.mongodb.com/atlas) 'sample data set' database collection 'sample_mflix.movies' as the source collection but this can be overridden to use any database and collection of your choice. For more information on configuring the Atlas sample data set, see the [manual page loading the sample data](https://docs.atlas.mongodb.com/sample-data/).

The utility issues multiple aggregation processes to perform copies of batches of records in parallel for increased performance. The resulting collection will contain documents with duplicated data, but with new unique _id's. The ratio of variance in the new collection will approximately reflect the ratio of variance in the source collection. Therefore, it is recommended to have at least a few different documents (if not a few hundred or thousand different documents) in the source collection you supply.

If deployed to a sharded cluster, the utility will ensure the target collection is sharded with a shard key, and where it can, it will pre-split the chunks, to avoid subsequent needless balancer overhead. For example if you specify the `--shardkey` for this utility to reference a field (e.g. `product_name`) to be the range based shard key, before creating the target collection, the utility will introspect the spread of values for the shard key field (e.g. `product_name`). The utility will then [pre-split chunks](https://docs.mongodb.com/manual/tutorial/create-chunks-in-sharded-cluster/) in the new empty target collection before any data is copied to it, to maximise performance. 


## How To Run

In a running MongoDB cluster (self-managed or running in Atlas), ensure you have created and populated a source collection with at least a few sample records in it (ideally more), with varying values for the fields across the different documents, to reflect the shape and variance you desire.

From a terminal, ensure the '.py' script is executable: 

```console
chmod u+x data-inflater.py
```

Execute the following to view the utility's _help_ instructions and the full list of parameters that you can provide:

```
./data-inflater.py -h
```

Execute the following to connect to a locally installed _single-server_ database (listening on the default port) to copy and expand the data from the existing source collection `mydb.mySrcColl` to an a new collection called `mydb.myDestColl`, which will contain 1 million records:

```console
./data-inflater.py --url 'mongodb://localhost:27017' -d 'mydb' -c 'mySrcColl' -t 'myDestColl' -s 1000000
```


Execute the following to connect to an Atlas cluster (which you've already loaded the [Atlas sample data set into](https://docs.atlas.mongodb.com/sample-data/)), to copy and expand the data from the source `movies` collection to an a new collection called `movies_big`, which will contain 100 million records (note, change the URL in the command first, to match the URL of your Atlas cluster):

```console
./data-inflater.py --url 'mongodb+srv://usr:pwd@mycluster.abcd.mongodb.net/'
```

