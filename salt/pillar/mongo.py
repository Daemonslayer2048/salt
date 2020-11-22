# -*- coding: utf-8 -*-
'''
Read Pillar data from a mongodb collection

:depends: pymongo (for salt-master)

This module will load a node-specific pillar dictionary from a mongo
collection. It uses the node's id for lookups and can load either the whole
document, or just a specific field from that
document as the pillar dictionary.

Salt Master Mongo Configuration
===============================

The module shares the same base mongo connection variables as
:py:mod:`salt.returners.mongo_return`. These variables go in your master
config file.

   * ``mongo.db`` - The mongo database to connect to. Defaults to ``'salt'``.
   * ``mongo.host`` - The mongo host to connect to. Supports replica sets by
     specifying all hosts in the set, comma-delimited. Defaults to ``'salt'``.
   * ``mongo.port`` - The port that the mongo database is running on. Defaults
     to ``27017``.
   * ``mongo.user`` - The username for connecting to mongo. Only required if
     you are using mongo authentication. Defaults to ``''``.
   * ``mongo.password`` - The password for connecting to mongo. Only required
     if you are using mongo authentication. Defaults to ``''``.


Configuring the Mongo ext_pillar
================================

The Mongo ext_pillar takes advantage of the fact that the Salt Master
configuration file is yaml. It uses a sub-dictionary of values to adjust
specific features of the pillar. This is the explicit single-line dictionary
notation for yaml. One may be able to get the easier-to-read multi-line dict to
work correctly with some experimentation.

.. code-block:: yaml

  ext_pillar:
    - mongo: {collection: vm, id_field: name, re_pattern: \\.example\\.com, fields: [customer_id, software, apache_vhosts]}

In the example above, we've decided to use the ``vm`` collection in the
database to store the data. Minion ids are stored in the ``name`` field on
documents in that collection. And, since minion ids are FQDNs in most cases,
we'll need to trim the domain name in order to find the minion by hostname in
the collection. When we find a minion, return only the ``customer_id``,
``software``, and ``apache_vhosts`` fields, as that will contain the data we
want for a given node. They will be available directly inside the ``pillar``
dict in your SLS templates.

If one wishes to use the ``aggregate`` key, the ``fields`` key will be ignored.
Below is an example of an aggregate query that could be used.

 ext_pillar:
   - mongo: {collection: minions, id_field: _id, aggregate: [{"$match": {"_id": "Master-Minion"}},{"$lookup": {"from": "clients","localField":"Client","foreignField": "_id","as": "Client"}},{"$unset": ["Client._id",]},{"$project": {"name": 1,"Client": {"$arrayElemAt":["$Client",0]}}}]}

The above example pulls external grains from a mongodb. Note the operators such
as ``$match``, ``$project``, ``$lookup``, etc must all be doucle quoted. The above
example is functionally the same as running the below query for a minion by name.

db.minions.aggregate([
  {$match: {
    "_id": "Master-Minion"
  }},
  {$lookup: {
    from: "clients",
    localField:"Client",
    foreignField: "_id",
    as: "Client"
  }},
  {$unset: [
    "Client._id",
    "General_Config._id",
    "Role_Config._id"
  ]},
  {$project: {
      name: 1,
      Client: {$arrayElemAt:["$Client",0]}
    }
  }
])

As one can see a ``match`` statement will be applied to the beginning of the query
statement before running. The ``match`` statement can be tweaked to change the
field that will be mached against, in this case the monogodb ``_id`` is the
field containing the minion names.

Module Documentation
====================
'''
from __future__ import absolute_import, print_function, unicode_literals

# Import python libs
import logging
import re

# Import third party libs
from salt.ext import six

try:
    import pymongo

    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

__opts__ = {
    'mongo.db': 'salt',
    'mongo.host': 'salt',
    'mongo.password': '',
    'mongo.port': 27017,
    'mongo.user': '',
}

log = logging.getLogger(__name__)


def __virtual__():
    if not HAS_PYMONGO:
        return False
    return "mongo"


def get_connection(host, port):
    log.info("connecting to %s:%s for mongo ext_pillar", host, port)
    conn = pymongo.MongoClient(host, port)
    return conn


def authenticate_connection(mdb, user, password):
    log.debug("authenticating as '%s'", user)
    mdb.authenticate(user, password)
    return mdb


def get_find_one(mdb, collection, id_field, minion_id, fields):
    result = mdb[collection].find_one({id_field: minion_id}, projection=fields)
    if result:
        if fields:
            log.debug(
                "ext_pillar.mongo: found document, returning fields '%s'", fields
            )
        else:
            log.debug("ext_pillar.mongo: found document, returning whole doc")
        if "_id" in result:
            # Converting _id to a string
            # will avoid the most common serialization error cases, but DBRefs
            # and whatnot will still cause problems.
            result["_id"] = six.text_type(result["_id"])
        return result
    else:
        # If we can't find the minion the database it's not necessarily an
        # error.
        log.debug(
            "ext_pillar.mongo: no document found in collection %s", collection
        )
        return {}


def get_aggregate(mdb, collection, id_field, minion_id, aggregate):
    pipeline = []
    pipeline.append({"$match": {id_field: minion_id}})
    for pipe in aggregate:
        pipeline.append(pipe)
    results = mdb[collection].aggregate(pipeline)
    if results:
        # Drop the cursor and get the list
        results = list(results)
        if len(results) == 0:
            log.error("ext_pillar.mongo: pipeline returned no results")
            return {}
        elif len(results) > 1:
            # More than one item was returned in the list.
            log.error("ext_pillar.mongo: pipeline returned more than one result")
        else:
            # A single result was returned, so return inex 0 of the list.
            return results[0]
    else:
        log.debug("ext_pillar.mongo: no response from pipeline %s", collection)
        return {}


def ext_pillar(
    minion_id,
    pillar,  # pylint: disable=W0613
    collection='pillar',
    id_field='_id',
    re_pattern=None,
    re_replace='',
    fields=None,
    aggregate=None,
):
    '''
    Connect to a mongo database and read per-node pillar information.

    Parameters:
        * `collection`: The mongodb collection to read data from. Defaults to
          ``'pillar'``.
        * `id_field`: The field in the collection that represents an individual
          minion id. Defaults to ``'_id'``.
        * `re_pattern`: If your naming convention in the collection is shorter
          than the minion id, you can use this to trim the name.
          `re_pattern` will be used to match the name, and `re_replace` will
          be used to replace it. Backrefs are supported as they are in the
          Python standard library. If ``None``, no mangling of the name will
          be performed - the collection will be searched with the entire
          minion id. Defaults to ``None``.
        * `re_replace`: Use as the replacement value in node ids matched with
          `re_pattern`. Defaults to ''. Feel free to use backreferences here.
        * `fields`: The specific fields in the document to use for the pillar
          data. If ``None``, will use the entire document. If using the
          entire document, the ``_id`` field will be converted to string. Be
          careful with other fields in the document as they must be string
          serializable. Defaults to ``None``.
        * `aggregate`: The aggregate query to run, do note query operators
          must be quoted and the fields, argument will be ignored if also
          supplied.
    '''
    # Get connection to DB
    conn = get_connection(__opts__['mongo.host'], __opts__['mongo.port'])
    # Select database to use
    log.debug("using database '%s'", __opts__['mongo.db'])
    mdb = conn[__opts__['mongo.db']]
    # Authenticate if needed
    if __opts__['mongo.user'] and __opts__['mongo.password']:
        mdb = authenticate_connection(
            mdb, __opts__['mongo.user'], __opts__['mongo.password']
        )
    # Do the regex string replacement on the minion id
    if re_pattern:
        minion_id = re.sub(re_pattern, re_replace, minion_id)

    log.info(
        "ext_pillar.mongo: looking up pillar def for {'%s': '%s'} in mongo",
        id_field,
        minion_id,
    )
    if aggregate is None:
        result = get_find_one(mdb, collection, id_field, minion_id, fields)
        return result
    else:
        result = get_aggregate(mdb, collection, id_field, minion_id, aggregate)
        return result
