======================
wukong documentation
======================

wukong offers an ORM query engine for Solr and Solr Cloud.


Get Started
===================

Define your document class
--------------------
.. code-block:: python

    from wukong.models import SolrDoc

    Class YourDocClass(SolrDoc):
        solr_hosts = "localhost:8080"
        collection_name = "my_solr_collection"

Fetch documents
--------------------
.. code-block:: python

    docs = YourDocClass.documents.filter(firstname__eq="james").all()

Update documents
--------------------
.. code-block:: python

    docs[0].firstname = "Jim"
    docs[0].index() # single update

    docs[1].firstname = "Smith"
    docs.index() # batch update

Delete documents
--------------------
.. code-block:: python

    docs[0].delete() # single delete
    docs.delete() # batch delete

Create documents
--------------------
.. code-block:: python

    YourDocClass.documents.create(id=1, firstname__eq="james", lastname__eq="bond")


Documentations
===================

Solr Document Class
--------------------
In order to connect to your Solr collection, we can just extend a base class called `SolrDoc`. You can specify four attributes in your class.

    * `solr_hosts`: the host name(s) for your Solr servers.
    * `zookeeper_hosts`: the host name(s) for your Zookeeper hosts which monitor Solr. (optional)
    * `collection_name`: the collection name for your collection in Solr.
    * `request_timeout`: in how many seconds to drop the request to Solr. (optional, defaults to 15)

For example, if you have a collection named `User`, you can do the following.

.. code-block:: python

    from wukong.models import SolrDoc

    Class User(SolrDoc):
        solr_hosts = "localhost:8080"
        zookeeper_hosts = "localhost:2181"
        collection_name = "users"

    def validate_schema_fields(self, fields):
        pass

    def get_data_for_solr(self):
        pass

You can overide existing methods to fit your business logic, like `validate_schema_fields`, `get_data_for_solr`.

    * `validate_schema_fields`: return boolean to validate if the current document is consistent with the Solr Schema
    * `get_data_for_solr`: return a json format to send to Solr for indexing

If you have multiple collections, you can define a base class to define `solr_hosts` and `zookeeper_hosts`, and the subclasses to only specify the `collection_name`.

.. code-block:: python

    from wukong.models import SolrDoc

    Class BaseDoc(SolrDoc):
        solr_hosts = "localhost:8080"
        zookeeper_hosts = "localhost:2181"

    Class User(BaseDoc)
        collection_name = "users"

    Class Car(BaseDoc)
        collection_name = "cars"



Documents Retrieval
--------------------
Once you define your document class, you can use it to fetch documents in Solr.

Filtering
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # fetch all documents whose name is james
    User.documents.filter(name__eq="james").all()

    # fetch all documents whose name is not james
    User.documents.filter(name__ne="james").all()

    # fetch all documents whose name has james as substring
    User.documents.filter(name__wc="james").all()

    # fetch all documents whose name doesn't have james as substring
    User.documents.filter(name__nwc="james").all()

    # fetch all documents whose age is greater to 30
    User.documents.filter(age__g=30).all()

    # fetch all documents whose age is less to 30
    User.documents.filter(age__l=30).all()

    # fetch all documents whose age is greater or equal to 30
    User.documents.filter(age__ge=30).all()

    # fetch all documents whose age is less or equal to 30
    User.documents.filter(age__le=30).all()

    # fetch all documents who lives in either in Ottawa or New York
    User.documents.filter(city__in=['Ottawa', 'New York']).all()

    # fetch all documents who lives in neither in Ottawa nor New York
    User.documents.filter(city__nin=['Ottawa', 'New York']).all()

    # fetch all documents whose has zip field
    User.documents.filter(zip__ex=True).all()

    # fetch all documents whose doesn't have zip field
    User.documents.filter(zip__nex=True).all()

    # fetch all documents whose age is less to 30 and live in Ottawa
    User.documents.filter(age__l=30, city__eq="Ottawa").all()

    # fetch all documents whose age is less to 30 or live in Ottawa
    User.documents.filter(OR(age__l=30, city__eq="Ottawa")).all()

    # fetch all documents whose age is less to 30 or live in Ottawa and also has zip field
    User.documents.filter(AND(OR(age__l=30, city__eq="Ottawa"), zip__ex=True)).all()

    # fetch all documents whose age is less to 30 and live in Ottawa
    User.documents.filter(age__l=30).filter(city__eq="Ottawa").all()


Sorting
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # fetch all documents sorted by age ascendingly
    User.documents.sort_by('age').all()

    # fetch all documents whose name is james sorted by age descendingly
    User.documents.filter(name__eq="james").sort_by('-age').all()


Search
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # fetch all documents matched `james bond` in the default field (usually `text`)
    User.documents.search('james bond').all()

    # fetch all documents matched `james bond` in name (weight 10) and city (weight 1)
    User.documents.search('james bond', name=10, city=1).all()

    # fetch all documents matched `james bond` in default field with at least 2 tokens matched
    User.documents.search('james bond', minimin_matches=2).all()


Grouping
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # group all documents by `gender` and fetch the groups
    User.documents.group_by('gender').groups()

    # group all documents by `gender` and `city` and fetch the groups
    User.documents.group_by(['gender', 'city']).groups()

    # group all documents by `gender` and `city` and get 3 documents in each group
    User.documents.group_by(['gender', 'city'], group_limit=3).groups()


Faceting
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # facet all documents by `gender` and fetch the facets
    User.documents.facet_by('gender').facets()

    # facet all documents by `gender` and `city` and fetch the facets
    User.documents.facet_by(['gender', 'city']).facets()

    # facet all documents by `gender` and `city` and fetch the facets at least having 10 docs
    User.documents.facet_by(['gender', 'city'], mincount=10).groups()


Pagination
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # paginate documents and get 100 documents starting from 200
    User.documents.offset(200).limit(100).all()


Return Fields
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # only fetch the fields (id and name) for each document
    User.documents.only('id, 'name').all()

Raw Documents
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # fetch all documents matched `james bond` and fetch a list of raw json rather than SolrDoc list
    User.documents.search('james bond').raw()


Chained Query
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # fetch the documents matching `james bond` and with age greater than 30, and get 100 documents starting from 200
    User.documents.search('james bond').filter(age__g=30).offset(200).limit(100).all()


Document Creation
--------------------

.. code-block:: python

    # Create a document in Solr
    User.documents.create(id=12345, name="James Bond", city="London")

    # Batch create within one request to Solr
    docs = [
        User(id=12345, name="James Bond", city="London"),
        Entity(id=12346, name="Kate", city="New York")
        ...
    ]
    docs = SolrDocs(docs)
    docs.index()


Document Update
--------------------

.. code-block:: python

    doc = User.documents.create(id=12345, name="James Bond", city="London")

    # Update a document in Solr
    doc.name = "Jim Bond"
    doc.city = "Ottawa"
    doc.index()


Document Delete
--------------------

.. code-block:: python

    doc = User.documents.create(id=12345, name="James Bond", city="London")

    # Update a document in Solr
    doc.delete()

    # Batch delete within one request to Solr
    docs = [
        User(id=12345, name="James Bond", city="London"),
        Entity(id=12346, name="Kate", city="New York")
        ...
    ]
    docs = SolrDocs(docs)
    docs.delete()


Complex Query
--------------------

.. code-block:: python

    # You can always use `User.solr.select` to build your custom query
    User.solr.select({
        q: "it is complex",
        ...,
        ...,
        ...
    })


Modules
==================

* :ref:`genindex`
* :ref:`modindex`
