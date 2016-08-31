# Wukong

Wukong offers an ORM query engine for Solr and Solr Cloud.

Usage
--------------

###Create Solr Collection
Before you use wukong, make sure you create your collection with its schema on SolrCloud. For example,
```
	curl http://localhost:8080/solr/admin/collections?action=CREATE&name=user&numShards=1&replicationFactor=2
```

A sample schema will be like:
```
<fields>
	<uniqueKey>id</uniqueKey>
  	<field name="id" type="int" indexed="true" stored="true" required="true" />
	<field name="name" type="string" indexed="true" stored="true" required="true"/>
	<field name="city" type="string" indexed="true" stored="true"/>
	<field name="age" type="int" indexed="true" stored="true"/>
	...
</fields>
```

###Create a model class for Solr collection
Create a class for your Solr collection by extending the class `SolrDoc`. For example,

```
from wukong.models import SolrDoc

class User(SolrDoc):
    collection_name = "users"

    def validate_schema_fields(self, fields):
    	pass

    def get_data_for_solr(self):
    	pass

```
You can overide existing methods to fit your business logic, like `validate_schema_fields`, `get_data_for_solr`.


###Use Solr QueryManger

Creat a document
```
User.documents.create(User_id=12345, name="Test Name", city="Test City")
```

Update a document
```
User.documents.update(User_id=12345, name="Test Name")
```

To index a batch of documentsto your Solr collection, use the container class: SolrDocs. Instead of accessing SOLR
multiple times, it only issues one request to SOLR, which is more efficient.

```
	docs = [
		User(User_id=12345, name="Test Name1", city="Test Cit1"),
		User(User_id=123456, name="Test Name2", city="Test City2")
		...
	]
	docs = SolrDocs(docs)
	docs.index()
```

Fetch a document
```
User.documents.get(User_id__eq=12345)
```

Fetch multiple documents
```
User.documents.filter(name__eq="Test Name", city__wc="Test*").all()
```

Use compounded logic
```
User.documents.filter(OR(city__wc="Test*", name__eq="Test Name"))
```

Sort by a field
```
User.documents.sort_by("-name").all()
```

Force only return a certain fields
```
User.documents.only("is", "name").all()
```

Force only return the top 10 documents
```
User.documents.limit(10).all()
```

Chain the query methods
```
User.documents.filter(city__wc="Test*").sort_by("-name").limit(10).all()
```

Delete a document
```
User.documents.get(User_id__eq=12345).delete()
```

Batch delete documents
```
User.documents.filter(name__eq="Test Name").all().delete()
```