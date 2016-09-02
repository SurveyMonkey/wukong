from wukong.api import SolrAPI
from wukong.query import SolrQueryManager
import wukong.errors as solr_errors
from six import with_metaclass
import re


class SolrDocMetaClass(type):
    """
    Meta class for SolrDoc
    """

    @property
    def solr(self):
        """
        Return a instance of SOLR api class.
        """
        if not hasattr(self, '_solr'):
            self._solr = SolrAPI(
                solr_hosts=self.solr_hosts,
                solr_collection=self.collection_name,
                zookeeper_hosts=self.zookeeper_hosts,
                timeout=self.request_timeout
            )
        return self._solr

    @property
    def documents(self):
        """
        Return a instance of SOLR Query Manager class.
        """
        return SolrQueryManager(doc_class=self)

    @property
    def unique_key(self):
        """
        Return the unique key of the collection schema
        """
        return self.schema.get("uniqueKey")

    @property
    def schema(self):
        """
        Return the current collection schema
        """
        if not hasattr(self, '_schema'):
            self._schema = self.solr.get_schema()
        return self._schema


class SolrDocs(object):
    """
    A Wrapper Container for a collection of SolrDoc
    instances, designed for batch update and delete
    """

    def __init__(self, docs=None):
        self.docs = []

        self.doc_class = None
        for doc in docs or []:
            self.add(doc)

    def __str__(self):
        return str(self.docs)

    def __repr__(self):
        return '%s(len(%s)=%s)' % (
            self.__class__.__name__,
            self.doc_class.__name__ if self.doc_class else "doc",
            len(self.docs))

    def __setitem__(self, key, value):
        self.docs[key] = value

    def __getitem__(self, key):
        return self.docs[key]

    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        for doc in self.docs:
            yield doc

    def add(self, doc):
        """
        Add a document to the SolrDoc container

        :param doc: the document to add
        :type doc: SolrDoc
        """
        if self.doc_class:
            if doc.__class__ != self.doc_class:
                raise solr_errors.SolrError(
                    "The types of documents in a " +
                    "container should be the same"
                )
        else:
            self.doc_class = doc.__class__

        self.docs.append(doc)

    def index(self, commit=False):
        """
        Index all current documents in the container to SOLR collection

        :param commit: whether or not to commit to SOLR when submitted
        :type commit: boolean
        """
        if len(self.docs) == 0:
            return

        data = []

        for doc in self.docs:
            data.append(doc.get_data_for_solr())

        self.doc_class.solr.update(data)

        if commit:
            self.doc_class.solr.commit()

    def delete(self, commit=False):
        """
        Index all current documents in the container to SOLR collection

        :param commit: whether or not to commit to SOLR when submitted
        :type commit: boolean
        """
        if len(self.docs) == 0:
            return

        unique_keys_str = "(%s)" % ' '.join(
            [str(doc.get_unique_field()) for doc in self.docs]
        )
        self.doc_class.solr.delete(
            self.doc_class.unique_key,
            unique_keys_str,
            commit=commit)


class SolrDoc(with_metaclass(SolrDocMetaClass, object)):
    """
    The base class for modeling any type of solr document
    """
    solr_hosts = None
    zookeeper_hosts = None
    collection_name = None
    request_timeout = 15

    @property
    def solr(self):
        """
        Reference the property from the metaclass and
        return a instance of SOLR api class.
        """
        return self.__class__.solr

    @property
    def documents(self):
        """
        Reference the property from the metaclass and
        return a instance of SOLR Query Manager class.
        """
        return self.__class__.documents

    @property
    def unique_key(self):
        """
        Reference the property from the metaclass and
        return the unique key of the collection schema
        """
        return self.__class__.unique_key

    @property
    def schema(self):
        """
        Reference the property from the metaclass and
        return the current collection schema
        """
        return self.__class__.schema

    def __init__(self, partial_update=False, field_weights=None, **kwargs):
        """
        :param partial_update: whether or not only the provided
            fields should be updated, otherwise, the entire
            document will be overridden.
        :type partial_update: boolean

        :param field_weights: the weights for different fields for boosting
        :type field_weights: dict

        :param kwargs: the dictionary of key value mapping
            for fields of the document
        :param kwargs: dict
        """

        self.fields = {}

        self.partial_update = partial_update

        self.field_weights = field_weights

        self.set_fields(**kwargs)

        self.validate_schema_fields(self.fields)

    @classmethod
    def add_schema_fields(cls, fields):
        """
        Add fields to the SOLR schema which will hit the SOLR schema api.

        :param fields: a list of field meta info (e.g. name, type)
        :type fields: list
        """
        schema_fields = [field["name"] for field in cls.schema['fields']]

        added_fields = []
        for field in fields:
            if field["name"] not in schema_fields:
                added_fields.append(field)

        return cls.solr.add_schema_fields(added_fields)

    @classmethod
    def from_json_docs(cls, json_docs):
        """
        Convert a list of json dict from SOLR to a list of SolrDoc.

        :param json_docs: a list of dict returned from SOLR server
        :type json_docs: list

        :return: a list of SolrDoc
        :rtype: list
        """
        docs = []
        for doc in json_docs:
            docs.append(cls(**doc))

        return docs

    def __str__(self):
        return str(self.fields)

    def __repr__(self):
        return '%s(%s=%s)' % (
            self.__class__.__name__, self.unique_key,
            self.get_unique_field()
        )

    def __eq__(self, other):
        """
        Compare two SOLR documents

        :param other: the other doc to compare
        :type other: SolrDoc

        :return: whether or not the fields of the two docs are the same
        :rtype: boolean
        """

        return self.fields == other.fields

    def __ne__(self, other):
        """
        Compare two SOLR documents

        :param other: the other doc to compare
        :type other: SolrDoc

        :return: whether or not the fields of the two docs are different
        :rtype: boolean
        """

        return self.fields != other.fields

    def __setitem__(self, key, value):
        self.set_field(key, value)

    def __getitem__(self, key):
        return self.get_field(key)

    def __getattr__(self, key):
        item = self.get_field(key)
        return item

    def __len__(self):
        return len(self.fields)

    def __iter__(self):
        for field in self.fields:
            yield field, self.get_field(field)

    def get_unique_field(self):
        """
        Get the value of the unique key in the SolrDoc
        """
        return self.fields.get(self.unique_key, None)

    def set_fields(self, **fields):
        """
        Set the values for all fields in the SolrDoc
        """
        for field in fields:
            # if the data is from SOLR, we don't want '_version_'
            if fields[field] is not None and field != '_version_':
                self.set_field(field, fields[field])

    def get_field(self, key):
        """
        Get the value of an field from the SolrDoc
        """
        return self.fields.get(key, None)

    def set_field(self, key, value):
        """
        Set the value of an field from the SolrDoc
        """
        self.fields[key] = value

    def del_field(self, key):
        """
        Delete the value of an field from the SolrDoc
        """
        if key in self.fields:
            if key == self.unique_key:
                raise solr_errors.SolrDeleteUniqueKeyError(key)
            del self.fields[key]

    def is_partial_update(self):
        """
        Return if the SolrDoc is only for partial update
        """
        return self.partial_update

    def set_partial_update(self, value):
        """
        Set the value of partial update
        """
        self.partial_update = bool(value)

    def set_field_weight(self, field, weight):
        """
        Set the weight of an field from the SolrDoc
        """
        if self.field_weights is None:
            self.field_weights = {}

        self.field_weights[field] = weight

    def get_field_weight(self, field):
        """
        Get the weight of an field from a SolrDoc
        """
        if self.field_weights is None:
            return None

        return self.field_weights.get(field)

    @classmethod
    def validate_schema_fields(cls, fields):
        """
        Validate if the fields are valid for SOLR by checking with the schema

        :param fields: a list of dicts for the field meta info
        :type fields: list

        :return: whether or not the fields are consistent with SOLR schema
        :rtype: boolean
        """
        schema_fields = dict([(field["name"], field) for field
                              in cls.schema.get("fields", {})])

        dynamic_fields = dict([(field["name"], field) for field
                               in cls.schema.get("dynamicFields", {})])

        pk = cls.unique_key

        if pk not in fields or fields[pk] is None:
            raise solr_errors.SolrSchemaValidationError(
                pk,
                message="Unique key is not specified"
            )

        for key, field in fields.items():
            if key in schema_fields:
                if schema_fields[key].get("multiValued") and \
                        not isinstance(field, list):
                    raise solr_errors.SolrSchemaValidationError(
                        key,
                        message="%s is not list type" % key
                    )
            else:
                found = False

                for df in dynamic_fields:
                    df_re = "^%s$" % df.replace("*", ".*")
                    if re.match(df_re, key):
                        found = True

                if not found:
                    raise solr_errors.SolrSchemaValidationError(key)

    def get_data_for_solr(self):
        """
        Generate the data for SOLR indexing for the current SolrDoc.

        :return: a json representation of a SolrDoc for SOLR indexing.
            If the SolrDoc is invalid, it returns None.
        :rtype: dict
        """
        self.validate_schema_fields(self.fields)

        solr_data = {}

        for key in self.fields:
            value = self.get_field(key)
            if self.is_partial_update() and key != self.unique_key:
                # {"set": self.get_attr(key)} is for partial update in SOLR
                # if "set" is not used, the entire entity in SOLR will be
                # replaced
                value = {"set": value}

            # boost is to adjust the weight of a specific field for
            # that SolrDoc
            if self.get_field_weight(key):
                value = {
                    "value": value,
                    "boost": self.get_field_weight(key)
                }

            solr_data[key] = value

        return solr_data

    def index(self, commit=False):
        """
        Index the current SolrDoc from SOLR.

        :param commit: whether or not to commit upon submission.
        :type commit: boolean
        """
        solr_doc = self.get_data_for_solr()

        self.solr.update([solr_doc])
        if commit:
            self.solr.commit()

    def delete(self, commit=False):
        """
        Delete the current SolrDoc from SOLR.

        :param commit: whether or not to commit upon submission.
        :type commit: boolean
        """
        unique_key = self.unique_key
        unique_key_value = self.get_unique_field()
        self.solr.delete(unique_key, unique_key_value, commit=commit)
