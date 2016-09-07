import wukong.errors as solr_errors
import copy
import re
import six

INFINITE_ROWS = 999999999


class SolrNode(object):
    """
    The base class to model a tree node in the query logic to SOLR
    """
    def __init__(self, *args, **kwargs):
        self.items = self.build_items(args, kwargs)

    @property
    def parsed_solr_query(self):
        """
        Parse the current node to a query string
        """
        pass

    @classmethod
    def build_items(cls, args, kwargs):
        """
        Build a list of items under the current logic operator
        """
        items = []
        for arg in args:
            items.append(arg)

        for key, value in kwargs.items():
            try:
                name, operator = key.split("__")
            except ValueError:
                raise solr_errors.SolrUnspecifiedOperatorError(key)

            items.append(Comparator(operator, name, value))
        return items


class AND(SolrNode):
    """
    Model the AND logic in SOLR query
    """
    logic = "AND"

    @property
    def parsed_solr_query(self):
        if len(self.items) == 0:
            return ""
        if len(self.items) == 1:
            return self.items[0].parsed_solr_query
        return "(%s)" % (" %s " % self.logic).join([
            node.parsed_solr_query for node in self.items
        ])


class OR(SolrNode):
    """
    Model the OR logic in SOLR query
    """
    logic = "OR"

    @property
    def parsed_solr_query(self):
        if len(self.items) == 0:
            return ""
        if len(self.items) == 1:
            return self.items[0].parsed_solr_query
        return "(%s)" % (" %s " % self.logic).join([
            node.parsed_solr_query for node in self.items
        ])


class NOT(SolrNode):
    """
    Model the NOT logic in SOLR query
    """
    logic = "NOT"

    def __init__(self, *args, **kwargs):
        super(NOT, self).__init__(*args, **kwargs)

        if len(self.items) > 1:
            raise solr_errors.SolrError(
                "Logic node:NOT only supports one operand"
            )

    @property
    def parsed_solr_query(self):
        if len(self.items) == 0:
            return ""
        return "!(%s)" % self.items[0].parsed_solr_query


class Comparator(SolrNode):
    """
    Model the compare logic in SOLR query
    """
    logic = "COMP"

    def __init__(self, operator, key, value):
        self.operator = operator
        self.key = key
        self.value = value

    @property
    def parsed_solr_query(self):
        query_string = ""

        if self.key is None:
            return query_string

        operator = self.operator
        key = self.key
        value = self.value

        # EQUAL
        if operator == 'eq':
            if value is None:
                query_string = "-%s:[* TO *]" % key
            elif isinstance(value, six.string_types):
                query_string = "%s:\"%s\"" % (key, value)
            else:
                query_string = "%s:%s" % (key, value)

        # NOT EQUAL
        elif operator == 'ne':
            if value is None:
                query_string = "%s:*" % key
            elif isinstance(value, six.string_types):
                query_string += "-%s:\"%s\"" % (key, value)
            else:
                query_string += "-%s:%s" % (key, value)

        # IN
        elif operator == 'in':
            if not isinstance(value, list):
                raise solr_errors.SolrError(
                    "Operator:in only takes list type"
                )
            new_value = []
            for i in value:
                if i is not None:
                    if isinstance(i, six.string_types):
                        new_value.append("\"%s\"" % i)
                    else:
                        new_value.append(str(i))
            list_str = "(%s)" % " ".join(new_value)

            query_string = "%s:%s" % (key, list_str)

        # NOT IN
        elif operator == 'nin':
            if not isinstance(value, list):
                raise solr_errors.SolrError(
                    "Operator:not in only takes list type"
                )
            new_value = []
            for i in value:
                if i is not None:
                    if isinstance(i, six.string_types):
                        new_value.append("\"%s\"" % i)
                    else:
                        new_value.append(str(i))
            list_str = "(%s)" % " ".join(new_value)

            query_string = "-%s:%s" % (key, list_str)

        # WILD CARD
        elif operator == 'wc':
            value = value or ""
            if '*' in value:
                query_string = "%s:%s" % (key, value)
            else:
                query_string = "%s:*%s*" % (key, value)
            query_string = query_string.replace("**", "*")

        # NOT WILD CARD
        elif operator == 'nwc':
            value = value or ""
            if '*' in value:
                query_string = "-%s:%s" % (key, value)
            else:
                query_string = "-%s:*%s*" % (key, value)
            query_string = query_string.replace("**", "*")

        # GREATER THAN
        elif operator == 'g':
            query_string = "%s:{%s TO *]" % (key, value or "*")

        # GREATER THAN OR EQUAL TO
        elif operator == 'ge':
            query_string = "%s:[%s TO *]" % (key, value or "*")

        # LESS THAN
        elif operator == 'l':
            query_string = "%s:[* TO %s}" % (key, value or "*")

        # LESS THAN OR EQUAL TO
        elif operator == 'le':
            query_string = "%s:[* TO %s]" % (key, value or "*")

        # EXISTS
        elif operator == 'ex':
            query_string = "%s:[* TO *]" % key

        # NOT EXISTS
        elif operator == 'nex':
            query_string = "-%s:[* TO *]" % key

        # UNKNOWN
        else:
            raise solr_errors.SolrUnspportedOperatorError(operator)

        return query_string


class SolrQueryManager(object):
    """
    A class to chain different query methods for SOLR
    and construct the final query to SOLR
    """
    def __init__(
        self,
        doc_class,
        node=None,
        sort_str=None,
        weight_dict=None,
        returned_fields=None,
        edismax=False,
        rows=INFINITE_ROWS,
        start=0,
        facet_fields=None,
        facet_options={},
        mincount=1,
        facet_group=True,
        group_fields=None,
        group_limit=0,
        group_options={},
        boost_func=None,
        bf_weight=1,
        boost_query=None,
        bq_weight=1,
        minimum_matches=None,
        text_keywords=None,
        stats_fields=None,
    ):

        self.node = node if node else AND()
        self.sort_str = sort_str
        self.weight_dict = weight_dict
        self.returned_fields = returned_fields
        self.edismax = edismax
        self.start = start
        self.rows = rows
        self.doc_class = doc_class
        self.facet_fields = facet_fields
        self.facet_options = facet_options
        self.mincount = mincount
        self.facet_group = facet_group
        self.group_fields = group_fields
        self.group_limit = group_limit
        self.group_options = group_options
        self.boost_func = boost_func
        self.bf_weight = bf_weight
        self.boost_query = boost_query
        self.bq_weight = bq_weight
        self.minimum_matches = minimum_matches
        self.text_keywords = text_keywords
        self.stats_fields = stats_fields

    def to_dict(self):
        """
        Get the json representation of the query manager
        """
        return {
            "node": self.node,
            "doc_class": self.doc_class,
            "sort_str": self.sort_str,
            "weight_dict": self.weight_dict,
            "returned_fields": self.returned_fields,
            "edismax": self.edismax,
            "start": self.start,
            "rows": self.rows,
            "facet_fields": self.facet_fields,
            "facet_options": self.facet_options,
            "mincount": self.mincount,
            "facet_group": self.facet_group,
            "group_fields": self.group_fields,
            "group_limit": self.group_limit,
            "group_options": self.group_options,
            "boost_func": self.boost_func,
            "bf_weight": self.bf_weight,
            "boost_query": self.boost_query,
            "bq_weight": self.bq_weight,
            "minimum_matches": self.minimum_matches,
            "text_keywords": self.text_keywords,
            "stats_fields": self.stats_fields,
        }

    def filter(self, *args, **kwargs):
        """
        Filter the SOLR documents by mathmatical and logical operators.
        Usage:
            filter(name__eq="Test Name")
            filter(name__eq="Test Name", city__wc="Test*")
            filter(OR(name__eq="Test Name", city__wc="Test*"),
                    population__ge=300000)
        """
        node = copy.deepcopy(self.node)
        items = node.items if hasattr(node, 'items') else []

        if node.logic != "AND":
            node = AND(node)
            items = node.items

        items += SolrNode.build_items(args, kwargs)
        if len(items) == 1:
            node = copy.deepcopy(items[0])

        params = self.to_dict()
        params.update({
            "node": node
        })
        return SolrQueryManager(**params)

    def sort_by(self, sort_str):
        """
        Sort the SOLR documents
        Usage:
            ascending: sort_by("name")
            descending: sort_by("-date")
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "sort_str": sort_str
        })
        return SolrQueryManager(**params)

    def group_by(self, group_fields, group_limit=0, **kwargs):
        """
        Group the SOLR documents
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "group_fields": group_fields,
            "group_limit": group_limit,
            "group_options": kwargs
        })
        return SolrQueryManager(**params)

    def facet(self, facet_fields, mincount=1, group=True, **kwargs):
        """
        Facet the SOLR documents
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "facet_fields": facet_fields,
            "mincount": mincount,
            "facet_group": group,
            "facet_options": kwargs
        })
        return SolrQueryManager(**params)

    def stats(self, stats_fields):
        node = copy.deepcopy(self.node)
        params = self.to_dict()

        params.update({
            'node': node,
            'stats_fields': stats_fields,
        })
        return SolrQueryManager(**params)

    def boost_by_func(self, boost_func, bf_weight=1):
        """
        Boost query by function
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "boost_func": boost_func,
            "bf_weight": bf_weight
        })
        return SolrQueryManager(**params)

    def boost_by_query(self, boost_query, bq_weight=1):
        """
        Boost query by query
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "boost_query": boost_query,
            "bq_weight": bq_weight
        })
        return SolrQueryManager(**params)

    def search(self, text, minimum_matches=None, **weights):
        """
        Search SOLR by text query.
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "edismax": True,
            "text_keywords": text,
            "minimum_matches": minimum_matches,
            "weight_dict": weights
        })
        return SolrQueryManager(**params)

    def only(self, *args):
        """
        Specify the returned fields in each document.
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "returned_fields": args
        })
        return SolrQueryManager(**params)

    def limit(self, rows):
        """
        Specify the number of documents returned.
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "rows": rows
        })
        return SolrQueryManager(**params)

    def offset(self, start):
        """
        Specify the offset of the entire documents
        """
        node = copy.deepcopy(self.node)
        params = self.to_dict()
        params.update({
            "node": node,
            "start": start
        })
        return SolrQueryManager(**params)

    @property
    def query(self):
        """
        Construct the query string to SOLR
        """
        solr_query = self.node.parsed_solr_query

        if solr_query == "":
            solr_query = "*:*"

        params = {
            "rows": self.rows,
            "start": self.start,
            "wt": "json"
        }

        if self.edismax:
            params['defType'] = 'edismax'
            params["fq"] = solr_query
            text = self.text_keywords
            text = text.strip()
            query_string = " ".join([k for k in re.compile("\s+").split(text)])
            params["q"] = query_string
        else:
            params["q"] = solr_query

        if self.weight_dict:
            params["qf"] = " ".join(
                ["%s^%s" % (k, self.weight_dict[k]) for k in self.weight_dict]
            )

        if self.minimum_matches:
            params["mm"] = self.minimum_matches

        if self.returned_fields:
            params['fl'] = ','.join(self.returned_fields)

        if self.sort_str:
            splits = self.sort_str.split("-", 1)
            if len(splits) > 1:
                sort_field = splits[1]
                sort_dir = "desc"
            else:
                sort_field = splits[0]
                sort_dir = "asc"

            params['sort'] = "%s %s" % (sort_field, sort_dir)

        if self.group_fields:
            params['group'] = "on"
            params['group.ngroups'] = "true"
            params['group.field'] = self.group_fields
            params['group.limit'] = self.group_limit
            params.update(dict([
                ("group.{}".format(k), v) for k, v in
                self.group_options.items()
            ]))

        if self.facet_fields:
            params['facet'] = "on"
            params['facet.field'] = self.facet_fields
            params['facet.mincount'] = self.mincount
            params.update(dict([
                ("facet.{}".format(k), v) for k, v in
                self.facet_options.items()
            ]))

        if self.stats_fields:
            params['stats'] = "true"
            params['stats.field'] = self.stats_fields

        if self.boost_func:
            params['bf'] = "%s^%s" % (self.boost_func, self.bf_weight)

        if self.boost_query:
            params['bq'] = "%s^%s" % (self.boost_query, self.bq_weight)

        return params

    def get(self, *args, **kwargs):
        """
        Fetch one document from SOLR

        :return: document from SOLR
        :rtype: SolrDoc
        """
        return self.filter(*args, **kwargs).one()

    def create(self, field_weights=None, **kwargs):
        """
        Create one document in SOLR

        :return: document created in SOLR
        :rtype: SolrDoc
        """
        doc = self.doc_class(
            field_weights=field_weights,
            **kwargs
        )

        kwargs = {"%s__eq" % doc.unique_key: doc.get_unique_field()}
        if self.get(**kwargs):
            raise solr_errors.SolrDuplicateUniqueKeyError(
                doc.get_unique_field()
            )
        doc.index(commit=True)
        return doc

    def update(self, field_weights=None, **kwargs):
        """
        Update one document in SOLR

        :return: document created in SOLR
        :rtype: SolrDoc
        """
        doc = self.doc_class(
            partial_update=True,
            field_weights=field_weights,
            **kwargs
        )
        kwargs = {"%s__eq" % doc.unique_key: doc.get_unique_field()}
        solr_doc = self.get(**kwargs)
        if solr_doc is None or solr_doc.get_unique_field() != \
                doc.get_unique_field():
            raise solr_errors.SolrDocumentNotExistError(doc.get_unique_field())
        doc.index(commit=True)
        return doc

    def raw(self, **extra):
        """
        Retrieve matched documents from SOLR in json format

        :return: documents from SOLR
        :rtype: list of json
        """
        return self.doc_class.solr.select(self.query, **extra)

    def groups(self, **extra):
        """
        Retrieve document groups when group is ON in the query

        :return: document groups of SOLR documents
        :rtype: dict
        """
        result = self.raw(groups=True, **extra)

        return result['groups']

    def facets(self, **extra):
        """
        Retrieve document facets when facet is ON in the query

        :return: facet counts of SOLR documents
        :rtype: dict
        """
        result = self.raw(facets=True, **extra)

        return result['facets']

    def all(self, **extra):
        """
        Retrieve all matched documents from SOLR and convert them into SolrDocs

        :return: documents from SOLR
        :rtype: list of SolrDoc
        """
        from wukong.models import SolrDocs

        result = self.raw(**extra)

        return SolrDocs(docs=self.doc_class.from_json_docs(result['docs']))

    def one(self, **extra):
        """
        Get one document from SOLR and convert it into SolrDoc

        :return: one document from SOLR
        :rtype: SolrDoc
        """
        new_query_manager = self.limit(1)

        result = new_query_manager.raw(**extra)

        if len(result['docs']) == 0:
            return None

        return self.doc_class(**result['docs'][0])
