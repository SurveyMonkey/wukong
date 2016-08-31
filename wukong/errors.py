class SolrError(Exception):

    def __init__(self, message=None, status_code=None):
        if isinstance(message, tuple):
            message = message[1]

        super(SolrError, self).__init__(message)
        self.status_code = status_code


class SolrSchemaUpdateError(SolrError):

    def __init__(self, fields, message=None, status_code=None):
        self.fields = [field["name"] for field in fields]
        if message is None:
            message = "Unable to update SOLR schema for %s" % ", ".join(
                self.fields
            )
        super(SolrSchemaUpdateError, self).__init__(
            message=message,
            status_code=status_code
        )


class SolrSchemaValidationError(SolrError):

    def __init__(self, field, message=None):
        self.field = field
        if message is None:
            message = "Field (%s) is not in SOLR schema fields" % field
        super(SolrSchemaValidationError, self).__init__(message=message)


class SolrDuplicateUniqueKeyError(SolrError):
    def __init__(self, pk):
        self.pk = pk
        message = "The unique key %s already exists" % self.pk
        super(SolrDuplicateUniqueKeyError, self).__init__(message=message)


class SolrDocumentNotExistError(SolrError):
    def __init__(self, pk):
        self.pk = pk
        message = "The document with unique key %s does not exist" % self.pk
        super(SolrDocumentNotExistError, self).__init__(message=message)


class SolrDeleteUniqueKeyError(SolrError):
    def __init__(self, pk):
        self.pk = pk
        message = "The unique key %s can not be deleted" % self.pk
        super(SolrDeleteUniqueKeyError, self).__init__(message=message)


class SolrUnspecifiedOperatorError(SolrError):
    def __init__(self, field_name):
        self.field_name = field_name
        message = "The operator is not specified in %s" % self.field_name
        super(SolrUnspecifiedOperatorError, self).__init__(message=message)


class SolrUnspportedOperatorError(SolrError):
    def __init__(self, operator):
        self.operator = operator
        message = "The operator:%s is not supported" % self.operator
        super(SolrUnspportedOperatorError, self).__init__(message=message)
