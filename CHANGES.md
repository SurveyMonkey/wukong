
#Release Notes

1.1.0
==========
- Catch RequestsException Instead Of Connection Errors To Allow For Retries during timeouts (#30)

1.0.1
==========
- search(): _actually_ send the query_dict in the request body as was intended in 1.0.0 (missed a cherry-pick)

1.0.0
==========
- Removed support for Python versions < 3.5 and pypy
- Use POST instead of GET for /select API calls in order to avoid
  exceeding the max uri length for long queries (#26)

0.5.1
==========
- Only add /solr/ to the end of a solr url if it isn't already there

0.5.0
==========
- Simplify the method of making requests, and add logging so it's more visible what's happening (#18)
- Don't log debugging messages as error messages (#19)
- Broaden Travis support (#17)
- Make sure a scheme is always on the solr request (#20)

0.4.0
==========
- Add ability to update extraparams like omitHeader for solr requests

0.3.1
==========
- Collection alias support

0.2.2
==========
- Fix zookeeper cluster state unicode issue

0.2.0
==========
- Zookeeper states for Solr 6
- handle situations with unknown Solr hostnames
- support stats in the query for Solr 6

0.1.1
==========
- Add support for Python 3.x

0.0.3
==========
- initial version for wukong
