===================================
Lovelysystems Elasticsearch Plugins
===================================


Uncached Facets
===============

The uncached facets try to avoid the field cache to save memory.

Date Histogram for Int/Long Values
----------------------------------

Example::

    {
        "query" : {
            "match_all" : {}
        },
        "facets" : {
            "histo1" : {
                "uncached_date_histogram" : {
                    "field" : "field_name",
                    "value_field" : "value_field_name",
                    "interval" : "day"
                }
            }
        }
    }

The field cache is used for "field" but not for "value_field".

Works like the "date_histogram" with these exceptions:

    - value_field is mandatory
    - value_field must be Int or Long
    - no value_script

