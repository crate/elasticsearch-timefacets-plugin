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


Distinct Date Histogram Facet
=============================

This facet counts distinct values for string fields.

Example::

    {
        "query" : {
            "match_all" : {}
        },
        "facets" : {
            "distinct" : {
                "distinct_date_histogram" : {
                    "field" : "field_name",
                    "value_field" : "value_field_name",
                    "interval" : "day"
                }
            }
        }
    }

Works like the "date_histogram" with these exceptions:

    - value_field is mandatory
    - value_field must be String
    - no value_script


Maven
=====

To use this project in with maven follow the steps described at
https://github.com/lovelysystems/maven


Deployment
==========

The distributionManagement section in the pom contains the actual
repository urls on github. It will lead to an error if you try to
deploy to those urls, because these are no Maven API endpoints, where
maven could upload the artifacts.

So to deploy to the Lovely Systems Maven repository first clone
https://github.com/lovelysystems/maven to your local machine and set
the deployment target location on the commandline like this::

 mvn -DaltDeploymentRepository=repo::default::file:../maven/releases clean deploy

After deployment simply commit the changes in the maven repository
project and push.

This approach was take from the very useful blog entry at
http://cemerick.com/2010/08/24/hosting-maven-repos-on-github/

