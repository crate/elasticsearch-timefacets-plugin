package com.lovelysystems.facet.test.uncached;


import com.lovelysystems.facet.test.AbstractNodes;
import com.lovelysystems.facet.uncached.datehistogram.InternalFullDateHistogramFacet;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DateHistogramFacetTests extends AbstractNodes {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }


    @Test
    public void testIt() throws Exception {
        String settings = XContentFactory.jsonBuilder()
                .startObject()
                .field("number_of_shards", 4)
                .field("number_of_replicas", 0)
                .startArray("aliases").value("data").endArray()
                .endObject().string();
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("data")
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_source").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("created_at").field("type", "date").field("store", "yes").endObject()
                .startObject("total").field("type", "integer").field("store", "yes").endObject()
                .startObject("more").field("type", "long").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject().string();

        client.admin().indices().preparePutTemplate("data")
                .setTemplate("data_*")
                .setSettings(settings)
                .addMapping("data", mapping)
                .execute().actionGet();

        Thread.sleep(100); // sleep a bit here..., so the mappings get applied

        client.prepareIndex("data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("total", 2)
                        .field("more", 5)
                        .endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();
        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("int_result")
                .startObject("uncached_histogram")
                .field("field", "created_at")
                .field("valueField", "total")
                .field("interval", "week")
                .endObject()
                .endObject()
                .startObject("long_result")
                .startObject("uncached_histogram")
                .field("field", "created_at")
                .field("valueField", "more")
                .field("interval", "week")
                .endObject()
                .endObject()
                .endObject();

        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.copiedBytes())
                .execute().actionGet();
        MatcherAssert.assertThat(response.shardFailures().length, Matchers.equalTo(0));
        InternalFullDateHistogramFacet facet = (InternalFullDateHistogramFacet) response.facets().facet("int_result");
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"int_result\":{" +
                        "\"_type\":\"uncached_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":1," +
                        "\"min\":2," +
                        "\"max\":2," +
                        "\"total\":2," +
                        "\"total_count\":1," +
                        "\"mean\":2.0" +
                        "}]" +
                        "}}"));

        facet = (InternalFullDateHistogramFacet) response.facets().facet("long_result");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"long_result\":{" +
                        "\"_type\":\"uncached_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":1," +
                        "\"min\":5," +
                        "\"max\":5," +
                        "\"total\":5," +
                        "\"total_count\":1," +
                        "\"mean\":5.0" +
                        "}]" +
                        "}}"));

        client.prepareIndex("data_1", "data", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("total", 5)
                        .field("more", 5)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("data_1", "data", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .field("total", 3)
                        .field("more", 3)
                        .endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();
        response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.copiedBytes())
                .execute().actionGet();
        MatcherAssert.assertThat(response.shardFailures().length, Matchers.equalTo(0));
        facet = (InternalFullDateHistogramFacet) response.facets().facet("int_result");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"int_result\":{" +
                        "\"_type\":\"uncached_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":2," +
                        "\"min\":2," +
                        "\"max\":5," +
                        "\"total\":7," +
                        "\"total_count\":2," +
                        "\"mean\":3.5" +
                        "},{" +
                        "\"time\":1555200000," +
                        "\"count\":1," +
                        "\"min\":3," +
                        "\"max\":3," +
                        "\"total\":3," +
                        "\"total_count\":1," +
                        "\"mean\":3.0" +
                        "}]" +
                        "}}"));
    }

}
