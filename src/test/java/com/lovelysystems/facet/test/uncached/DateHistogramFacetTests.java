package com.lovelysystems.facet.test.uncached;


import com.lovelysystems.facet.test.AbstractNodes;
import com.lovelysystems.facet.uncached.datehistogram.InternalFullDateHistogramFacet;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
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
import org.testng.annotations.AfterMethod;
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
        startNode("server2");
        client = getClient();
        setupTemplates(client);
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @AfterMethod
    public void tearDownData() {
        client.admin().indices().prepareDelete("data_1").execute().actionGet();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testSimple() throws Exception {
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
                .startObject("uncached_date_histogram")
                .field("field", "created_at")
                .field("valueField", "total")
                .field("interval", "week")
                .endObject()
                .endObject()
                .startObject("long_result")
                .startObject("uncached_date_histogram")
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
                        "\"_type\":\"uncached_date_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":1," +
                        "\"min\":2.0," +
                        "\"max\":2.0," +
                        "\"total\":2.0," +
                        "\"total_count\":1," +
                        "\"mean\":2.0" +
                        "}]" +
                        "}}"));

        facet = (InternalFullDateHistogramFacet) response.facets().facet("long_result");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"long_result\":{" +
                        "\"_type\":\"uncached_date_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":1," +
                        "\"min\":5.0," +
                        "\"max\":5.0," +
                        "\"total\":5.0," +
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
    }

    @Test
    public void testMultidata() throws Exception {
        client.prepareIndex("data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("total", 2)
                        .field("floattotal", 2.0)
                        .field("more", 2)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("data_1", "data", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("total", 5)
                        .field("floattotal", 5.0)
                        .field("more", 5)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("data_1", "data", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .field("total", 3)
                        .field("floattotal", 3.0)
                        .field("more", 3)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("data_1", "data", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .field("total", -24)
                        .field("floattotal", -24.0)
                        .field("more", -24)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("data_1", "data", "5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .field("total", -24)
                        .field("floattotal", -24.0)
                        .field("more", -24)
                        .endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("int_result")
                .startObject("uncached_date_histogram")
                .field("field", "created_at")
                .field("valueField", "total")
                .field("interval", "week")
                .endObject()
                .endObject()
                .startObject("long_result")
                .startObject("uncached_date_histogram")
                .field("field", "created_at")
                .field("valueField", "more")
                .field("interval", "week")
                .endObject()
                .endObject()
                .startObject("float_result")
                .startObject("uncached_date_histogram")
                .field("field", "created_at")
                .field("valueField", "floattotal")
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
                        "\"_type\":\"uncached_date_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":2," +
                        "\"min\":2.0," +
                        "\"max\":5.0," +
                        "\"total\":7.0," +
                        "\"total_count\":2," +
                        "\"mean\":3.5" +
                        "},{" +
                        "\"time\":1555200000," +
                        "\"count\":3," +
                        "\"min\":-24.0," +
                        "\"max\":3.0," +
                        "\"total\":-45.0," +
                        "\"total_count\":3," +
                        "\"mean\":-15.0" +
                        "}]" +
                        "}}"));
        facet = (InternalFullDateHistogramFacet) response.facets().facet("long_result");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"long_result\":{" +
                        "\"_type\":\"uncached_date_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":2," +
                        "\"min\":2.0," +
                        "\"max\":5.0," +
                        "\"total\":7.0," +
                        "\"total_count\":2," +
                        "\"mean\":3.5" +
                        "},{" +
                        "\"time\":1555200000," +
                        "\"count\":3," +
                        "\"min\":-24.0," +
                        "\"max\":3.0," +
                        "\"total\":-45.0," +
                        "\"total_count\":3," +
                        "\"mean\":-15.0" +
                        "}]" +
                        "}}"));
        facet = (InternalFullDateHistogramFacet) response.facets().facet("float_result");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"float_result\":{" +
                        "\"_type\":\"uncached_date_histogram\"," +
                        "\"entries\":[" +
                        "{" +
                        "\"time\":950400000," +
                        "\"count\":2," +
                        "\"min\":2.0," +
                        "\"max\":5.0," +
                        "\"total\":7.0," +
                        "\"total_count\":2," +
                        "\"mean\":3.5" +
                        "},{" +
                        "\"time\":1555200000," +
                        "\"count\":3," +
                        "\"min\":-24.0," +
                        "\"max\":3.0," +
                        "\"total\":-45.0," +
                        "\"total_count\":3," +
                        "\"mean\":-15.0" +
                        "}]" +
                        "}}"));
    }

}
