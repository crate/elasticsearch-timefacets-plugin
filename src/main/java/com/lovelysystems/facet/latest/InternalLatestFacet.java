package com.lovelysystems.facet.latest;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

public abstract class InternalLatestFacet implements InternalFacet {

    public static final String TYPE = "latest";

    protected String name;
    protected int size;
    protected int start;
    protected int total = 0;

    public static void registerStreams() {
        IntInternalLatestFacet.registerStreams();
        LongInternalLatestFacet.registerStreams();
    }

    public InternalLatestFacet() {
    }

    public InternalLatestFacet(String facetName, int size, int start, int total) {
        this.size = size;
        this.start = start;
        this.name = facetName;
        this.total = total;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return type();
    }


    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString(
                "_type");
        static final XContentBuilderString TS = new XContentBuilderString("ts");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString ENTRIES = new XContentBuilderString(
                "entries");
        static final XContentBuilderString KEY = new XContentBuilderString(
                "key");
        static final XContentBuilderString VALUE = new XContentBuilderString(
                "value");
    }

    public abstract Facet reduce(String name, List<Facet> facets);

}
