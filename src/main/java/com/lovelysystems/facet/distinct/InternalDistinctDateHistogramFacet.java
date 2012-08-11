package com.lovelysystems.facet.distinct;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.datehistogram.InternalDateHistogramFacet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class InternalDistinctDateHistogramFacet extends InternalDateHistogramFacet {

    private static final String STREAM_TYPE = "DistinctDateHistogram";

    public static final String TYPE = "distinct_date_histogram";

    public static void registerStreams() {
        InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static InternalFacet.Stream STREAM = new InternalFacet.Stream() {
        @Override public Facet readFacet(String type, StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override public String streamType() {
        return STREAM_TYPE;
    }


    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     * 
     * It holds a set of distinct values and the time.
     */
    public static class DistinctEntry implements Entry {
        private final long time;
        private final Set<Object> values;

        public DistinctEntry(long time, Set<Object> values) {
            this.time = time;
            this.values = values;
        }

        public DistinctEntry(long time) {
            this.time = time;
            this.values = new HashSet<Object>();
        }

        @Override public long time() {
            return time;
        }

        @Override public long getTime() {
            return time();
        }

        public Set<Object> value() {
            return this.values;
        }

        public Set<Object> getValue() {
            return value();
        }

        @Override public long count() {
            return value().size();
        }

        @Override public long getCount() {
            return count();
        }

        @Override public long totalCount() {
            return 0;
        }

        @Override public long getTotalCount() {
            return 0;
        }

        @Override public double total() {
            return Double.NaN;
        }

        @Override public double getTotal() {
            return total();
        }

        @Override public double mean() {
            return Double.NaN;
        }

        @Override public double getMean() {
            return mean();
        }

        @Override public double min() {
            return Double.NaN;
        }

        @Override public double getMin() {
            return Double.NaN;
        }

        @Override public double max() {
            return Double.NaN;
        }

        @Override public double getMax() {
            return Double.NaN;
        }
    }

    protected String name;

    protected ComparatorType comparatorType;

    ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> tEntries;
    boolean cachedEntries;
    Collection<DistinctEntry> entries = null;

    private InternalDistinctDateHistogramFacet() {
    }

    public InternalDistinctDateHistogramFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> entries, boolean cachedEntries) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.tEntries = entries;
        this.cachedEntries = cachedEntries;
        this.entries = entries.valueCollection();
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return name();
    }

    @Override public String type() {
        return TYPE;
    }

    @Override public String getType() {
        return type();
    }

    @Override public List<DistinctEntry> entries() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<DistinctEntry>(entries);
        }
        return (List<DistinctEntry>) entries;
    }

    @Override public List<DistinctEntry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    void releaseCache() {
        if (cachedEntries) {
            CacheRecycler.pushLongObjectMap(tEntries);
            cachedEntries = false;
            tEntries = null;
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            // we need to sort it
            InternalDistinctDateHistogramFacet internalFacet = (InternalDistinctDateHistogramFacet) facets.get(0);
            List<DistinctEntry> entries = internalFacet.entries();
            Collections.sort(entries, comparatorType.comparator());
            internalFacet.releaseCache();
            return internalFacet;
        }

        ExtTLongObjectHashMap<DistinctEntry> map = CacheRecycler.popLongObjectMap();
        for (Facet facet : facets) {
            InternalDistinctDateHistogramFacet histoFacet = (InternalDistinctDateHistogramFacet) facet;
            for (DistinctEntry fullEntry : histoFacet.entries) {
                DistinctEntry current = map.get(fullEntry.time);
                if (current != null) {
                    current.values.addAll(fullEntry.values);
                } else {
                    map.put(fullEntry.time, fullEntry);
                }
            }
            histoFacet.releaseCache();
        }

        // sort
        Object[] values = map.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());
        List<DistinctEntry> ordered = new ArrayList<DistinctEntry>(map.size());
        for (int i = 0; i < map.size(); i++) {
            DistinctEntry value = (DistinctEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        CacheRecycler.pushLongObjectMap(map);

        // just initialize it as already ordered facet
        InternalDistinctDateHistogramFacet ret = new InternalDistinctDateHistogramFacet();
        ret.name = name;
        ret.comparatorType = comparatorType;
        ret.entries = ordered;
        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("count");
    }

    /**
     * Builds the final JSON result.
     *
     * For each time entry we provide the number of distinct values in the time range.
     */
    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Set<Object> all = null;
        if (entries().size() != 1) {
            all = new HashSet<Object>();
        }
        builder.startObject(name);
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for (DistinctEntry entry : entries) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time());
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
            if (entries().size() == 1) {
                all = entry.value();
            } else {
                all.addAll(entry.value());
            }
        }
        builder.endArray();
        builder.field(Fields.TOTAL_COUNT, all.size());
        builder.endObject();
        return builder;
    }

    public static InternalDistinctDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalDistinctDateHistogramFacet facet = new InternalDistinctDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    /**
     * The reader for the internal transport protocol.
     */
    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());

        cachedEntries = false;
        int size = in.readVInt();
        entries = new ArrayList<DistinctEntry>(size);
        for (int i = 0; i < size; i++) {
            long time = in.readLong();
            int nameSize = in.readVInt();
            Set<Object> names = new HashSet<Object>(nameSize);
            for (int j = 0; j < nameSize; j++) {
                names.add(in.readUTF());
            }
            entries.add(new DistinctEntry(time, names));
        }
    }

    /**
     * The writer for the internal transport protocol.
     */
    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(entries.size());
        for (DistinctEntry entry : entries) {
            out.writeLong(entry.time);
            out.writeVInt(entry.getValue().size());

            for (Object name : entry.getValue()) {
                out.writeUTF((String) name);
            }
        }
        releaseCache();
    }
}