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

public class LongInternalLatestFacet implements InternalFacet {

    public static final String TYPE = "long_latest";

    private static final String STREAM_TYPE = "long_latest";

    public static void registerStreams() {
        InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);
    }

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    static InternalFacet.Stream STREAM = new InternalFacet.Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readDistinctTermsFacet(in);
        }
    };

    private String name;
    private int size;
    private int start;
    private int total = 0;

    public EntryPriorityQueue queue;

    public static class EntryPriorityQueue extends PriorityQueue<Entry> {

        public EntryPriorityQueue(int size) {
            initialize(size);
        }

        @Override
        protected boolean lessThan(Entry a, Entry b) {
            return a.value < b.value;
        }
    }

    public LongInternalLatestFacet() {
    }

    public void insert(TLongObjectMap<LongInternalLatestFacet.Entry> entries) {
        if (queue == null) {
            this.queue = new EntryPriorityQueue(start + size);
        }
        entries.forEachEntry(new TLongObjectProcedure<Entry>() {
            @Override
            public boolean execute(long key, Entry entry) {
                entry.key = key;
                queue.insertWithOverflow(entry);
                return true;
            }
        });
    }

    public LongInternalLatestFacet(String facetName, int size, int start, int total) {
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

    public static class Entry {
        public long ts;
        public long value;
        public long key;

        public Entry(long ts, long value) {
            this.ts = ts;
            this.value = value;
        }

        public Entry(long ts, long value, long key) {
            this.ts = ts;
            this.value = value;
            this.key = key;
        }

    }

    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        for (Facet facet : facets) {
            LongInternalLatestFacet f = (LongInternalLatestFacet) facet;
            if (this == facet) {
                continue;
            }
            while (f.queue.size() > 0) {
                queue.insertWithOverflow(f.queue.pop());
            }
            this.total += f.total;
        }
        return this;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {

        builder.startObject(name);
        builder.field(Fields._TYPE, TYPE);
        builder.field(Fields.TOTAL, total);
        builder.startArray(Fields.ENTRIES);
        int num_entries = queue.size() - start;

        if (num_entries > 0) {
            Entry[] entries = new Entry[num_entries];
            for (int i = entries.length - 1; i >= 0; i--) {
                entries[i] = queue.pop();
            }
            for (int i = 0; i < entries.length; i++) {
                Entry e = entries[i];
                builder.startObject();
                builder.field(Fields.VALUE, e.value);
                builder.field(Fields.KEY, e.key);
                builder.field(Fields.TS, e.ts);
                builder.endObject();
            }
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static LongInternalLatestFacet readDistinctTermsFacet(StreamInput in)
            throws IOException {
        LongInternalLatestFacet facet = new LongInternalLatestFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readUTF();
        this.size = in.readVInt();
        this.start = in.readVInt();
        this.total = in.readVInt();
        this.queue = new EntryPriorityQueue(start + size);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            queue.insertWithOverflow(new Entry(in.readVLong(), in.readVLong(),
                    in.readVLong()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVInt(size);
        out.writeVInt(start);
        out.writeVInt(total);
        out.writeVInt(queue.size());
        while (queue.size() > 0) {
            Entry e = queue.pop();
            out.writeVLong(e.ts);
            out.writeVLong(e.value);
            out.writeVLong(e.key);
        }
    }
}
