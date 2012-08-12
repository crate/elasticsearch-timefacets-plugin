package com.lovelysystems.facet.distinct;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class StringInternalDistinctDateHistogramFacet extends InternalDistinctDateHistogramFacet {

    private static final String STREAM_TYPE = "DistinctDateHistogram";

    public static void registerStreams() {
        InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);

    }

    public StringInternalDistinctDateHistogramFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<DistinctEntry> entries, boolean cachedEntries) {
        super(name, comparatorType, entries, cachedEntries);
    }

    @Override
    protected InternalDistinctDateHistogramFacet newFacet() {
        return new StringInternalDistinctDateHistogramFacet();
    }

    static InternalFacet.Stream STREAM = new InternalFacet.Stream() {
        @Override public Facet readFacet(String type, StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override public String streamType() {
        return STREAM_TYPE;
    }

    public StringInternalDistinctDateHistogramFacet() {
        super();
    }



    public static StringInternalDistinctDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        StringInternalDistinctDateHistogramFacet facet = new StringInternalDistinctDateHistogramFacet();
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
            out.writeLong(entry.getTime());
            out.writeVInt(entry.getValue().size());

            for (Object name : entry.getValue()) {
                out.writeUTF((String) name);
            }
        }
        releaseCache();
    }
}