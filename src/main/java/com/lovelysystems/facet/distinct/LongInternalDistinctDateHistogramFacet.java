package com.lovelysystems.facet.distinct;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class LongInternalDistinctDateHistogramFacet extends InternalDistinctDateHistogramFacet{
    private static final String STREAM_TYPE = "LongDistinctDateHistogram";

    public static final String TYPE = "long_distinct_date_histogram";

    public LongInternalDistinctDateHistogramFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<DistinctEntry> entries, boolean cachedEntries) {
        super(name, comparatorType, entries, cachedEntries);
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
                names.add(in.readLong());
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
                out.writeLong((Long) name);
            }
        }
        releaseCache();
    }
}
