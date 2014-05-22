package crate.elasticsearch.facet.distinct;

import org.elasticsearch.common.hppc.LongObjectOpenHashMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.datehistogram.InternalDateHistogramFacet;

import java.io.IOException;
import java.util.*;

/**
 */
public abstract class InternalDistinctDateHistogramFacet extends InternalDateHistogramFacet {

    public static final String TYPE = "distinct_date_histogram";
    protected ComparatorType comparatorType;

    Collection<DistinctEntry> entries = null;

    public InternalDistinctDateHistogramFacet() {
    }

    public InternalDistinctDateHistogramFacet(String facetName) {
        super(facetName);
    }

    public static void registerStreams() {
        LongInternalDistinctDateHistogramFacet.registerStreams();
        StringInternalDistinctDateHistogramFacet.registerStreams();
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

        public long getTime() {
            return time;
        }

        public Set<Object> getValues() {
            return this.values;
        }

        public long getCount() {
            return this.values.size();
        }

        public long getTotalCount() {
            return 0;
        }

        public double getTotal() {
            return Double.NaN;
        }

        public double getMean() {
            return Double.NaN;
        }

        public double getMin() {
            return Double.NaN;
        }

        public double getMax() {
            return Double.NaN;
        }
    }

    public List<DistinctEntry> entries() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<DistinctEntry>(entries);
        }
        return (List<DistinctEntry>) entries;
    }

    public List<DistinctEntry> getEntries() {
        return entries();
    }

    public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("count");
    }

    @Override
    public Facet reduce(ReduceContext context) { 
        List<Facet> facets = context.facets();

        if (facets.size() == 1) {
            // we need to sort it
            InternalDistinctDateHistogramFacet internalFacet = (InternalDistinctDateHistogramFacet) facets.get(0);
            List<DistinctEntry> entries = internalFacet.entries();
            Collections.sort(entries, comparatorType.comparator());
            return internalFacet;
        }

        Recycler.V<LongObjectOpenHashMap<DistinctEntry>> map = context.cacheRecycler().longObjectMap(-1);
        for (Facet facet : facets) {

            InternalDistinctDateHistogramFacet histoFacet = (InternalDistinctDateHistogramFacet) facet;
            for (DistinctEntry fullEntry : histoFacet.entries) {

                if(fullEntry != null){
                    DistinctEntry current = map.v().get(fullEntry.getTime());
                    if (current != null) {
                        current.getValues().addAll(fullEntry.getValues());
                    } else {
                        map.v().put(fullEntry.getTime(), fullEntry);
                    }
                }

            }
        }

        // sort
        Object[] values = map.v().values;
        Arrays.sort(values, (Comparator) comparatorType.comparator());
        List<DistinctEntry> ordered = new ArrayList<DistinctEntry>(map.v().size());
        for (int i = 0; i < map.v().size(); i++) {
            DistinctEntry value = (DistinctEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        map.release();

        // just initialize it as already ordered facet
        InternalDistinctDateHistogramFacet ret = newFacet();
        ret.comparatorType = comparatorType;
        ret.entries = ordered;
        return ret;
    }

    protected abstract InternalDistinctDateHistogramFacet newFacet();

    /**
     * Builds the final JSON result.
     *
     * For each time entry we provide the number of distinct values in the time range.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Set<Object> all = null;
        if (entries().size() != 1) {
            all = new HashSet<Object>();
        }
        builder.startObject(getName());
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for (DistinctEntry entry : entries) {
            builder.startObject();
            builder.field(Fields.TIME, entry.getTime());
            builder.field(Fields.COUNT, entry.getCount());
            builder.endObject();
            if (entries().size() == 1) {
                all = entry.getValues();
            } else {
                all.addAll(entry.getValues());
            }
        }
        builder.endArray();
        builder.field(Fields.TOTAL_COUNT, all.size());
        builder.endObject();
        return builder;
    }
}