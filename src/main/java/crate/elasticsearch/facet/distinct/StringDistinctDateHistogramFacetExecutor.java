package crate.elasticsearch.facet.distinct;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.plain.PackedArrayIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;

import java.io.IOException;
import java.util.ArrayList;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;

/**
 * Collect the distinct values per time interval.
 */
public class StringDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final PackedArrayIndexFieldData keyIndexFieldData;
    private final PagedBytesIndexFieldData distinctIndexFieldData;


    private MutableDateTime dateTime;
    private final long interval;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final Recycler.V<LongObjectOpenHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>> entries;

    public StringDistinctDateHistogramFacetExecutor(PackedArrayIndexFieldData keyIndexFieldData,
                                                    PagedBytesIndexFieldData distinctIndexFieldData,
                                                    MutableDateTime dateTime, long interval, DateHistogramFacet.ComparatorType comparatorType,
                                                    final CacheRecycler cacheRecycler) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.distinctIndexFieldData = distinctIndexFieldData;
        this.entries = cacheRecycler.longObjectMap(-1);
        this.dateTime = dateTime;
        this.interval = interval;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        ArrayList<InternalDistinctDateHistogramFacet.DistinctEntry> entries1 = new ArrayList<InternalDistinctDateHistogramFacet.DistinctEntry>(entries.v().size());
        final boolean[] states = entries.v().allocated;
        final Object[] values = entries.v().values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                InternalDistinctDateHistogramFacet.DistinctEntry value = (InternalDistinctDateHistogramFacet.DistinctEntry) values[i];
                entries1.add(value);
            }
        }
        entries.close();
        return new StringInternalDistinctDateHistogramFacet(facetName, comparatorType, entries1);
    }

    /*
     * Similar to the Collector from the ValueDateHistogramFacetExecutor
     *
     * Only difference is that dateTime and interval is passed to DateHistogramProc instead of tzRounding
     */
    class Collector extends FacetExecutor.Collector {

        private LongValues keyValues;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(entries, dateTime, interval);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = distinctIndexFieldData.load(context).getBytesValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, keyValues);
        }

        @Override
        public void postCollection() {
        }
    }


    /**
     * Collect the time intervals in value aggregators for each time interval found.
     * The value aggregator finally contains the facet entry.
     */
    public static class DateHistogramProc  {

        private int total;
        private int missing;
        BytesValues.WithOrdinals valueValues;
        private final long interval;
        private MutableDateTime dateTime;
        final Recycler.V<LongObjectOpenHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>> entries;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(Recycler.V<LongObjectOpenHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>> entries, MutableDateTime dateTime, long interval) {
            this.dateTime = dateTime;
            this.entries = entries;
            this.interval = interval;
        }

        /*
         * Pass a dateTime to onValue to account for the interval and rounding that is set in the Parser
         */
        public void onDoc(int docId, LongValues values) {
            int totalDocumentEntries = values.setDocument(docId);
            for(int i = 0 ; i < totalDocumentEntries ; i++) {
                dateTime.setMillis(values.nextValue());
                onValue(docId, dateTime);
                total++;
            }
            if(totalDocumentEntries > 0)
                missing++;
        }

        protected void onValue(int docId, MutableDateTime dateTime) {
            long time = dateTime.getMillis();
            onValue(docId, time);
        }

        /*
         * for each time interval an entry is created in which the distinct values are aggregated
         */
        protected void onValue(int docId, long time) {
            if (interval != 1) {
                time = ((time / interval) * interval);
            }

            InternalDistinctDateHistogramFacet.DistinctEntry entry = entries.v().get(time);
            if (entry == null) {
                entry = new InternalDistinctDateHistogramFacet.DistinctEntry(time);
                entries.v().put(time, entry);
            }
            valueAggregator.entry = entry;
            valueAggregator.onDoc(docId, valueValues);
        }

        public final int total() {
            return total;
        }

        public final int missing() {
            return missing;
        }

        /*
         * aggregates the values in a set
         */
        public final static class ValueAggregator extends HashedAggregator {

            InternalDistinctDateHistogramFacet.DistinctEntry entry;

            @Override
            protected void onValue(int docId, BytesRef value, int hashCode, BytesValues values) {
                entry.getValues().add(value.utf8ToString());
            }
        }
    }
}
