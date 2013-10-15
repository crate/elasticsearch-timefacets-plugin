package crate.elasticsearch.facet.distinct;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Collect the distinct values per time interval.
 */
public class LongDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData distinctIndexFieldData;


    private MutableDateTime dateTime;
    private final long interval;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final Recycler.V<ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>> entries;
    private final CacheRecycler cacheRecycler;

    public LongDistinctDateHistogramFacetExecutor(IndexNumericFieldData keyIndexFieldData,
                                                  IndexNumericFieldData distinctIndexFieldData,
                                                  MutableDateTime dateTime, long interval, DateHistogramFacet.ComparatorType comparatorType,
                                                  final CacheRecycler cacheRecycler) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.distinctIndexFieldData = distinctIndexFieldData;
        this.entries = cacheRecycler.longObjectMap(-1);
        this.dateTime = dateTime;
        this.interval = interval;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        ArrayList<LongInternalDistinctDateHistogramFacet.DistinctEntry> entries1 = new ArrayList<LongInternalDistinctDateHistogramFacet.DistinctEntry>(entries.v().valueCollection());

        entries.release();
        return new LongInternalDistinctDateHistogramFacet(facetName, comparatorType, entries1);
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
            this.histoProc = new DateHistogramProc(entries.v(), dateTime, interval);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = distinctIndexFieldData.load(context).getLongValues();
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
     *
     *
     */
    public static class DateHistogramProc extends LongFacetAggregatorBase {

        private int total;
        private int missing;
        LongValues valueValues;
        private final long interval;
        private MutableDateTime dateTime;
        final ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> entries;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>  entries, MutableDateTime dateTime, long interval) {
            this.dateTime = dateTime;
            this.entries = entries;
            this.interval = interval;
        }

        /*
         * Extend the onDoc implementation of LongFacetAggregatorBase to pass a dateTime to onValue
         * to account for the interval and rounding that is set in the Parser
         */
        @Override
        public void onDoc(int docId, LongValues values) {
            if (values.hasValue(docId)) {
                final LongValues.Iter iter = values.getIter(docId);
                while (iter.hasNext()) {
                    dateTime.setMillis(iter.next());
                    //dateTime = new MutableDateTime(iter.next());
                    onValue(docId, dateTime);
                    total++;
                }
            } else {
                missing++;
            }
        }

        protected void onValue(int docId, MutableDateTime dateTime) {
            long time = dateTime.getMillis();
            onValue(docId, time);
        }

        /*
         * for each time interval an entry is created in which the distinct values are aggregated
         */
        @Override
        protected void onValue(int docId, long time) {
            if (interval != 1) {
                time = ((time / interval) * interval);
            }

            InternalDistinctDateHistogramFacet.DistinctEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalDistinctDateHistogramFacet.DistinctEntry(time);
                entries.put(time, entry);
            }
            valueAggregator.entry = entry;
            valueAggregator.onDoc(docId, valueValues);
        }

        /*
         * aggregates the values in a set
         */
        public final static class ValueAggregator extends LongFacetAggregatorBase {

            InternalDistinctDateHistogramFacet.DistinctEntry entry;

            @Override
            public void onValue(int docId, long value) {
                entry.getValues().add(value);
            }
        }
    }
}
