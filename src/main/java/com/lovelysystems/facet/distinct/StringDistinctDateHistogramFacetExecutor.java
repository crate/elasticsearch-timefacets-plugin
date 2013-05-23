//package com.lovelysystems.facet.distinct;
//
//import org.apache.lucene.index.IndexReader;
//import org.elasticsearch.common.CacheRecycler;
//import org.elasticsearch.common.joda.time.MutableDateTime;
//import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
//import org.elasticsearch.index.mapper.FieldMapper;
//import org.elasticsearch.index.mapper.MapperService;
//import org.elasticsearch.search.facet.Facet;
//import org.elasticsearch.search.facet.FacetExecutor;
//import org.elasticsearch.search.facet.FacetPhaseExecutionException;
//import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
//import org.elasticsearch.search.internal.SearchContext;
//
//import java.io.IOException;
//
///**
// * Collect the distinct values per time interval.
// *
// * Field cache is used for the key and the value field.
// */
//public class StringDistinctDateHistogramFacetExecutor extends FacetExecutor {
//
//    private final String keyIndexFieldName;
//    private final String valueIndexFieldName;
//
//    private MutableDateTime dateTime;
//
//    private final DateHistogramFacet.ComparatorType comparatorType;
//
//    private final FieldDataCache fieldDataCache;
//
//    private final FieldDataType keyFieldDataType;
//    private LongFieldData keyFieldData;
//    private final FieldDataType valueFieldDataType;
//
//    private final DateHistogramProc histoProc;
//
//    public StringDistinctDateHistogramFacetExecutor(String facetName, String keyFieldName, String distinctFieldName, MutableDateTime dateTime, long interval, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
//        super(facetName);
//        this.dateTime = dateTime;
//        this.comparatorType = comparatorType;
//        this.fieldDataCache = context.fieldDataCache();
//
//        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(keyFieldName);
//        if (smartMappers == null || !smartMappers.hasMapper()) {
//            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyFieldName + "]");
//        }
//
//        // add type filter if there is exact doc mapper associated with it
//        if (smartMappers.hasDocMapper()) {
//            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
//        }
//
//        keyIndexFieldName = smartMappers.mapper().names().indexName();
//        keyFieldDataType = smartMappers.mapper().fieldDataType();
//
//
//        FieldMapper mapper = context.smartNameFieldMapper(distinctFieldName);
//        if (mapper == null) {
//            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + distinctFieldName + "]");
//        }
//        valueIndexFieldName = mapper.names().indexName();
//        valueFieldDataType = mapper.fieldDataType();
//
//        this.histoProc = new DateHistogramProc(interval);
//    }
//
//    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
//        keyFieldData = (LongFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyIndexFieldName);
//        histoProc.valueFieldData = (StringFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueIndexFieldName);
//    }
//
//    @Override protected void doCollect(int doc) throws IOException {
//        keyFieldData.forEachValueInDoc(doc, dateTime, histoProc);
//    }
//
//    @Override public Facet facet() {
//        return new StringInternalDistinctDateHistogramFacet(facetName, comparatorType, histoProc.entries, true);
//    }
//
//    /**
//     * Collect the time intervals in value aggregators for each time interval found.
//     * The value aggregator finally contains the facet entry.
//     */
//    public static class DateHistogramProc implements LongFieldData.DateValueInDocProc {
//
//        final ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> entries = CacheRecycler.popLongObjectMap();
//
//        StringFieldData valueFieldData;
//
//        private final long interval;
//
//        final ValueAggregator valueAggregator = new ValueAggregator();
//
//        public DateHistogramProc(long interval) {
//            this.interval = interval;
//        }
//
//        /*
//         * for each time interval an entry is created in which the distinct values are aggregated
//         */
//        @Override public void onValue(int docId, MutableDateTime dateTime) {
//            long time = dateTime.getMillis();
//            if (interval != 1) {
//                time = ((time / interval) * interval);
//            }
//
//            InternalDistinctDateHistogramFacet.DistinctEntry entry = entries.get(time);
//            if (entry == null) {
//                entry = new InternalDistinctDateHistogramFacet.DistinctEntry(time);
//                entries.put(time, entry);
//            }
//            valueAggregator.entry = entry;
//            valueFieldData.forEachValueInDoc(docId, valueAggregator);
//        }
//
//        /*
//         * aggregates the values in a set
//         */
//        public static class ValueAggregator implements StringFieldData.StringValueInDocProc {
//
//            InternalDistinctDateHistogramFacet.DistinctEntry entry;
//
//            @Override public void onValue(int docId, String value) {
//                entry.getValue().add(value);
//            }
//
//            @Override public void onMissing(int docId) {
//            }
//        }
//    }
//}
