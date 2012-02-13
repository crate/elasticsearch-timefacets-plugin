package com.lovelysystems.facet.uncached.datehistogram;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.datehistogram.CountDateHistogramFacetCollector;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.FieldsLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;

/**
 * A histogram facet collector that uses different fields for the key and the value.
 */
public class DateHistogramFacetCollector extends AbstractFacetCollector {

    private final String keyIndexFieldName;
    private final String valueIndexFieldName;

    private MutableDateTime dateTime;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType keyFieldDataType;
    private LongFieldData keyFieldData;

    private final DateHistogramProc histoProc;
    final private SearchLookup docReader;

    public DateHistogramFacetCollector(String facetName,
                                       String keyFieldName,
                                       String valueFieldName,
                                       MutableDateTime dateTime,
                                       long interval,
                                       DateHistogramFacet.ComparatorType comparatorType,
                                       SearchContext context
                                      ) {
        super(facetName);
        this.dateTime = dateTime;
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(keyFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyFieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper() && smartMappers.explicitTypeInName()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        keyIndexFieldName = smartMappers.mapper().names().indexName();
        keyFieldDataType = smartMappers.mapper().fieldDataType();

        FieldMapper mapper = context.smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + valueFieldName + "]");
        }
        FieldDataType fdt = mapper.fieldDataType();
        if (   (fdt != FieldDataType.DefaultTypes.INT)
                && (fdt != FieldDataType.DefaultTypes.LONG)
                && (fdt != FieldDataType.DefaultTypes.FLOAT)
                && (fdt != FieldDataType.DefaultTypes.DOUBLE)
           ) {
            throw new FacetPhaseExecutionException(facetName, "(value) field [" + valueFieldName + "] is not of type int,long or float");
        }
        valueIndexFieldName = mapper.names().indexName();

        docReader = new SearchLookup(context.mapperService(), null);
        this.histoProc = new DateHistogramProc(interval, docReader, valueIndexFieldName);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        docReader.setNextDocId(doc);
        keyFieldData.forEachValueInDoc(doc, dateTime, histoProc);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        docReader.setNextReader(reader);
        keyFieldData = (LongFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyIndexFieldName);
    }

    @Override
    public Facet facet() {
        return new InternalFullDateHistogramFacet(facetName, comparatorType, histoProc.entries, true);
    }

    public static class DateHistogramProc implements LongFieldData.DateValueInDocProc {

        final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        private final long interval;
        private final SearchLookup docReader;
        private final String fieldName;

        public DateHistogramProc(long interval, SearchLookup docReader, String fieldName) {
            this.interval = interval;
            this.docReader = docReader;
            this.fieldName = fieldName;
        }

        @Override
        public void onValue(int docId, MutableDateTime dateTime) {
            long time = dateTime.getMillis();
            if (interval != 1) {
                time = CountDateHistogramFacetCollector.bucket(time, interval);
            }

            InternalFullDateHistogramFacet.FullEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalFullDateHistogramFacet.FullEntry(time, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0, 0);
                entries.put(time, entry);
            }
            entry.count++;
            FieldsLookup doc = docReader.fields();
            List<Object> o = ((FieldLookup) doc.get(fieldName)).getValues();
            for (Object v : o) {
                entry.totalCount++;
                double value;
                if (v instanceof Integer) {
                    value = (Integer) v;
                } else if (v instanceof Long) {
                    value = (Long) v;
                } else if (v instanceof Double) {
                    value = (Double) v;
                } else {
                    value = (Float) v;
                }
                entry.total += value;
                if (value < entry.min) {
                    entry.min = value;
                }
                if (value > entry.max) {
                    entry.max = value;
                }
            }
        }
    }
}
