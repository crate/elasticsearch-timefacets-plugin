package com.lovelysystems.facet.latest;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData.LongValueInDocProc;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

public abstract class LatestFacetCollector extends AbstractFacetCollector {

    static ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>>() {
        @Override
        protected ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>(
                    new ArrayDeque<TObjectIntHashMap<String>>());
        }
    };

    public static final FieldDataType keyDataType = FieldDataType.DefaultTypes.LONG;
    public static final FieldDataType tsDataType = FieldDataType.DefaultTypes.LONG;

    protected LongFieldData keyFieldData;

    protected final int limit = 10;
    protected int size = 10;
    protected int start = 0;

    protected final Set<String> names = new HashSet<String>();


    public LatestFacetCollector(String facetName) {
        super(facetName);
    }
}
