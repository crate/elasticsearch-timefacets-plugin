package com.lovelysystems.facet.latest;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.terms.InternalTermsFacet;
import org.elasticsearch.search.internal.SearchContext;

public class LatestFacetProcessor extends AbstractComponent implements
        FacetProcessor {

    @Inject
    public LatestFacetProcessor(Settings settings) {
        super(settings);
        InternalTermsFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[] { InternalLatestFacet.TYPE, LongInternalLatestFacet.TYPE };
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser,
            SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String tsField = null;
        int size = 10;
        int start = 0;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
            } else if (token.isValue()) {
                if ("key_field".equals(currentFieldName)) {
                    keyField = parser.text();
                } else if ("value_field".equals(currentFieldName)) {
                    valueField = parser.text();
                } else if ("ts_field".equals(currentFieldName)) {
                    tsField = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("start".equals(currentFieldName)) {
                    start = parser.intValue();
                }
            }
        }
        FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(
                keyField);
        if ((fieldMapper != null)
                && (fieldMapper.fieldDataType() != LatestFacetCollector.keyDataType)) {
            throw new FacetPhaseExecutionException(facetName,
                    "key field must be of type long but is "
                            + fieldMapper.fieldDataType());
        }

        fieldMapper = context.mapperService().smartNameFieldMapper(tsField);
        if ((fieldMapper != null)
                && (fieldMapper.fieldDataType() != LatestFacetCollector.tsDataType)) {
            throw new FacetPhaseExecutionException(facetName,
                    "ts field must be of type long but is "
                            + fieldMapper.fieldDataType());
        }

        fieldMapper = context.mapperService().smartNameFieldMapper(valueField);
        if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.INT) {
            return new IntLatestFacetCollector(facetName, keyField, valueField, tsField, size, start, context);
        } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.LONG) {
            return new LongLatestFacetCollector(facetName, keyField, valueField, tsField, size, start, context);
        } else {
            throw new FacetPhaseExecutionException(facetName, "value field  is not of type int or long");
        }


    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        InternalLatestFacet first = (InternalLatestFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}
