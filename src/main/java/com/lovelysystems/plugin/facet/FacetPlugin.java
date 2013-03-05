package com.lovelysystems.plugin.facet;

import com.lovelysystems.facet.distinct.DistinctDateHistogramFacetProcessor;
import com.lovelysystems.facet.distinct.InternalDistinctDateHistogramFacet;
import com.lovelysystems.facet.latest.InternalLatestFacet;
import com.lovelysystems.facet.latest.LatestFacetProcessor;
import com.lovelysystems.facet.uncached.datehistogram.DateHistogramFacetProcessor;
import com.lovelysystems.facet.uncached.datehistogram.InternalFullDateHistogramFacet;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

/**
 * @author jukart
 */
public class FacetPlugin extends AbstractPlugin {

    public FacetPlugin(Settings settings) {
    }

    @Override
    public String name() {
        return "ls-tools";
    }

    @Override
    public String description() {
        return "Lovely Systems Plugins";
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof FacetModule) {
            ((FacetModule) module).addFacetProcessor(DateHistogramFacetProcessor.class);
            InternalFullDateHistogramFacet.registerStreams();
            ((FacetModule) module).addFacetProcessor(DistinctDateHistogramFacetProcessor.class);
            InternalDistinctDateHistogramFacet.registerStreams();
            ((FacetModule) module).addFacetProcessor(LatestFacetProcessor.class);
            InternalLatestFacet.registerStreams();

        }
    }
}
