package com.lovelysystems.plugin.facet;

import com.lovelysystems.facet.distinct.DistinctDateHistogramFacetParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;


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

    public void onModule(FacetModule module) {
        module.addFacetProcessor(DistinctDateHistogramFacetParser.class);
    }

    //@Override
    //public void processModule(Module module) {
    //    if (module instanceof FacetModule) {
    //        ((FacetModule) module).addFacetProcessor(DateHistogramFacetProcessor.class);
    //        InternalFullDateHistogramFacet.registerStreams();
    //        ((FacetModule) module).addFacetProcessor(DistinctDateHistogramFacetParser.class);
    //        InternalDistinctDateHistogramFacet.registerStreams();
    //        ((FacetModule) module).addFacetProcessor(LatestFacetProcessor.class);
    //        InternalLatestFacet.registerStreams();
    //    }
    //}
}
