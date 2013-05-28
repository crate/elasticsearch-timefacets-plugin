package crate.elasticsearch.plugin.facet;

import crate.elasticsearch.facet.distinct.DistinctDateHistogramFacetParser;
import crate.elasticsearch.facet.latest.LatestFacetParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;


public class FacetPlugin extends AbstractPlugin {

    public FacetPlugin(Settings settings) {
    }

    @Override
    public String name() {
        return "time-facets";
    }

    @Override
    public String description() {
        return "Time-Facets Plugins";
    }

    public void onModule(FacetModule module) {
        module.addFacetProcessor(DistinctDateHistogramFacetParser.class);
        module.addFacetProcessor(LatestFacetParser.class);
    }
}
