package com.lovelysystems.facet.test;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.node.NodeBuilder.*;

public abstract class AbstractNodes {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    private Settings defaultSettings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName())
            .build();

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }

    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings.Builder settings) {
        return startNode(id, settings.build());
    }

    public Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings.Builder settings) {
        return buildNode(id, settings.build());
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }

        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Node node(String id) {
        return nodes.get(id);
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }

    public void setupTemplates(Client client) throws Exception {
        String settings = XContentFactory.jsonBuilder()
                .startObject()
                .field("number_of_shards", 2)
                .field("number_of_replicas", 0)
                .startArray("aliases").value("data").endArray()
                .endObject().string();
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("data")
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_source").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("created_at").field("type", "date").field("store", "yes").endObject()
                .startObject("total").field("type", "integer").field("store", "yes").endObject()
                .startObject("floattotal").field("type", "float").field("store", "yes").endObject()
                .startObject("more").field("type", "long").field("store", "yes").endObject()
                .startObject("notstored").field("type", "long").field("store", "no").endObject()
                .endObject()
                .endObject()
                .endObject().string();

        client.admin().indices().preparePutTemplate("data")
                .setTemplate("data_*")
                .setSettings(settings)
                .addMapping("data", mapping)
                .execute().actionGet();

        Thread.sleep(100); // sleep a bit here..., so the mappings get applied
    }
}