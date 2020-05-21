package proj.sbe;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class App {
    private static final String SPOUT_ID = "publisher_spout";
    private static final String BOLT_ID = "bolt_1";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new PublisherSpout(), 1);
        builder.setBolt(BOLT_ID, new Bolt1(), 1).shuffleGrouping(SPOUT_ID);

        // fine tuning
        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();

        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);
        config.setDebug(true);

        cluster.submitTopology("stocks_topology", config, topology);

        Utils.sleep(10000);
        cluster.killTopology("stocks_topology");
        cluster.shutdown();
    }
}
