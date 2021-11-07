package org.graph.analysis.example.citibike;

import org.graph.analysis.GraphStream;
import org.graph.analysis.GraphStreamSource;
import org.graph.analysis.example.citibike.operator.CitiBikeDataToEdge;
import org.graph.analysis.network.Server;
import org.graph.analysis.operator.DataSink;
import org.graph.analysis.operator.Grouping;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.operator.SubGraph;

public class CitiBikeAnalysis {
    public static void main(String[] args) throws Exception {
        Server.initWebSocketServer();
        String groupId = "citibank";
        String topic = "bank";
        StreamToGraph<String> mapFunc = new CitiBikeDataToEdge();
        GraphStreamSource graphStreamSource = new GraphStreamSource();
        GraphStream citibankGraph = graphStreamSource.fromKafka(groupId, topic, mapFunc);
        citibankGraph
                .apply(new SubGraph())
                .apply(new Grouping())
                .apply(new DataSink());

        graphStreamSource.getEnvironment().execute("Citibank Data Streaming To Graph");
    }
}
