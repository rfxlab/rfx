package rfx.graphdb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import rfx.core.stream.graph.DijkstraAlgorithm;
import rfx.core.stream.graph.Edge;
import rfx.core.stream.graph.Graph;
import rfx.core.stream.graph.Vertex;
import rfx.core.stream.worker.StreamWorkerInfo;

public class TestDijkstraAlgorithm {
	private List<Vertex> nodes;
	private Map<String,Vertex> nodesIndex = new HashMap<String, Vertex>();
	private List<Edge> edges;
	

	private void addLane(String laneId, String sourceId, String destId,	int weight) {
		Edge lane = new Edge(laneId, nodesIndex.get(sourceId),nodesIndex.get(destId), weight);
		edges.add(lane);
	}
	
	@Before
	public void initData(){
		nodes = new ArrayList<Vertex>();
		edges = new ArrayList<Edge>();
		String kkTopic = "true-imp";
		String host = "127.0.0.1";
		
		for (int i = 0; i < 5; i++) {
			int port = 15001 + i;			
			int beginPartitionId = i;
			int endPartitionId = i;
			Vertex location = StreamWorkerInfo.create(host, port , kkTopic, beginPartitionId, endPartitionId);
			nodesIndex.put(location.getId(), location);
			System.out.println("indexed VertexId "+location.getId());
			nodes.add(location);
		}

		addLane("15001_15002", StreamWorkerInfo.getId(host, 15001, kkTopic, 0, 0), StreamWorkerInfo.getId(host, 15002, kkTopic, 1, 1), 1);
		addLane("15001_15003", StreamWorkerInfo.getId(host, 15001, kkTopic, 0, 0), StreamWorkerInfo.getId(host, 15003, kkTopic, 2, 2), 2);
		addLane("15001_15004", StreamWorkerInfo.getId(host, 15001, kkTopic, 0, 0), StreamWorkerInfo.getId(host, 15004, kkTopic, 3, 3), 30);
		addLane("15001_15005", StreamWorkerInfo.getId(host, 15001, kkTopic, 0, 0), StreamWorkerInfo.getId(host, 15005, kkTopic, 4, 4), 20);
		
		addLane("15002_15003", StreamWorkerInfo.getId(host, 15002, kkTopic, 1, 1), StreamWorkerInfo.getId(host, 15003, kkTopic, 2, 2), 4);
		addLane("15002_15004", StreamWorkerInfo.getId(host, 15002, kkTopic, 1, 1), StreamWorkerInfo.getId(host, 15004, kkTopic, 3, 3), 10);
		
		addLane("15003_15004", StreamWorkerInfo.getId(host, 15003, kkTopic, 2, 2), StreamWorkerInfo.getId(host, 15004, kkTopic, 3, 3), 2);
		addLane("15003_15005", StreamWorkerInfo.getId(host, 15003, kkTopic, 2, 2), StreamWorkerInfo.getId(host, 15005, kkTopic, 4, 4), 1);
		
		addLane("15004_15005", StreamWorkerInfo.getId(host, 15004, kkTopic, 3, 3), StreamWorkerInfo.getId(host, 15005, kkTopic, 4, 4), 5);
	}
	
	

	@Test
	public void testExcute() {	
		// Lets check from (w:127.0.0.1:15001:true-imp:0:0) to (w:127.0.0.1:15005:true-imp:4:4)
		Vertex source = nodes.get(0);
		Vertex target = nodes.get(4);
		
		Graph graph = new Graph(nodes, edges);
		DijkstraAlgorithm dijkstra = new DijkstraAlgorithm(graph);
		dijkstra.execute(source);
		List<Vertex> path = dijkstra.getPath(target);

		assertNotNull(path);
		assertTrue(path.size() > 0);

		System.out.println("\n Shortest path of "+source + " -> " + target);
		for (Vertex vertex : path) {
			System.out.println(vertex);
		}
		List<Edge> paths = dijkstra.getShortestPath(source, target);
		for (Edge edge : paths) {
			System.out.println(edge.getId() + " w " + edge.getWeight());
		}
		
		assertTrue(paths.get(0).getId().equals("15001_15003"));
		assertTrue(paths.get(1).getId().equals("15003_15005"));		
	}

}
