package rfx.core.stream.graph;

import rfx.core.util.StringUtil;

public class Edge {
	private final String id;
	private final Vertex source;
	private final Vertex destination;
	private final int weight;

	public Edge(String id, Vertex source, Vertex destination, int weight) {
		if(source == null){
			throw new IllegalArgumentException("source is NULL");
		}
		if(destination == null){
			throw new IllegalArgumentException("destination is NULL");
		}
		this.id = id;
		this.source = source;
		this.destination = destination;
		this.weight = weight;
	}

	public String getId() {
		return id;
	}

	public Vertex getDestination() {
		return destination;
	}

	public Vertex getSource() {
		return source;
	}

	public int getWeight() {
		return weight;
	}

	@Override
	public String toString() {
		return source + " -> " + destination;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Edge){
			Edge e = (Edge)obj;
			if(this.source.equals(e.getSource()) && this.destination.equals(e.getDestination())){
				return true;
			}
		}
		return false;
	}

	public String getIndexKey(){
		return buildIndexKey(source, destination);
	}
	
	public static String buildIndexKey(Vertex source, Vertex destination){
		return StringUtil.toString(source.getId(),"->",destination.getId());
	}
}
