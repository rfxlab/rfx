package rfx.graphdb;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.SequentialUUIDHandleFactory;

public class TestSimpleGraphDB {

	public static class Name {
		private String surname;

		public String getSurname() {
			return surname;
		}

		public void setSurname(String surname) {
			this.surname = surname;
		}

		private String firstName;

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}
		
		private int age;
		

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public Name() {
		};
	}
	
	public static class Node implements Comparable<Node>{
		String name;
		int weight;
		public Node() {
		}
		
		public Node(String name) {
			super();
			this.name = name;
		}

		@Override
		public int compareTo(Node o) {
			if(o.weight > this.weight){
				return 1;
			} else if(o.weight < this.weight){
				return -1;
			}
			return 0;
		}
	}

	public static void main(String[] args) {
		try {
			
			HGConfiguration config = new HGConfiguration();
			config.setTransactional(false);
			config.setSkipOpenedEvent(true);
			String location = "data/hpg.data";
			SequentialUUIDHandleFactory handleFactory =
                    new SequentialUUIDHandleFactory(System.currentTimeMillis(), 0);
			config.setHandleFactory(handleFactory);
			HyperGraph graph = HGEnvironment.get(location, config);
			hg.addUnique(graph, new Node("master"), hg.and(hg.type(Node.class), hg.gt("name","master")));
						
					
			List<Name> hugos = hg.getAll(graph, hg.and(hg.type(Name.class), hg.gt("age",25) , hg.or( hg.eq("surname", "Hugo"), hg.eq("firstName", "Hugo") )));
	        List<Name> noHugos = hg.getAll(graph, hg.and(hg.type(Name.class), hg.not(hg.or(hg.eq("surname", "Hugo"), hg.eq("firstName", "Hugo")))));

	        for(Name n : hugos)
	                System.out.println("hugo: surname:" + n.getSurname() + " . first name: " + n.getFirstName());

	        for(Name n : noHugos)
	            System.out.println("Not a Hugo: surname:" + n.getSurname() + " . first name: " + n.getFirstName());

			graph.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
