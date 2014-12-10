package sample.rfx.query;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import rfx.core.util.Utils;
import rfx.query.ActorData;
import rfx.query.FunctionFactory;
import rfx.query.Functor;
import rfx.query.FunctorGroupResultByKey;
import rfx.query.PredicateFactory;
import rfx.query.Query;
import rfx.query.QueryContext;
import rfx.query.QueryResult;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;

public class ModelingUserWithActor {
	static int MAX_POOL_SIZE = 1000000;
	final static String SAMPLE_DATA_PATH = "/home/trieu/data/user-income.txt";

	static AtomicInteger idCount = new AtomicInteger();
		
	public static class Person extends ActorData {		
		
		private static final long serialVersionUID = -8917032934585345178L;
		int id;
		String education;
		String occupation;
		boolean incomeLess50k = true;
		boolean queried = false;
		String rawData;
		
		public Person(int id, String education, String occupation, boolean incomeLess50k) {
			super(id);
			this.id = id;
			this.education = education;
			this.occupation = occupation;
			this.incomeLess50k = incomeLess50k;
		}

		public String getRawData() {
			return rawData;
		}
		public void setRawData(String rawData) {
			this.rawData = rawData;
		}
		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getEducation() {
			return education;
		}

		public void setEducation(String education) {
			this.education = education;
		}

		public boolean isIncomeLess50k() {
			return incomeLess50k;
		}

		public void setIncomeLess50k(boolean incomeLess50k) {
			this.incomeLess50k = incomeLess50k;
		}
		
		public String getOccupation() {
			return occupation;
		}

		public void setOccupation(String occupation) {
			this.occupation = occupation;
		}

		public boolean isQueried() {
			return queried;
		}

		public void setQueried(boolean queried) {
			this.queried = queried;
		}

		@Override
		public String toString() {
			return new Gson().toJson(this);
		}
	}
	
	public static class PersonActor extends UntypedActor {
		Person person;		
		public PersonActor(Person person) {
			super();
			this.person = person; 
		}
		
		public void onReceive(Object message) throws Exception {
			//System.out.println("PersonActor got message "+message);	
			if(sender() == null || message == null){
				return;
			}
			if(message instanceof Predicate){				
				@SuppressWarnings("unchecked")
				Predicate<Person> predicate = (Predicate<Person>)message;
				if(predicate.test(person)){
					sender().tell(person, self());
				} else {
					sender().tell(person.getActorId(), self());
				}
			}
		}
	}	
	
	static Map<Integer,ActorRef> buildDataActorsFromRawData(ActorSystem system) throws IOException{	
		Map<Integer,ActorRef> dataActors = new ConcurrentHashMap<>(MAX_POOL_SIZE);
		Stream<String> lines = Files.lines(Paths.get(SAMPLE_DATA_PATH));		
		lines.forEach((String row)->{			
			String[] toks = row.split(",");
			if(toks.length == 15){
				int id = idCount.incrementAndGet();
				String education = toks[3].trim();
				String income = toks[14].trim();
				String occupation = toks[6].trim();				
				Person person = new Person(id, education, occupation, income.equals("<=50K"));
				person.setRawData(row);
				
				ActorRef actorRef = system.actorOf(Props.create(PersonActor.class,person));
				dataActors.put(id, actorRef);
				//personMap.put(id, person);
			} else {
				System.out.println(" bad data row "+row);
			}
		});		
		lines.close();
		return dataActors;
	}
	
	static void printResults(Query<?> query, QueryResult queryResult){
		AtomicInteger count = new AtomicInteger(0);
		int totalPersons = idCount.get();
		queryResult.getReducedResults().forEach((String edu, List<Object> groupedPersons)->{
			if(groupedPersons != null){
				int groupSize = groupedPersons.size();
				double percent = (double)(groupSize * 100) / totalPersons;
				percent = Math.round(percent * 100.0) / 100.0;
				System.out.println("["+edu + "] groupSize = " + groupSize + " takes " + percent +"% in total");
				count.addAndGet(groupSize);			
				System.out.println("------------------------------");
			}
		});		
		int totalResult = count.get();
		double percent = (double)( totalResult * 100) / totalPersons;
		percent = Math.round(percent * 100.0) / 100.0;
		
		System.out.println("data path: " + SAMPLE_DATA_PATH);
		System.out.println(query.getDescription());
		System.out.println("Total persons in result = " + totalResult + ", takes " + percent +"% in total");
		System.out.println("Total persons = " + totalPersons);	
	}		
		
	static class PredicateIncomeLargerThan50k implements PredicateFactory {
		private static final long serialVersionUID = -7011351296196435505L;
		public Predicate<Person> build(){
			return new Predicate<Person>() {			
				@Override
				public boolean test(Person person) {				
					return ! person.isIncomeLess50k() ;
				}
			};
		}
	}
	
	static class FunctionGroupByEducation implements FunctionFactory {
		private static final long serialVersionUID = -872087603235171330L;

		@Override
		public Function<ActorData, String> build() {
			return new Function<ActorData, String>() {				
				@Override
				public String apply(ActorData actorData) {
					Person person = (Person)actorData;
					String groupKey = person.getEducation();
					return groupKey;
				}
			};
		}		
	}

	public static void main(String[] args) {
		ActorSystem actorSystem = ActorSystem.create("MySystem");
		Map<Integer,ActorRef> dataActors = null;
		try {
			dataActors = buildDataActorsFromRawData(actorSystem);
			Utils.sleep(2000);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		Stopwatch stopwatch = Stopwatch.createStarted();
		//-------------------------------------------------------//
	
		FunctionGroupByEducation functionFactory = new FunctionGroupByEducation();
		PredicateIncomeLargerThan50k predicateFactory = new PredicateIncomeLargerThan50k();		
		
		Class<? extends Functor> functorClass = FunctorGroupResultByKey.class;		
		QueryResult queryResult = new QueryResult(functionFactory);
		int queryActorPoolSize = 500;
		QueryContext queryContext = new QueryContext(actorSystem, queryResult, dataActors, functorClass, queryActorPoolSize );
		
		String des2 = "--- Counting and grouping by [Education], where income >50K and Never-married ---";
		Query<Person> query = new Query<Person>(queryContext, des2, predicateFactory);		
		query.execute();
		
		System.out.println("getQueryMessageSize " + queryContext.getQueryMessageSize());
		while( ! queryContext.isQueryDone() ){
			Utils.sleep(5);
		}
		System.out.println("getQueryMessageSize " + queryContext.getQueryMessageSize());
		
		//-------------------------------------------------------//
		stopwatch.stop();
		long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS)-5;		 
		
		printResults(query, queryResult);
				
		System.out.println("Query time: " + millis);
		actorSystem.shutdown();
	}
}