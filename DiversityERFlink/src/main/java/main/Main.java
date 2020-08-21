package main;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import Data.EntityProfile;
import parser.LoadDataSource;


public class Main {

	public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		//load data sources (standalone)
		List<EntityProfile> source1 = LoadDataSource.loadData("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\warsampo\\municipalities_list");
		List<EntityProfile> source2 = LoadDataSource.loadData("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\geonames_list1K");
		List<EntityProfile> source3 = LoadDataSource.loadData("C:\\Users\\Brasileiro\\Bases_de_Dados\\LOD\\Geo\\wordnet31_list_locations");
		
		

		//load data source 1
		DataSet<EntityProfile> ds1 = env.fromCollection(source1);
		//load data source 2
		DataSet<EntityProfile> ds2 = env.fromCollection(source2);
		//load data source 3
		DataSet<EntityProfile> ds3 = env.fromCollection(source3);
		
		//job to extract tokens of DS1
		DataSet<Tuple2<Integer, EntityProfile>> entities1 = ds1.flatMap(new ExtractToken(1));
		
		//group entities from DS1 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenized1 = entities1.groupBy(0).reduceGroup(new EntityListReduce()).rebalance();
		
		//job to extract tokens of DS2
		DataSet<Tuple2<Integer, EntityProfile>> entities2 = ds2.flatMap(new ExtractToken(2));
		
		//group entities from DS2 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenized2 = entities2.groupBy(0).reduceGroup(new EntityListReduce()).rebalance();
		
		//job to extract tokens of DS3
		DataSet<Tuple2<Integer, EntityProfile>> entities3 = ds3.flatMap(new ExtractToken(3));
		
		//group entities from DS3 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenized3 = entities3.groupBy(0).reduceGroup(new EntityListReduce()).rebalance();
		
		//Join (lef join) the entities of DS1 and DS2 that share the same token (cross blocking)
		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenBlocking12 = tokenized1.leftOuterJoin(tokenized2/*, JoinHint.REPARTITION_SORT_MERGE*/)
		// key of the first input
	    .where(0)
	    // key of the second input
	    .equalTo(0)
	    // applying the JoinFunction on joining pairs
	    .with(new TokenBlocking()).rebalance();
		
		
//		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenBlocking12 = tokenized1.coGroup(tokenized2/*, JoinHint.REPARTITION_SORT_MERGE*/)
//		// key of the first input
//	    .where(0)
//	    // key of the second input
//	    .equalTo(0)
//	    .with(new CoGroupTokenBlocking()).rebalance();
		
		
		//Join (lef join) the entities of DS1(after merge with DS1) and DS3 that share the same token (cross blocking)
		DataSet<Tuple2<Integer, List<EntityProfile>>> tokenBlocking13 = tokenBlocking12.leftOuterJoin(tokenized3/*, JoinHint.REPARTITION_SORT_MERGE*/)
		// key of the first input
	    .where(0)
	    // key of the second input
	    .equalTo(0)
	    // applying the JoinFunction on joining pairs
	    .with(new TokenBlocking()).rebalance();
		
		//Performing metablocking strategy
		DataSet<Tuple2<Integer, EntityProfile>> metablocking = tokenBlocking13.flatMap(new MetablockingStrategy()).rebalance();
		
		//Grouping per entity and performing the pruning
		DataSet<Tuple2<Integer, EntityProfile>> metablockingBlocks = metablocking.groupBy(0).reduceGroup(new EntityReduce()).rebalance();
		
		
		//Remove blocks with only one entity.
		DataSet<Tuple2<Integer, EntityProfile>> outputBlocks = metablockingBlocks.filter(new FilterBlocks());
		
		
		//store the output
		outputBlocks.writeAsText("outputs/debug1");
		env.execute();
	
	}
	
}
