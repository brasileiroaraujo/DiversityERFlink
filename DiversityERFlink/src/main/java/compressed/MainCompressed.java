package compressed;

import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import Data.EntityProfile;
import Data.EntityProfileCompressed;
import parser.LoadDataSource;


public class MainCompressed {

	public static void main(String[] args) throws Exception {
		final int numberOfPairs = 2;
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
		DataSet<Tuple2<Integer, EntityProfileCompressed>> entities1 = ds1.flatMap(new ExtractTokenCompressed(1));
		
		//group entities from DS1 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfileCompressed>>> tokenized1 = entities1.groupBy(0).reduceGroup(new EntityListReduceCompressed()).rebalance();
		
		//job to extract tokens of DS2
		DataSet<Tuple2<Integer, EntityProfileCompressed>> entities2 = ds2.flatMap(new ExtractTokenCompressed(2));
		
		//group entities from DS2 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfileCompressed>>> tokenized2 = entities2.groupBy(0).reduceGroup(new EntityListReduceCompressed()).rebalance();
		
		//job to extract tokens of DS3
		DataSet<Tuple2<Integer, EntityProfileCompressed>> entities3 = ds3.flatMap(new ExtractTokenCompressed(3));
		
		//group entities from DS3 that share the same token
		DataSet<Tuple2<Integer, List<EntityProfileCompressed>>> tokenized3 = entities3.groupBy(0).reduceGroup(new EntityListReduceCompressed()).rebalance();
		
		//Join (lef join) the entities of DS1 and DS2 that share the same token (cross blocking)
		DataSet<Tuple2<Integer, List<EntityProfileCompressed>>> tokenBlocking12 = tokenized1.leftOuterJoin(tokenized2/*, JoinHint.REPARTITION_SORT_MERGE*/)
		// key of the first input
	    .where(0)
	    // key of the second input
	    .equalTo(0)
	    // applying the JoinFunction on joining pairs
	    .with(new TokenBlockingCompressed()).rebalance();

		
		//Join (lef join) the entities of DS1(after merge with DS1) and DS3 that share the same token (cross blocking)
		DataSet<Tuple2<Integer, List<EntityProfileCompressed>>> tokenBlocking13 = tokenBlocking12.leftOuterJoin(tokenized3/*, JoinHint.REPARTITION_SORT_MERGE*/)
		// key of the first input
	    .where(0)
	    // key of the second input
	    .equalTo(0)
	    // applying the JoinFunction on joining pairs
	    .with(new TokenBlockingCompressed()).rebalance();
		
		//Performing metablocking strategy. Key (integer) is the entity code (hash).
		DataSet<Tuple2<Integer, Tuple2<EntityProfileCompressed, EntityProfileCompressed>>> metablocking = tokenBlocking13.flatMap(new MetablockingStrategyCompressed()).rebalance();
		
		//Grouping per entity and performing the pruning
		DataSet<Tuple2<String, Set<EntityProfileCompressed>>> metablockingBlocks = metablocking.groupBy(0).reduceGroup(new EntityReduceCompressed(false, numberOfPairs)).rebalance();
		
		
		//Remove blocks with only one entity.
		DataSet<Tuple2<String, Set<EntityProfileCompressed>>> outputBlocks = metablockingBlocks.filter(new FilterBlocksCompressed());
		
		
		//store the output
		outputBlocks.writeAsText("outputs/test2");
		env.execute();
	
	}
	
}
