package compressed;

import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import Data.EntityProfile;
import Data.EntityProfileCompressed;

public class ExtractTokenCompressed implements FlatMapFunction<EntityProfile, Tuple2<Integer, EntityProfileCompressed>> {
	
	private int sourceID;

	public ExtractTokenCompressed(int sourceID) {
		this.sourceID = sourceID;
	}

	@Override
	public void flatMap(EntityProfile e, Collector<Tuple2<Integer, EntityProfileCompressed>> out) throws Exception {
		e.setSource(sourceID);
		EntityProfileCompressed entityCompressed = new EntityProfileCompressed(e);
		
		for (Integer key : entityCompressed.getTokens()) {
			out.collect(new Tuple2<Integer, EntityProfileCompressed>(key, entityCompressed));
		}
		
	}

}
