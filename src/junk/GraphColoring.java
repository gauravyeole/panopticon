package junk;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;



public class GraphColoring{
	
    public static void main(String[] args) {
    	initCluster(10);
    	
    }

	/**
	 * @param i
	 */
	private static void initCluster(int instanceNum) {
		Config cfg = new Config();
		HazelcastInstance instance[] = new HazelcastInstance[instanceNum];
		for(int i=0; i<instanceNum; i++){
	        instance[i] = Hazelcast.newHazelcastInstance(cfg);
		}
	}

	
	
}