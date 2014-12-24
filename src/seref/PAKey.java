/**
 * 
 */
package seref;

import java.io.Serializable;

import com.hazelcast.core.PartitionAware;

/**
 * @author aeyate
 *
 */
public class PAKey implements Serializable, Comparable<PAKey>, PartitionAware<String> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer vertexId;
    String partitionId;

    public PAKey(Integer vertexId, String partitionId) {
        this.vertexId = vertexId;
        this.partitionId = partitionId;
    }

    public Integer getVertexId() {
        return vertexId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getPartitionKey() {
        return partitionId;
    }

    @Override
    public String toString() {
        return "PAKey{" + vertexId + ", " + partitionId + '}';
    }

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(PAKey o) {
		return vertexId.compareTo(o.getVertexId());
	}
	
	@Override
	public boolean equals(Object other) {
	    if (other == null) { return false; }
//	    if (!(other instanceof PAKey)) { return false; }
	    PAKey m = (PAKey)other;
	    if(this.vertexId.intValue()==m.vertexId.intValue() 
	    		&& this.partitionId.equals(m.partitionId)){
	    	return true;
	    }
	    return false;
	}
}