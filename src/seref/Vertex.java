package seref;
/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

public class Vertex implements Portable{


	private ArrayList<Integer> outNeighbors;
	private ArrayList<Long> dummyList;
	private int vertexID;
	private int partitionID;
	private double value;
    final static int ID = 1;
    
    
	/**
	 * @return the dummyList
	 */
	public ArrayList<Long> getDummyList() {
		return dummyList;
	}


	/**
	 * @param dummyList the dummyList to set
	 */
	public void setDummyList(ArrayList<Long> dummyList) {
		this.dummyList = dummyList;
	}


	public Vertex(){
		outNeighbors = new ArrayList<Integer>();
		dummyList = new ArrayList<Long>();
	}

	/**
	 * @return the outNeighbors
	 */
	public ArrayList<Integer> getOutNeighbors() {
		return outNeighbors;
	}


	/**
	 * @param outNeighbors the outNeighbors to set
	 */
	public void setOutNeighbors(ArrayList<Integer> outNeighbors) {
		this.outNeighbors = outNeighbors;
	}


	/**
	 * @return the id
	 */
	public int getVertexID() {
		return vertexID;
	}


	/**
	 * @param id the id to set
	 */
	public void setVertexID(int id) {
		this.vertexID = id;
	}


	/**
	 * @return the value
	 */
	public double getValue() {
		return value;
	}


	/**
	 * @param value the value to set
	 */
	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Vertex [id=" + vertexID + ", value=" + value
				+ ", neighborSize=" + outNeighbors.size() + "]";
	}
	
    public int getFactoryId() {
        return 1;
    }

    public int getClassId() {
        return 1;
    }


	/**
	 * @return the partitionID
	 */
	public int getPartitionID() {
		return partitionID;
	}


	/**
	 * @param partitionID the partitionID to set
	 */
	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	@Override
	public void writePortable(PortableWriter writer) throws
	IOException {
		writer.writeInt("id", vertexID);
		writer.writeInt("pid", partitionID);
		writer.writeDouble("value", value);
		int[] outNeighborsArray = new int[outNeighbors.size()];
		int i=0;
		for(int neigbor: outNeighbors){
			outNeighborsArray[i++]=neigbor;
		}
		writer.writeIntArray("neighbors", outNeighborsArray);

		long[] dummyListA = new long[dummyList.size()];
		i=0;
		for(Long neigbor: dummyList){
			dummyListA[i++]=neigbor;
		}
		writer.writeLongArray("dummyList", dummyListA);
	}


	/* (non-Javadoc)
	 * @see com.hazelcast.nio.serialization.Portable#readPortable(com.hazelcast.nio.serialization.PortableReader)
	 */
	@Override
	public void readPortable(PortableReader reader) throws IOException {
		vertexID = reader.readInt("id");
		partitionID = reader.readInt("pid");
		value = reader.readDouble("value");
		int[] outNeighborsArray = (int[]) reader.readIntArray("neighbors");
		for(int neigbor: outNeighborsArray){
			outNeighbors.add(new Integer(neigbor));
		}

		long[] dummyListA = (long[]) reader.readLongArray("dummyList");
		for(long neigbor: dummyListA){
			dummyList.add(new Long(neigbor));
		}
	}

}
