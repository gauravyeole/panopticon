package seref;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class PortableVertexFactory implements PortableFactory {

    @Override
    public Portable create(int classId) {
        if (Vertex.ID == classId)
            return new Vertex();
        else return null;
     }
}