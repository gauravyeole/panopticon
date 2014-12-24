package junk;
import seref.Vertex;

import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.serialization.ByteArraySerializer;;

public class VertexKryoSerializer implements ByteArraySerializer<Vertex> {
    static final int BUFFER_SIZE = 64 * 1024;
	Kryo kryo = new Kryo();

	public VertexKryoSerializer() {
		kryo.register(Vertex.class);
	}

	public int getTypeId() {
		return 2;
	}

    public byte[] write(Vertex o) throws IOException {
        Output output = new Output(new byte[BUFFER_SIZE]);
        kryo.writeClassAndObject(output, o);
        output.flush();
        return output.toBytes();
    }

    public Vertex read(byte[] data) throws IOException {
        Input input = new Input(data);
        return (Vertex) kryo.readClassAndObject(input);
    }

	public void destroy() {
	}
}