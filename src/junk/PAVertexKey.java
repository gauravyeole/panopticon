package junk;
import java.io.Serializable;

import com.hazelcast.core.PartitionAware;

public class PAVertexKey implements Serializable, PartitionAware {
    int customerId;
    int orderId;

    public PAVertexKey(int orderId, int customerId) {
        this.customerId = customerId;
        this.orderId = orderId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public int getOrderId() {
        return orderId;
    }

    public Object getPartitionKey() {
        return customerId;
    }

    @Override
    public String toString() {
        return "OrderKey{" +
                "customerId=" + customerId +
                ", orderId=" + orderId +
                '}';
    }
}