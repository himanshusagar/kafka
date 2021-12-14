package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import java.util.HashMap;

public class OrderedMessageMap
{
    private HashMap<TopicPartition , HashMap<ProducerIdAndEpoch , MemoryRecords> > hMap;

    public OrderedMessageMap()
    {
        hMap = new HashMap<>();
    }
    public boolean inMap(TopicPartition key)
    {
        return hMap.containsKey(key);
    }
    public MemoryRecords get(TopicPartition key, ProducerIdAndEpoch producerIdAndEpoch)
    {
        if( inMap(key, producerIdAndEpoch) )
        {
            return hMap.get(key).get(producerIdAndEpoch);
        }
        return MemoryRecords.EMPTY;
    }
    public void put(TopicPartition key, ProducerIdAndEpoch producerIdAndEpoch , MemoryRecords message)
    {
        if( !inMap(key) )
            hMap.put(key, new HashMap<>() );
        hMap.get(key).put(producerIdAndEpoch , message);
    }
    public boolean inMap(TopicPartition key, ProducerIdAndEpoch producerIdAndEpoch)
    {
        if( hMap.containsKey(key) )
            return hMap.get(key).containsKey(producerIdAndEpoch);
        return false;
    }
}
