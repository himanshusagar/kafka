package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.MessageID;
import java.util.concurrent.ConcurrentHashMap;


public class OrderedMessageMap
{
    private ConcurrentHashMap<TopicPartition , ConcurrentHashMap<MessageID , MemoryRecords> > hMap;

    public void printf()
    {
        System.out.println("Printing ConcurrentHashMap<TopicPartition , ConcurrentHashMap<ProducerIdAndEpoch , MemoryRecords> > ");
        for(TopicPartition key : hMap.keySet())
        {
            System.out.println("Out = "  + key + " Begin" );
            ConcurrentHashMap<MessageID , MemoryRecords> inMap = hMap.get(key);

            for (MessageID key2 : inMap.keySet())
            {
                System.out.println("--- Key = " +  key2 + " Value = " + inMap.get(key2) );
            }
            System.out.println("Out = "  + key + " End" );

        }
    }
    public OrderedMessageMap()
    {
        hMap = new ConcurrentHashMap<>();
    }
    public boolean inMap(TopicPartition key)
    {
        return hMap.containsKey(key);
    }
    public MemoryRecords get(TopicPartition key, MessageID producerIdAndEpoch)
    {
        if( inMap(key, producerIdAndEpoch) )
        {
            return hMap.get(key).get(producerIdAndEpoch);
        }
        return MemoryRecords.EMPTY;
    }

    public ConcurrentHashMap<MessageID , MemoryRecords> get(TopicPartition key)
    {
        if( !hMap.containsKey(key) )
            hMap.put(key, new ConcurrentHashMap<>() );
        return hMap.get(key);
    }

    public void removeWithNoValidBytes(TopicPartition key) //hsagar : used with highwatermark
    {
        ConcurrentHashMap<MessageID , MemoryRecords> memMap = get(key);
        boolean retVal = memMap.entrySet().removeIf(e -> (e.getValue().validBytes == -2));
        //System.out.println("removeWithNoValidBytes = " + retVal);
    }

    public void put(TopicPartition key, MessageID producerIdAndEpoch , MemoryRecords message)
    {
        if( !inMap(key) )
            hMap.put(key, new ConcurrentHashMap<>() );
        ConcurrentHashMap<MessageID , MemoryRecords> internalMap = hMap.get(key);
        internalMap.put(producerIdAndEpoch , message);
        hMap.put(key , internalMap);
        //System.out.println("From Put:::::::::");
        //printf();
    }
    public boolean inMap(TopicPartition key, MessageID producerIdAndEpoch)
    {
        if( hMap.containsKey(key) )
            return hMap.get(key).containsKey(producerIdAndEpoch);
        return false;
    }
    public int length()
    {
        return hMap.size();
    }
}
