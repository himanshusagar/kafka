package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.MessageID;
import java.util.HashMap;

public class OrderedMessageMap
{
    private HashMap<TopicPartition , HashMap<MessageID , MemoryRecords> > hMap;

    public void printf()
    {
        System.out.println("Printing HashMap<TopicPartition , HashMap<ProducerIdAndEpoch , MemoryRecords> > ");
        for(TopicPartition key : hMap.keySet())
        {
            System.out.println("Out = "  + key + " Begin" );
            HashMap<MessageID , MemoryRecords> inMap = hMap.get(key);

            for (MessageID key2 : inMap.keySet())
            {
                System.out.println("--- Key = " +  key2 + " Value = " + inMap.get(key2) );
            }
            System.out.println("Out = "  + key + " End" );

        }
    }
    public OrderedMessageMap()
    {
        hMap = new HashMap<>();
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

    public HashMap<MessageID , MemoryRecords> get(TopicPartition key)
    {
        if( !hMap.containsKey(key) )
            hMap.put(key, new HashMap<>() );
        return hMap.get(key);
    }

    public void put(TopicPartition key, MessageID producerIdAndEpoch , MemoryRecords message)
    {
        if( !inMap(key) )
            hMap.put(key, new HashMap<>() );
        HashMap<MessageID , MemoryRecords> internalMap = hMap.get(key);
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
