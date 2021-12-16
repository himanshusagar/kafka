package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.util.ArrayList;
import java.util.HashMap;

public class OrderedListMap
{
    private HashMap<TopicPartition , ArrayList<ProducerIdAndEpoch> > hMap;

    public OrderedListMap()
    {
        hMap = new HashMap<>();
    }
    public boolean inMap(TopicPartition key)
    {
        return hMap.containsKey(key);
    }
    public ArrayList<ProducerIdAndEpoch> get(TopicPartition key)
    {
        if(inMap(key))
            return hMap.get(key);
        return new ArrayList<>();
    }
    public void put(TopicPartition key, ProducerIdAndEpoch producerIdAndEpoch)
    {
        if( !inMap(key) )
            hMap.put(key, new ArrayList<>() );
        hMap.get(key).add(producerIdAndEpoch);
    }
}
