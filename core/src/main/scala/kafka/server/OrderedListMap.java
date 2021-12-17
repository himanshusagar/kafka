package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class OrderedListMap
{
    private HashMap<TopicPartition , List<ProducerIdAndEpoch> > hMap;

    public OrderedListMap()
    {
        hMap = new HashMap<>();
    }
    public boolean inMap(TopicPartition key)
    {
        return hMap.containsKey(key);
    }

    public int length()
    {
        return hMap.size();
    }
    public List<FetchResponseData.ProducerIDEpoch> getProducerIDEpoch(TopicPartition key)
    {
        List<FetchResponseData.ProducerIDEpoch> output = new ArrayList<>();
        List<ProducerIdAndEpoch> tmp = get(key);
        for(ProducerIdAndEpoch pIE : tmp)
        {
            FetchResponseData.ProducerIDEpoch obj = new FetchResponseData.ProducerIDEpoch();
            obj.setProducerEpoch(pIE.epoch);
            obj.setProducerID(pIE.producerId);
            output.add( obj );
        }
        return output;
    }

    public void put(TopicPartition key, ProducerIdAndEpoch producerIdAndEpoch)
    {
        if( !inMap(key) )
            hMap.put(key, new ArrayList<>());
        for (ProducerIdAndEpoch idAndEpoch : hMap.get(key))
        {
            if(idAndEpoch.equals(producerIdAndEpoch))
                return;
        }
        hMap.get(key).add(producerIdAndEpoch);
    }
    public List<ProducerIdAndEpoch> get(TopicPartition key)
    {
        if(inMap(key))
            return hMap.get(key);
        return new ArrayList<>();
    }
}
