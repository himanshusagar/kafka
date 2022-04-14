package kafka.server;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.utils.MessageID;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class OrderedListMap
{
    private HashMap<TopicPartition , List<MessageID> > hMap;

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
    public List<FetchResponseData.FollowerSyncData> getProducerIDEpoch(TopicPartition key, int offset)
    {

        List<FetchResponseData.FollowerSyncData> output = new ArrayList<>();
        List<MessageID> tmp = get(key);
        for( int i = offset; i< tmp.size(); i++)
        {
            MessageID pIE = tmp.get(i);
            FetchResponseData.FollowerSyncData obj = new FetchResponseData.FollowerSyncData();
            obj.setProducerEpoch(pIE.producerEpoch);
            obj.setProducerID(pIE.producerId);
            obj.setSequenceBegin(pIE.sequenceBegin);
            obj.setSequenceEnd(pIE.sequenceEnd);
            output.add( obj );
        }
        return output;
    }

    public void put(TopicPartition key, MessageID producerIdAndEpoch)
    {
        if( !inMap(key) )
            hMap.put(key, new ArrayList<>());
//        for (ProducerIdAndEpoch idAndEpoch : hMap.get(key))
//        {
//            if(idAndEpoch.equals(producerIdAndEpoch))
//                return;
//        }
        hMap.get(key).add(producerIdAndEpoch);
    }
    public List<MessageID> get(TopicPartition key)
    {
        if(inMap(key))
            return hMap.get(key);
        return new ArrayList<>();
    }

    public int size(TopicPartition key)
    {
        if(inMap(key))
            return hMap.get(key).size();
        return 0;
    }
}
