package trie;

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import utilities.CSVRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StreamingQuery implements Serializable {

    long trajID;
    long timestamp;
    long roadSegment;
    private Long2LongAVLTreeMap timeToRS = new Long2LongAVLTreeMap();

    public void setTrajID(long trajID) {
        this.trajID = trajID;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setRoadSegment(long roadSegment) {
        this.roadSegment = roadSegment;
    }




    public long getTrajID() {
        return trajID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getRoadSegment() {
        return roadSegment;
    }





    public StreamingQuery(CSVRecord record){
        trajID=record.getTrajID();
        timestamp=record.getTimestamp();
        roadSegment=record.getRoadSegment();

    }

    public void addSQ(StreamingQuery streamingQuery) {

        timeToRS.put(streamingQuery.getTimestamp(),streamingQuery.getRoadSegment());

    }
}
