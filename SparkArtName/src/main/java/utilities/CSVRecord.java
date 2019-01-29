package utilities;

import java.io.Serializable;

/**
 * Created by giannis on 10/12/18.
 */
public class CSVRecord implements Serializable {

    long trajID;
    long timestamp;
    long roadSegment;

    public long getTrajID() {
        return trajID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getRoadSegment() {
        return roadSegment;
    }

    @Override
    public String toString() {
        return "CSVRecord{" +
                "trajID=" + trajID +
                ", timestamp=" + timestamp +
                ", roadSegment='" + roadSegment + '\'' +
                '}';
    }


    public CSVRecord(String trajectoryID, String timestampStr, String roadSegmentId) {

        trajID=Long.parseLong(trajectoryID.trim());
        timestamp=Long.parseLong(timestampStr.trim());
        roadSegment=Long.parseLong(roadSegmentId.trim());

    }

    public CSVRecord(String s){
        String[] tokens = s.split(",");
        trajID=Long.parseLong(tokens[0].trim());
        timestamp=Long.parseLong(tokens[1].trim());
        roadSegment=Long.parseLong(tokens[2].trim());


    }
}
