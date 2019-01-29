package utilities;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by giannis on 29/01/19.
 */
public class ResultPlaceHolder implements Serializable {

    public Set<Long> getResultSet() {
        return resultSet;
    }

    public long getQueryID() {
        return queryID;
    }

    public void setResultSet(Set<Long> resultSet) {
        this.resultSet = resultSet;
    }

    public void setQueryID(long queryID) {
        this.queryID = queryID;
    }

    Set<Long> resultSet;
    long queryID;



}
