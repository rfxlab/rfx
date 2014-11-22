/**
 * 
 */
package rfx.core.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * @author duhc
 *
 */
public class WorkerTimeLog {

    private List<Long> upTimes;
    private List<Long> downTimes;
    
    /**
     * Default constructor.
     */
    public WorkerTimeLog() {
        super();
        this.upTimes = new ArrayList<Long>();
        this.downTimes = new ArrayList<Long>();
    }

    /**
     * @param upTimes
     * @param downTimes
     */
    public WorkerTimeLog(List<Long> upTimes, List<Long> downTimes) {
        super();
        this.upTimes = upTimes;
        this.downTimes = downTimes;
    }

    /**
     * @return the upTimes
     */
    public List<Long> getUpTimes() {
        return upTimes;
    }
    
    /**
     * @param upTimes the upTimes to set
     */
    public void setUpTimes(List<Long> upTimes) {
        this.upTimes = upTimes;
    }
    
    /**
     * @author duhc
     *
     * @return up times in Date type.
     */
    public List<Date> getUpTimesInDateType() {
        List<Date> result = new ArrayList<Date>(upTimes.size());
        for (Long upTime : upTimes) {
            result.add(new Date(upTime));
        }
        return result;
    }
    
    /**
     * @author duhc
     *
     * @return last up time.
     */
    public Long getLastUpTime() {
        if (upTimes.isEmpty()) {
            return 0L;
        }
        return upTimes.get(upTimes.size()-1);
    }
    
    /**
     * @author duhc
     *
     * @return last up time in Date type.
     */
    public Date getLastUpTimeInDate() {
        if (upTimes.isEmpty()) {
            return null;
        }
        return new Date(upTimes.get(upTimes.size()-1));
    }
    
    /**
     * Add up time to time log.
     * @author duhc
     *
     * @param upTime up Time
     */
    public void addUpTime(Date upTime) {
        upTimes.add(upTime.getTime());
    }

    /**
     * Add up time to time log.
     * @author duhc
     *
     * @param upTime up Time
     */
    public void addUpTime(long upTime) {
        upTimes.add(upTime);
    }
    
    /**
     * @return the downTimes
     */
    public List<Long> getDownTimes() {
        return downTimes;
    }
    
    /**
     * @param downTimes the downTimes to set
     */
    public void setDownTimes(List<Long> downTimes) {
        this.downTimes = downTimes;
    }
    
    /**
     * @author duhc
     *
     * @return down times in Date type.
     */
    public List<Date> getDownTimesInDateType() {
        List<Date> result = new ArrayList<Date>(downTimes.size());
        for (Long downTime : downTimes) {
            result.add(new Date(downTime));
        }
        return result;
    }
    
    /**
     * @author duhc
     *
     * @return last down time.
     */
    public Long getLastDownTime() {
        if (downTimes.isEmpty()) {
            return 0L;
        }
        return downTimes.get(downTimes.size()-1);
    }
    
    /**
     * @author duhc
     *
     * @return last down time in Date type.
     */
    public Date getLastDownTimeInDate() {
        if (downTimes.isEmpty()) {
            return null;
        }
        return new Date(downTimes.get(downTimes.size()-1));
    }

    /**
     * Add down time to time log.
     * @author duhc
     *
     * @param downTime down Time
     */
    public void addDownTime(Date downTime) {
        downTimes.add(downTime.getTime());
    }

    /**
     * Add down time to time log.
     * @author duhc
     *
     * @param downTime down Time
     */
    public void addDownTime(long downTime) {
        downTimes.add(downTime);
    }
}
