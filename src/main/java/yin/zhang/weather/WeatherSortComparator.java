package yin.zhang.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/***
 * 年月正序， 温度倒序
 */
public class WeatherSortComparator extends WritableComparator {
    WeatherBo bo1 = null;
    WeatherBo bo2 = null;

    public WeatherSortComparator() {
        super(WeatherBo.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        bo1 = (WeatherBo) a;
        bo2 = (WeatherBo) b;
        int year = Integer.compare(bo1.getYear(), bo2.getYear());
        if (year == 0) {
            int month = Integer.compare(bo1.getMonth(), bo2.getMonth());
            if (month == 0) {
                return -Integer.compare(bo1.getTemp(), bo2.getTemp());
            }
            return month;
        }
        return year;
    }
}
