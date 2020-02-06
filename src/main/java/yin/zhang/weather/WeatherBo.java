package yin.zhang.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherBo implements WritableComparable<WeatherBo> {

    private int year;
    private int month;
    private int day;
    private int temp;   // 温度

    public int compareTo(WeatherBo o) {
        int year = Integer.compare(this.getYear(), o.getYear());
        if ( year == 0) {
            int month = Integer.compare(this.getMonth(), o.getMonth());
            if(month == 0) {
                return Integer.compare(this.getDay(), o.getDay());
            }
            return month;
        }
        return year;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getYear());
        dataOutput.writeInt(this.getMonth());
        dataOutput.writeInt(this.getDay());
        dataOutput.writeInt(this.getTemp());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.setYear(dataInput.readInt());
        this.setMonth(dataInput.readInt());
        this.setDay(dataInput.readInt());
        this.setTemp(dataInput.readInt());
    }

    public int getDay() {
        return day;
    }

    public int getTemp() {
        return temp;
    }

    public int getMonth() {
        return month;
    }

    public int getYear() {
        return year;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return year + "-" + month + "-" + day;
    }
}
