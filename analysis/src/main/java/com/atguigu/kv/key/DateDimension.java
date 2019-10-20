package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDimension extends BaseDimension {

    private String year;
    private String month;
    private String day;

    public DateDimension() {
    }

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return year + "\t" + month + "\t" + day;
    }

    @Override
    public int compareTo(BaseDimension o) {

        DateDimension dateDimension = (DateDimension) o;

        int result;

        //按照年月日顺序比较
        result = this.year.compareTo(dateDimension.year);

        if (result == 0) {
            result = this.month.compareTo(dateDimension.month);
            if (result == 0) {
                result = this.day.compareTo(dateDimension.day);
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
    }
}
