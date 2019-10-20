package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommDimension extends BaseDimension {

    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    public CommDimension() {
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public String toString() {
        return contactDimension + "\t" + dateDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {

        CommDimension commDimension = (CommDimension) o;

        int result;

        result = this.contactDimension.compareTo(commDimension.contactDimension);
        if (result == 0) {
            result = this.dateDimension.compareTo(commDimension.dateDimension);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        this.contactDimension.write(out);
        this.dateDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.contactDimension.readFields(in);
        this.dateDimension.readFields(in);
    }
}
