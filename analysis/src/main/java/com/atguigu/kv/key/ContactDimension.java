package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ContactDimension extends BaseDimension {

    private String name;
    private String phoneNum;

    public ContactDimension() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public String toString() {
        return name + "\t" + phoneNum;
    }

    @Override
    public int compareTo(BaseDimension o) {

        ContactDimension contactDimension = (ContactDimension) o;

        return this.phoneNum.compareTo(contactDimension.phoneNum);

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.name);
        out.writeUTF(this.phoneNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.name = in.readUTF();
        this.phoneNum = in.readUTF();
    }
}


支持强移植性
阿里巴巴 淘宝使用的数据库为Percona
AliSql+AliRedis
xtradb引擎
MyISAM 和InnoDB
