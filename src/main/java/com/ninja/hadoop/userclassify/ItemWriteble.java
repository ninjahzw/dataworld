package com.ninja.hadoop.userclassify;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 10/22/13
 * Time: 12:41 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ItemWriteble implements Writable, Cloneable  {

    private String cookies = "";
    private String tuid = "";

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getCookies() {
        return cookies;
    }

    public void setCookies(String cookies) {
        this.cookies = cookies;
    }

    public String getTuid() {
        return tuid;
    }

    public void setTuid(String tuid) {
        this.tuid = tuid;
    }
}
