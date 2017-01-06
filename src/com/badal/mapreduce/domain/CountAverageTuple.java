package com.badal.mapreduce.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable
{
    private long   count;
    private double average;

    @Override
    public void readFields(DataInput input) throws IOException
    {
        count = new Long(input.readLong());
        average = new Double(input.readDouble());

    }

    @Override
    public void write(DataOutput output) throws IOException
    {
        output.writeLong(count);
        output.writeDouble(average);

    }

    public long getCount()
    {
        return count;
    }

    public void setCount(long count)
    {
        this.count = count;
    }

    public double getAverage()
    {
        return average;
    }

    public void setAverage(double average)
    {
        this.average = average;
    }

    @Override
    public String toString()
    {
        return "average=" + average;
    }
    
    

}
