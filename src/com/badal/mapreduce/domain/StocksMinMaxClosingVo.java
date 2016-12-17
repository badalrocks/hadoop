package com.badal.mapreduce.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Domain class for wrapping min and max closing price of stock. Implements Writable interface so that its instances can
 * be uses as Value in Mapper and Reducer classes. Overrides readFields and write methods of Writable interface.
 * 
 * @author badal
 *
 */
public class StocksMinMaxClosingVo implements Writable
{
    private Double minClosing;
    private Double maxClosing;

    public Double getMinClosing()
    {
        return minClosing;
    }

    public void setMinClosing(Double minClosing)
    {
        this.minClosing = minClosing;
    }

    public Double getMaxClosing()
    {
        return maxClosing;
    }

    public void setMaxClosing(Double maxClosing)
    {
        this.maxClosing = maxClosing;
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
        minClosing = new Double(input.readDouble());
        maxClosing = new Double(input.readDouble());
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
        output.writeDouble(minClosing);
        output.writeDouble(maxClosing);
    }

    @Override
    public String toString()
    {
        // output format of this object.
        return minClosing + ", " + maxClosing;
    }

}
