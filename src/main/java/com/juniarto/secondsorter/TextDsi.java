/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 *
 * @author hduser
 */
public class TextDsi implements WritableComparable<TextDsi>{
    
    private String theKey;
    private long theOffset;
    private int theKeyCount;
    
    public TextDsi(){}
    
    public TextDsi(String key, long offset, int keycount){
        this.theKey = key;
        this.theOffset = offset;
        this.theKeyCount = keycount;
    }
    
    @Override
    public String toString(){
        return (new StringBuilder())
                    //.append('{')
                    .append(theKey)
                    //.append(",")
                    .append(theOffset)
                    .append(",")
                    .append(theKeyCount)
                    //.append("}")
                .toString();
    }
   
    
 
    /** 
   * Serialize the fields of this object to <code>out</code>.
   * 
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
    @Override
    public void write(DataOutput out) throws IOException{
        WritableUtils.writeString(out, theKey);
        out.writeLong(theOffset);
        out.writeInt(theKeyCount);
    }
    
    /** 
   * Deserialize the fields of this object from <code>in</code>.  
   * 
   * <p>For efficiency, implementations should attempt to re-use storage in the 
   * existing object where possible.</p>
   * 
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
    @Override
    public void readFields(DataInput in) throws IOException{
        theKey = WritableUtils.readString(in);
        theOffset = in.readLong();
        theKeyCount = in.readInt();
    }
    
    @Override
    public int compareTo(TextDsi o){
        int result = theKey.compareTo(o.theKey);
        if(0 == result){
            result = (theOffset > (o.theOffset)) ? 1 : 0;
        }
        return result;
    }
    
    public String getKey(){
        return theKey;
    }
    
    public void setKey(String key){
        this.theKey = key;
    }
    
    public long getOffset(){
        return theOffset;
    }
    
    public void setOffset(int offset){
        this.theOffset = offset;
    }
    
    public int getKeyCount(){
        return this.theKeyCount;
    }
    
    public void setKeyCount(int keycount){
        this.theKeyCount = keycount;
    }
}
