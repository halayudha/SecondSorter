/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.io.TextDsi;
/**
 *
 * @author hduser
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
    
    protected NaturalKeyGroupingComparator(){
        super(TextDsi.class, true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2){
        TextDsi k1 = (TextDsi)w1;
        TextDsi k2 = (TextDsi)w2;
        
        return k1.getKey().compareTo(k2.getKey());
    }
    
}
