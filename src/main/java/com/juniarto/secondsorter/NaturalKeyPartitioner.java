/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.juniarto.secondsorter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author hduser
 */
public class NaturalKeyPartitioner extends Partitioner<TextDsi, IntWritable> {
    public int getPartition(TextDsi key, IntWritable val, int numPartitions){
        int hash = key.getKey().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
