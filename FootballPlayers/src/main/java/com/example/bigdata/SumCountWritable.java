package com.example.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {

    private IntWritable sumWage;
    private IntWritable sumAge;
    private IntWritable count;

    // konstruktor domyslny
    public SumCountWritable() {
        this.sumWage = new IntWritable(0);
        this.sumAge = new IntWritable(0);
        this.count = new IntWritable(0);
    }

    // konstruktor inicjalizujacy
    public SumCountWritable(int wage, int age, int count) {
        this.sumWage = new IntWritable(wage);
        this.sumAge = new IntWritable(age);
        this.count = new IntWritable(count);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sumWage.write(out);
        sumAge.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sumWage.readFields(in);
        sumAge.readFields(in);
        count.readFields(in);
    }

    // gettery
    public int getSumWage() {
        return sumWage.get();
    }

    public int getSumAge() {
        return sumAge.get();
    }

    public int getCount() {
        return count.get();
    }

    // dodawanie wartosci obiektu
    public void add(SumCountWritable other) {
        this.sumWage.set(this.sumWage.get() + other.getSumWage());
        this.sumAge.set(this.sumAge.get() + other.getSumAge());
        this.count.set(this.count.get() + other.getCount());
    }

    // ustawianie nowych wartosci
    public void set(IntWritable sumWage, IntWritable sumAge, IntWritable count) {
        this.sumWage = sumWage;
        this.sumAge = sumAge;
        this.count = count;
    }

    @Override
    public String toString() {
        return sumWage + "\t" + sumAge + "\t" + count;
    }
}
