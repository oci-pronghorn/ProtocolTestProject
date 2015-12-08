package com.ociweb.protocoltest.data;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import com.ociweb.pronghorn.util.Appendables;

public class SequenceExampleA {

    private int user;
    private int year;
    private int month;
    private int date;
    private int sampleCount;
    
    private SequenceExampleASample[] samples;
    private List<SequenceExampleASample> samplesAsList = new SampleList();

    public String toString() {
        try {
            return appendToString(new StringBuilder()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SequenceExampleA) {
            SequenceExampleA that = (SequenceExampleA)obj;
            return this.user == that.user &&
                   this.year == that.year &&
                   this.month == that.month &&
                   this.date == that.date &&
                   this.sampleCount == that.sampleCount &&
                   Arrays.deepEquals(this.samples, that.samples);
        }
        return false;
    }

    private <A extends Appendable> A appendToString(A target) throws IOException {
        Appendables.appendValue(target,"User:" ,user,"\n");
        Appendables.appendValue(target,"Year:" ,year,"\n");
        Appendables.appendValue(target,"Month:",month,"\n");
        Appendables.appendValue(target,"Date:" ,date,"\n");
        Appendables.appendValue(target,"SampleCount:",sampleCount,"\n");
        
        for(int i=0; i<sampleCount; i++) {
            target.append("----------------\n");
            samples[i].appendToString(target);
        }
        return target;
    }
    
    public static void setAll(SequenceExampleA that, int user, int year, int month, int date, int sampleCount) {
        that.user = user;
        that.year = year;
        that.month = month;
        that.date = date;
        that.sampleCount = sampleCount;
        ensureCapacity(that,sampleCount);
    }
    
    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDate() {
        return date;
    }

    public void setDate(int date) {
        this.date = date;
    }

    public int getSampleCount() {
        return sampleCount;
    }
    
    public List<SequenceExampleASample> getSamples() {
        return samplesAsList;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
        ensureCapacity(this, sampleCount);
    }

    private static void ensureCapacity(SequenceExampleA that, int sampleCount) {
        //System.out.println("capacity of "+sampleCount);
        //TODO: NOTE when the sample count is zero we crash because it gets run anyway - FIX ASAP
        if (sampleCount==0) {
            sampleCount = 1;
        }
        
        
        if (null==that.samples || that.samples.length<sampleCount) {
            that.samples = new SequenceExampleASample[sampleCount];
            int i = sampleCount;
            while (--i>=0) {
                that.samples[i] = new SequenceExampleASample();
            } 
        }
    }
    
    private class SampleList extends AbstractList<SequenceExampleASample> {

        @Override
        public SequenceExampleASample get(int index) {
            return samples[index];
        }

        @Override
        public int size() {
           return sampleCount;
        }
    }

    public static void setSample(SequenceExampleA obj, int idx, int id, long time, int measurement, int action) {
        SequenceExampleASample.setAll(obj.samples[idx], id, time, measurement, action);   
    }
    
}
