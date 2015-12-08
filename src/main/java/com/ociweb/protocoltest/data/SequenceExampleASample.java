package com.ociweb.protocoltest.data;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;

public class SequenceExampleASample {
    private int id;
    private long time;
    private int measurement;
    private int action;
        
    public static void setAll(SequenceExampleASample that,int id, long time, int measurement, int action) {
        that.id = id;
        that.time = time;
        that.measurement = measurement;
        that.action = action;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SequenceExampleASample) {
            SequenceExampleASample that = (SequenceExampleASample)obj;
            return this.id == that.id &&
                   this.time == that.time &&
                   this.measurement == that.measurement &&
                   this.action == that.action;
        }
        return false;
    }
    
    public <A extends Appendable> A appendToString(A target) throws IOException {
        
        Appendables.appendValue(target,"Id:" ,id,"\n");
        Appendables.appendValue(target,"Time:" ,time,"\n");
        Appendables.appendValue(target,"Measurment:" ,measurement,"\n");
        Appendables.appendValue(target,"Action:" ,action,"\n");
                
        return target;
    }
    
    public String toString() {
        try {
            return appendToString(new StringBuilder()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public long getTime() {
        return time;
    }
    public void setTime(long time) {
        this.time = time;
    }
    public int getMeasurement() {
        return measurement;
    }
    public void setMeasurement(int measurement) {
        this.measurement = measurement;
    }
    public int getAction() {
        return action;
    }
    public void setAction(int action) {
        this.action = action;
    }


       
}
