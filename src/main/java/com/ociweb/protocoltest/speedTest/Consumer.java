package com.ociweb.protocoltest.speedTest;

import java.io.IOException;
import java.io.InputStream;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;

public class Consumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;
    
    public Consumer(StreamRegulator regulator, int count, Histogram histogram) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
    }
    
    @Override
    public void run() {
        try {
        
            InputStream in = regulator.getInputStream();
            
            int i = count;
            StreamRegulator r = regulator; //TODO: may want to make same change in protocol test.
            Histogram h = histogram;
            while (i>0) {
                while (r.hasNextChunk() && --i>=0) {
                    //use something to read the data from the input stream
                    
                    
                        int myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        myValue = in.read();//Do not keep this code, for example only.
                        if (myValue!=42) {
                            throw new RuntimeException("expected 42 but found "+myValue);
                        }
                        
                        //This shoudl come from one of the fields inside the encoded message
                        long timeMessageWasSent =  ( ( (  (long)in.read()) << 56) |     //Do not keep this code, for example only.         
                                                    ( (0xFFl & in.read()) << 48) |
                                                    ( (0xFFl & in.read()) << 40) |
                                                    ( (0xFFl & in.read()) << 32) |
                                                    ( (0xFFl & in.read()) << 24) |
                                                    ( (0xFFl & in.read()) << 16) |
                                                    ( (0xFFl & in.read()) << 8) |
                                                      (0xFFl & in.read()) ); 
                        
                        
                        //Note after the message is decoded the latency for the message must be computed using.
                        long latency = System.nanoTime() - timeMessageWasSent;
                        if (latency>=0) {//conditional to protect against numerical overflow, see docs on nanoTime();
                            h.recordValue(latency);
                        }
                        
                }
              //  Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg consumer is faster than producer.  
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
        log.info("consumer finished");        
    }

}
