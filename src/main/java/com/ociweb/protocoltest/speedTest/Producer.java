package com.ociweb.protocoltest.speedTest;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;

public class Producer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);
            
    private final StreamRegulator regulator;
    private final int count;
    
    public Producer(StreamRegulator regulator, int count) {
        this.regulator = regulator;
        this.count = count;
    }
    
    @Override
    public void run() {
        try {
            
            OutputStream out = regulator.getOutputStream();
                      
            StreamRegulator r = regulator;
            int i = count;
            while (i>0) {
                while (r.hasRoomForChunk() && --i>=0) { //Note we are only dec when ther is room for write
                    
                    //NOTE: the messages sent must contain the timestamp for now so we can compute latency per message 
                    long now = System.nanoTime();
                    
                    //Use something to write objects to the output stream
                    //Note this must NOT exceeded the chunk size.
                    
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        out.write(42);  //Do not keep this code, for example only.
                        
                        out.write((byte)(now >>> 56));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 48));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 40));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 32));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 24));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 16));//Do not keep this code, for example only.
                        out.write((byte)(now >>> 8));//Do not keep this code, for example only.
                        out.write((byte) now);//Do not keep this code, for example only.
                      
                    
                    
                }
                Thread.yield(); //we are faster than the consumer
            }
            
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } 
        log.info("producer finished");        
    }
}
