package com.ociweb.protocoltest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;

public class App {

    //Put this line at the top of every class and be sure to change the Class name to that of the class in question.
    private static final Logger log = LoggerFactory.getLogger(App.class);
    
    public static void main(String[] args) {
       
        log.info("Hello World, we are running...");
               
        
        int totalMessageCount = 100000; //large fixed value for running the test
        Histogram histogram = new Histogram(3600000000000L, 2);
        
        
        long bitPerSecond = 10*1024*1024;
        int maxWrittenChunksInFlight = 10;
        int maxWrittenChunkSizeInBytes= 10*1024;
        StreamRegulator regulator = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
                        
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        Producer p = new Producer(regulator, totalMessageCount);
        Consumer c = new Consumer(regulator, totalMessageCount, histogram);
           
        long startTime = System.currentTimeMillis();
        
        executor.execute(p);
        executor.execute(c);
        
        executor.shutdown();//prevent any new submissions to execution service but let those started run.
                 
        try {
            if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
                log.error("test time out, no valid results");
                System.exit(-1);
            }
        } catch (InterruptedException e) {
            //Nothing to do Just exit
        }
        
        long totalBytesSent =regulator.getBytesWritten();
        long durationInMs = System.currentTimeMillis()-startTime;
        
        log.info("Total duration {}ms",durationInMs);
        log.info("TotalBytes {}",totalBytesSent);
        
        System.out.println("Latency measured in Nanoseconds");
        histogram.outputPercentileDistribution(System.out, 100.0);
    }
    
    
    
    

}
