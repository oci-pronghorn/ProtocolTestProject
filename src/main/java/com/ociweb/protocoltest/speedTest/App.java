package com.ociweb.protocoltest.speedTest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.pronghorn.util.CPUMonitor;

public class App {

    //Put this line at the top of every class and be sure to change the Class name to that of the class in question.
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static ThreadFactory factory = new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) { //TODO: may want to add to protocol test.
           Thread t = new Thread(r);
           t.setPriority(Thread.MAX_PRIORITY);//Must prevent external actvities from getting in the way of what is to be measured.
           return t;
        }
        
    };
    
    public static void main(String[] args) {
       
        log.info("Hello World, we are running...");
               
        //Can we hit 1Gb with shuch a small message? 16 bytes.
        
        //NOTE the fastest handoff is 10-20 million messages per second
        //    this value goes down slowly as mesage size grows 
        //    larger messages will consume more bandwidth up into 10's Gbps
        
        
        int totalMessageCount = 10*1000000; //large fixed value for running the test
        Histogram histogram = new Histogram(3600000000000L, 3);
        
        
        long bitPerSecond = 1L*1024L*1024L*1024L;
        int maxWrittenChunksInFlight = 10;
        int maxWrittenChunkSizeInBytes= 64;//10*1024;
        StreamRegulator regulator = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
                
        CPUMonitor cpuMonitor = new CPUMonitor(100);
        
        ExecutorService executor = Executors.newFixedThreadPool(2,factory);
        
        Producer p = new Producer(regulator, totalMessageCount);
        Consumer c = new Consumer(regulator, totalMessageCount, histogram);
           
        long startTime = System.currentTimeMillis();
        
        cpuMonitor.start();
        executor.execute(p);
        executor.execute(c);
        
        executor.shutdown();//prevent any new submissions to execution service but let those started run.
                 
        try {
            if (!executor.awaitTermination(2000, TimeUnit.SECONDS)) {
                log.error("test time out, no valid results");
                System.exit(-1);
            }
        } catch (InterruptedException e) {
            //Nothing to do Just exit
        }
        Histogram cpuHist = cpuMonitor.stop();
        
        long totalBytesSent =regulator.getBytesWritten();
        long durationInMs = System.currentTimeMillis()-startTime;
        
        long bitsSent = totalBytesSent * 8L;
        float mBitsPerSec = (1000L*bitsSent)/(float)(durationInMs*1024*1024); 
        float kBitsPerSec = (1000L*bitsSent)/(float)(durationInMs*1024); 
        
        
        log.info("Total duration {}ms",durationInMs);
        log.info("TotalBytes {}",totalBytesSent);
        
        log.info("{} Kbps",kBitsPerSec);        
        log.info("{} Mbps",mBitsPerSec);
        
        System.out.println("Latency Value in microseconds");
        histogram.outputPercentileDistribution(System.out, 1000.0);
        
        System.out.println();
        System.out.println("Process CPU Usage (All threads started by this Java instance)");
        cpuHist.outputPercentileDistribution(System.out, CPUMonitor.UNIT_SCALING_RATIO);
    }
    

}
