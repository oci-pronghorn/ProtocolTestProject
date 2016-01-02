package com.ociweb.protocoltest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.pronghorn.util.CPUMonitor;
import com.ociweb.protocoltest.avro.AvroConsumer;
import com.ociweb.protocoltest.avro.AvroProducer;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASchema;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGeneratorCustom;
import com.ociweb.protocoltest.kryo.KryoConsumer;
import com.ociweb.protocoltest.kryo.KryoProducer;
import com.ociweb.protocoltest.phast.PhastConsumer;
import com.ociweb.protocoltest.phast.PhastProducer;
import com.ociweb.protocoltest.protobuf.size.PBSizeConsumer;
import com.ociweb.protocoltest.protobuf.size.PBSizeProducer;
import com.ociweb.protocoltest.protobuf.speed.PBSpeedConsumer;
import com.ociweb.protocoltest.protobuf.speed.PBSpeedProducer;
import com.ociweb.protocoltest.template.EmptyConsumer;
import com.ociweb.protocoltest.template.EmptyProducer;
import com.ociweb.protocoltest.thrift.ThriftConsumer;
import com.ociweb.protocoltest.thrift.ThriftProducer;

public class App {

    //Put this line at the top of every class and be sure to change the Class name to that of the class in question.
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public enum TestType {
        Empty,
        PBSpeed,
        PBSize,
        Kryo,
        Avro,
        Thrift,
        Phast
    }

    public enum PBSpeedRunType {
        Translate,
        DirectSend
    }
    
    public static PBSpeedRunType pbSpeedRunType = PBSpeedRunType.Translate;

    public static void main(String[] args) {

        String testType = getOptArg("-testType","-t", args, "Empty");
        String bandwidthMpbs = getOptArg("-bandwidth","-b", args, "102400");
        String termWait = getOptArg("-termWaitSec","-w", args, "500");


        TestType type;

        switch(testType) {
            case "PBSpeed":
                type = TestType.PBSpeed;
    
                String pbRunType = getOptArg("-PBSpeedRunType","-p", args, "Translate");
                switch (pbRunType) {
                case "DirectSend":
                    pbSpeedRunType = PBSpeedRunType.DirectSend;
                    break;
                case "Translate":
                default:
                    pbSpeedRunType = PBSpeedRunType.Translate;
                }
    
                break;
            case "PBSize":
                type = TestType.PBSize;
                break;
            case "Kryo":
                type = TestType.Kryo;
                break;
            case "Avro":
                type = TestType.Avro;
                break;
            case "Thrift":
                type = TestType.Thrift;
                break;
            case "Pronghorn":
                type = TestType.Phast;
                break;
            case "Empty":
            default:
                type = TestType.Empty;
        }
        
        long mbps = Long.parseLong(bandwidthMpbs);
        long termination_wait = Long.parseLong(termWait); //Seconds to wait for test to complete

        log.info("Hello World, we are running...");
        
        Histogram histogram = new Histogram(3600000000000L, 3);
        
        
        long bitPerSecond = mbps*1024L*1024L;
        int maxWrittenChunksInFlight = 8;//to minimize latency (littles law) this is kept small.
        int maxWrittenChunkSizeInBytes= (int)(SequenceExampleA.estimatedBytes(SequenceExampleASchema.FIXED_SAMPLE_COUNT) * 10);
        
        
        StreamRegulator regulator = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);

        CPUMonitor cpuMonitor = new CPUMonitor(1000);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        Runnable p,c;

        SequenceExampleAFactory testSentDataFactory = new SequenceExampleAFuzzGeneratorCustom();
        SequenceExampleAFactory testExpectedDataFactory = new SequenceExampleAFuzzGeneratorCustom();
        
        int totalMessageCount = 100_000;// fixed value for running the test
        
        //for small values must grow message count to lengthen the test duration beyond 1 second for accurate results
        if (SequenceExampleASchema.FIXED_SAMPLE_COUNT<128) {
            totalMessageCount *= 100;
        }
        
        switch (type) {
            case PBSize:
                System.out.println("Running Protobuf Size Test");
                p = new PBSizeProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new PBSizeConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case PBSpeed:
                System.out.println("Running Protobuf Speed Test");
                p = new PBSpeedProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new PBSpeedConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case Avro:
                System.out.println("Running Avro Test");
                p = new AvroProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new AvroConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case Kryo:
                System.out.println("Running Kryo Test");
                p = new KryoProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new KryoConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case Thrift:
                System.out.println("Running Thrift Test");
                p = new ThriftProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new ThriftConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case Phast:
                System.out.println("Running Pronghorn Test");
                p = new PhastProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new PhastConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
                break;
            case Empty:
            default:
                System.out.println("Running Empty Test");
                //NOTE: when adding new protocols to test start by making copies of the EmptyProducer and EmptyConsumer
                p = new EmptyProducer(regulator, totalMessageCount, testSentDataFactory);
                c = new EmptyConsumer(regulator, totalMessageCount, histogram, testExpectedDataFactory);
        }


        long startTime = System.currentTimeMillis();

        cpuMonitor.start();
        executor.execute(p);
        executor.execute(c);

        executor.shutdown();//prevent any new submissions to execution service but let those started run.

//
//  Block used for profile record, not normaly used.
//        
//        try {
//            Thread.sleep(60*1000);
//        } catch (InterruptedException e1) {
//           return;
//        }
//        System.out.println("one minute has passed");
        
        
        
        try {
            if (!executor.awaitTermination(termination_wait, TimeUnit.SECONDS)) {
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
        float kmsgPerSec = totalMessageCount/(float)durationInMs;
        
        System.out.println("Latency Value in microseconds");
        histogram.outputPercentileDistribution(System.out, 1000.0);

        System.out.println();
        System.out.println("Process CPU Usage (All threads started by this Java instance)");
        cpuHist.outputPercentileDistribution(System.out, CPUMonitor.UNIT_SCALING_RATIO);
        
        log.info("K Mgs Per Second {}",kmsgPerSec);
        log.info("Total duration {}ms",durationInMs);
        log.info("{} bytes sent",totalBytesSent);

        log.info("{} Kbps",kBitsPerSec);
        log.info("{} Mbps",mBitsPerSec);
        
        
        long totalGeneratedRawSize = totalSizeGenerated(totalMessageCount);
        float fraction = ((float)totalBytesSent)/((float)totalGeneratedRawSize);
        float compressionPct = 100f*(1f-fraction);
        log.info("{} B raw test bytes ",totalGeneratedRawSize);
        log.info("{} B message size ",totalGeneratedRawSize/totalMessageCount);
        log.info("{}% compressed", compressionPct);
        
        
    }
    
    public static void commmonWait() {
      // LockSupport.parkNanos(10); //rarely needed when we have contention over the regulator
    }
    
    private static long totalSizeGenerated(int totalCount) {
        long sum = 0;
        SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGeneratorCustom();

        int i = totalCount;
        while (--i>=0) {
            sum += testDataFactory.nextObject().estimatedBytes();
        }
        return sum;        
    }
    
    private static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
        
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    return defaultValue;
                }
                return reportChoice(longName, shortName, token.trim());
            }
            prev = token;
        }
        return reportChoice(longName, shortName, defaultValue);
    }
    
    private static String reportChoice(final String longName, final String shortName, final String value) {
        System.out.print(longName);
        System.out.print(" ");
        System.out.print(shortName);
        System.out.print(" ");
        System.out.println(value);
        return value;
    }
    
    public static long recordSentTime(long lastNow, DataOutputBlobWriter<RawDataSchema> writer) {
        long now = System.nanoTime();
        if (now < lastNow) {//defend against the case that this is not "real" time and can move backwards.
            now = lastNow;
        }
        writer.writePackedLong(now-lastNow);                       
        return now;
    }

    public static long recordLatency(long lastNow, Histogram h, DataInputBlobReader<RawDataSchema> reader) {
        long timeMessageWasSentDelta = reader.readPackedLong();
        
        lastNow += timeMessageWasSentDelta;                            
        //Note after the message is decoded the latency for the message must be computed using.
        
        long latency = System.nanoTime() - lastNow;
        if (latency>=0 && 0!=lastNow) {//conditional to protect against numerical overflow, see docs on nanoTime();
            h.recordValue(latency);
        }
        return lastNow;
    }

}
