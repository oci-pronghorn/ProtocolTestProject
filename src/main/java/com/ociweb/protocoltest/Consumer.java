package com.ociweb.protocoltest;

import java.io.IOException;
import java.io.InputStream;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
import com.ociweb.protocoltest.PBMessageProvider.PBQuery;
import com.ociweb.protocoltest.PBMessageProvider.PBQuery.PBSample;
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

    public boolean compareSamples(PBQuery query, SequenceExampleA sample) {
        if (query.getUser() != sample.getUser() ||
            query.getYear() != sample.getYear() ||
            query.getMonth() != sample.getMonth() ||
            query.getDate() != sample.getDate() ||
            query.getSampleCount() != sample.getSampleCount()) {
            throw new RuntimeException("Received: "+query+"\nExpecting: "+sample);
        }
        if (query.getSamplesList().size() != sample.getSamples().size() ||
            query.getSampleCount() != query.getSamplesList().size()) {
            throw new RuntimeException("SampleCount: "+query.getSampleCount()
                                       +"\nQuery List Size: "+query.getSamplesList().size()
                                       +"\nGenerated List Size: "+sample.getSamples().size());
        }
        for (int x = 0; x < query.getSamplesList().size(); ++x) {
            if (query.getSamples(x).getId() != sample.getSamples().get(x).getId() ||
                query.getSamples(x).getMeasurement() != sample.getSamples().get(x).getMeasurement() ||
                query.getSamples(x).getAction() != sample.getSamples().get(x).getAction()) {
                    throw new RuntimeException("Received Id: "+query.getSamples(x).getId()+" Expected Id: "+sample.getSamples().get(x).getId()
                        +"\nReceived Measurement: "+query.getSamples(x).getMeasurement()+" Expected Measurement: "+sample.getSamples().get(x).getMeasurement()
                        +"\nReceived Action: "+query.getSamples(x).getAction()+" Expected Action: "+sample.getSamples().get(x).getAction());
            }
        }
        return true;
    }

    @Override
    public void run() {
        try {

            InputStream in = regulator.getInputStream();
            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();

            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    //use something to read the data from the input stream

                    PBQuery query = PBQuery.parseDelimitedFrom(in);
                    //At this point the message has been decoded, so use this time for latency calcs
                    long now = System.nanoTime();

                    SequenceExampleA compareToMe = testDataFactory.nextObject();

                    compareSamples(query, compareToMe);

                    for(PBSample pbsample : query.getSamplesList()) {
                        //Note after the message is decoded the latency for the message must be computed using.
                        long latency = now - pbsample.getTime();
                        if (latency>=0) {//conditional to protect against numerical overflow, see docs on nanoTime();
                            histogram.recordValue(latency);
                        }
                    }

                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg consumer is faster than producer.
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("consumer finished");
    }

}
