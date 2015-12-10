package com.ociweb.protocoltest;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.PBMessageProvider.PBQuery;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;

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

            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();

            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write

                    //NOTE: the messages sent must contain the timestamp for now so we can compute latency per message
//                    long now = System.nanoTime();

                    //Use something to write objects to the output stream
                    //Note this must NOT exceeded the chunk size.


                    //NOTE: this is how objects are fetched for writing.
                    SequenceExampleA writeMe = testDataFactory.nextObject();

                    PBQuery.Builder query = PBQuery.newBuilder()
                        .setUser(writeMe.getUser())
                        .setYear(writeMe.getYear())
                        .setMonth(writeMe.getMonth())
                        .setDate(writeMe.getDate())
                        .setSampleCount(writeMe.getSampleCount());

                    for (SequenceExampleASample sample : writeMe.getSamples()) {
                        query.addSamples(PBQuery.PBSample.newBuilder()
                            .setId(sample.getId())
                            //TODO: Currently time in sample is incorrect when comparing with
                            //System.nanoTime in consumer.  Need to use same time standard.
//                          .setTime(sample.getTime())
                            .setTime(System.nanoTime())
                            .setMeasurement(sample.getMeasurement())
                            .setAction(sample.getAction())
                            .build());
                    }

                    query.build().writeDelimitedTo(out);

                }
                Thread.yield(); //we are faster than the consumer
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("producer finished");
    }
}
