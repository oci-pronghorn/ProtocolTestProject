package com.ociweb.protocoltest.protobuf;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
import com.ociweb.protocoltest.protobuf.PBQueryProvider.PBQuery;

public class PBProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PBProducer.class);

    private final StreamRegulator regulator;
    private final int count;

    public PBProducer(StreamRegulator regulator, int count) {
        this.regulator = regulator;
        this.count = count;
    }

    @Override
    public void run() {
        try {

            OutputStream out = regulator.getOutputStream();

            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            PBQuery.Builder query_builder = PBQuery.newBuilder();
            PBQuery.PBSample.Builder sample_builder = PBQuery.PBSample.newBuilder();
            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write

                    //NOTE: the messages sent must contain the timestamp for now so we can compute latency per message
//                    long now = System.nanoTime();

                    //Use something to write objects to the output stream
                    //Note this must NOT exceeded the chunk size.


                    //NOTE: this is how objects are fetched for writing.
                    SequenceExampleA writeMe = testDataFactory.nextObject();

                        query_builder.setUser(writeMe.getUser())
                        .setYear(writeMe.getYear())
                        .setMonth(writeMe.getMonth())
                        .setDate(writeMe.getDate())
                        .setSampleCount(writeMe.getSampleCount());

                    for (SequenceExampleASample sample : writeMe.getSamples()) {
                        query_builder.addSamples(sample_builder
                            .setId(sample.getId())
                            //TODO: Currently time in sample is incorrect when comparing with
                            //System.nanoTime in consumer.  Need to use same time standard.
//                          .setTime(sample.getTime())
                            .setTime(System.nanoTime())
                            .setMeasurement(sample.getMeasurement())
                            .setAction(sample.getAction())
                            .build());
                        sample_builder.clear();
                    }

                    query_builder.build().writeDelimitedTo(out);
                    query_builder.clear();
                }
                Thread.yield(); //we are faster than the consumer
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("PBProducer finished");
    }
}
