package com.ociweb.protocoltest.protobuf.size;

import java.io.IOException;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
import com.ociweb.protocoltest.protobuf.size.PBSizeQueryProvider.PBQuery;

public class PBSizeProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PBSizeProducer.class);

    private final StreamRegulator regulator;
    private final int count;
    private final SequenceExampleAFactory testDataFactory;

    public PBSizeProducer(StreamRegulator regulator, int count, SequenceExampleAFactory testSentDataFactory) {
        this.regulator = regulator;
        this.count = count;
        this.testDataFactory = testSentDataFactory;
    }
    
    @Override
    public void run() {
        try {

            OutputStream out = regulator.getOutputStream();
            DataOutputBlobWriter<RawDataSchema> blobWriter = regulator.getBlobWriter();
            long lastNow = 0;

            PBQuery.Builder query_builder = PBQuery.newBuilder();
            PBQuery.PBSample.Builder sample_builder = PBQuery.PBSample.newBuilder();
            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write

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
                            .setTime(sample.getTime())
                            .setMeasurement(sample.getMeasurement())
                            .setAction(sample.getAction())
                            .build());
                        sample_builder.clear();
                    }

                    query_builder.build().writeDelimitedTo(out);
//                    query_builder.build().writeTo(out);
//                    out.close();

                    query_builder.clear();
                    
                    lastNow = App.recordSentTime(lastNow, blobWriter);
                    
                    
                }
                App.commmonWait(); //we are faster than the consumer
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("PBSizeProducer finished");
    }
}
