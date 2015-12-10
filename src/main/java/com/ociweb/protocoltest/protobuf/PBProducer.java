package com.ociweb.protocoltest.protobuf;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.*;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
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
    
    private int memoizedSerializedSizeSample = -1;
    public int getSerializedSize(final SequenceExampleASample sample) {
      int size = memoizedSerializedSizeSample;
      if (size != -1) return size;

      size = 0;
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, sample.getId());
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, sample.getTime());
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, sample.getMeasurement());
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(4, sample.getAction());
      memoizedSerializedSize = size;
      return size;
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize(final SequenceExampleA sample) {
        int size = memoizedSerializedSize;
        if (size != -1) return size;

        size = 0;
        size += com.google.protobuf.CodedOutputStream
            .computeInt32Size(1, sample.getUser());
          size += com.google.protobuf.CodedOutputStream
            .computeInt32Size(2, sample.getYear());
          size += com.google.protobuf.CodedOutputStream
            .computeInt32Size(3, sample.getMonth());
          size += com.google.protobuf.CodedOutputStream
            .computeInt32Size(4, sample.getDate());
          size += com.google.protobuf.CodedOutputStream
            .computeInt32Size(5, sample.getSampleCount());
        for (int i = 0; i < sample.getSamples().size(); i++) {
        	size += com.google.protobuf.CodedOutputStream.computeTagSize(6);
            final int tmp_size = getSerializedSize(sample.getSamples().get(i));
            size += com.google.protobuf.CodedOutputStream.computeRawVarint32Size(tmp_size) + tmp_size;
        }
        memoizedSerializedSize = size;
        return size;
      }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output, final SequenceExampleA sample)
            throws java.io.IOException {
//		getSerializedSize();
		output.writeInt32(1, sample.getUser());
		output.writeInt32(2, sample.getYear());
		output.writeInt32(3, sample.getMonth());
		output.writeInt32(4, sample.getDate());
		output.writeInt32(5, sample.getSampleCount());
		for (int i = 0; i < sample.getSamples().size(); i++) {
			output.writeMessage(6, samples_.get(i));
		}
	}
    
    private void writeDelimitedTo(final OutputStream output, final SequenceExampleA sample) throws IOException {
        final int serialized = getSerializedSize(sample);
        final int dataLength = CodedOutputStream.computeRawVarint32Size(serialized) + serialized;
        final int DEFAULT_BUFFER_SIZE = 4096;
        final int bufferSize = dataLength > DEFAULT_BUFFER_SIZE ? DEFAULT_BUFFER_SIZE : dataLength; 
//        		CodedOutputStream.computePreferredBufferSize(
//            CodedOutputStream.computeRawVarint32Size(serialized) + serialized);
        final CodedOutputStream codedOutput =
            CodedOutputStream.newInstance(output, bufferSize);
        codedOutput.writeRawVarint32(serialized);
        writeTo(codedOutput, sample);
        codedOutput.flush();
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
