package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class SequenceExampleASchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc140000c,0x80000000,0x80200001,0x80200002,0x80200003,0xd0000004,0xc1800005,0x80000005,0x90800000,0x80200006,0x80600007,0xc1a00005,0xc120000c},
            (short)0,
            new String[]{"DaySummary","User","Year","Month","Date","SampleCount","Samples","Id","Time","Measurement","Action",null,null},
            new long[]{1, 100, 101, 102, 103, 202, 0, 203, 204, 205, 206, 0, 0},
            new String[]{"global",null,null,null,null,null,null,null,null,null,null,null,null},
            "sequenceExampleA.xml",
            new long[]{16, 16, 0},
            new int[]{16, 1, 0, 3, 1, 2, 0, 1, 1, 1, 10, 8, 0});
    
    public final static SequenceExampleASchema instance = new SequenceExampleASchema();
    
    
    private SequenceExampleASchema() {
        super(FROM);
    }
    
    public static final int MSG_DAYSUMMARY_1 = 0x00000000;
    public static final int MSG_DAYSUMMARY_1_FIELD_USER_100 = 0x00000001;
    public static final int MSG_DAYSUMMARY_1_FIELD_YEAR_101 = 0x00000002;
    public static final int MSG_DAYSUMMARY_1_FIELD_MONTH_102 = 0x00000003;
    public static final int MSG_DAYSUMMARY_1_FIELD_DATE_103 = 0x00000004;
    public static final int MSG_DAYSUMMARY_1_FIELD_SAMPLECOUNT_202 = 0x02800005;
    public static final int MSG_DAYSUMMARY_1_FIELD_SAMPLES_0 = 0x02000006;
    public static final int MSG_DAYSUMMARY_1_FIELD_ID_203 = 0x00000000;
    public static final int MSG_DAYSUMMARY_1_FIELD_TIME_204 = 0x00800001;
    public static final int MSG_DAYSUMMARY_1_FIELD_MEASUREMENT_205 = 0x00000003;
    public static final int MSG_DAYSUMMARY_1_FIELD_ACTION_206 = 0x00000004;
    

}
