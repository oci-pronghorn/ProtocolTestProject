package com.ociweb.protocoltest.data;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.stage.test.FuzzGeneratorGenerator;
import com.ociweb.pronghorn.util.Appendables;

public class SequenceExampleAGenerator extends FuzzGeneratorGenerator {

    private final static String WORKING_SPACE = "working";
    private final static String WORKING_IDX   = "workingIdx";
    
    
    private final static int    WORKING_BITS = 5;
    private final static int    WORKING_SIZE = 1<<WORKING_BITS;
    private final static int    WORKING_MASK = WORKING_SIZE-1;
    
    public SequenceExampleAGenerator(Appendable target, boolean asRunnable, int seqCountBits) {
        super(SequenceExampleASchema.instance, target, asRunnable, false);
        
        setMaxSequenceLengthInBits(seqCountBits);
        
        //TODO: add method here for building sparse array.
        //TODO: need to fix zero length case for sequence
        //TODO: need to modify run to continue until object is complete? use the  if statement instead of case?
        
    }
    
    @Override
    protected void additionalMembers(Appendable target) throws IOException {
        super.additionalMembers(target);
                
        target.append(tab).append(SequenceExampleA.class.getSimpleName()).append("[] ").append(WORKING_SPACE).append(" = buildWorkspace();\n");
        target.append(tab).append("int ").append(WORKING_IDX).append(";\n");
               
    }

    @Override
    protected void additionalImports(Appendable target) throws IOException {
        super.additionalImports(target);
        target.append("import ").append(SequenceExampleA.class.getCanonicalName()).append(";\n");       
    }
    
    @Override
    protected void additionalMethods(Appendable target) throws IOException {
        super.additionalMethods(target);
        
        
        target.append("public ").append(SequenceExampleA.class.getSimpleName()).append("[] ").append("buildWorkspace() {\n");
        Appendables.appendValue(target.append(tab), "int i = ",WORKING_SIZE,";\n");
        target.append(tab).append(SequenceExampleA.class.getSimpleName()).append("[] ").append(WORKING_SPACE).append(" = new ");
        target.append(SequenceExampleA.class.getSimpleName()).append("[i];\n");
        target.append(tab).append("while(--i>=0) {\n");
        target.append(tab).append(tab).append(WORKING_SPACE).append("[i] = new ").append(SequenceExampleA.class.getSimpleName()).append("();\n");     
        target.append(tab).append("}\n");
        target.append(tab).append("return ").append(WORKING_SPACE).append(";\n");
        target.append("}\n");
               
        
        
        //public SequenceExampleA nextObject() {        
        //  do {
        //     run();
        //  } while (!LowLevelStageManager.isStartNewMessage(navState));        
        //return workingObj;
        //}
        
        target.append("public ").append(SequenceExampleA.class.getSimpleName()).append(" nextObject(){\n");
        target.append(tab).append("do {\n");
        target.append(tab).append(tab).append("run();\n");
        Appendables.appendStaticCall(target.append(tab).append("} while (!"), LowLevelStateManager.class, "isStartNewMessage").append(stageMgrVarName).append("));\n");        
        target.append(tab).append("return ").append(WORKING_SPACE).append("[").append(WORKING_IDX).append("]").append(";\n");
        target.append("}\n");
        
        
        
        target.append("public void skip(int i) {\n");
        target.append(tab).append("while(--i>=0) {\n");
        target.append(tab).append(tab).append("nextObject();\n");
        target.append(tab).append("}\n");
        target.append("}\n");
        
    }

    @Override
    protected void appendAdditionalWriteLogic(Appendable target, int cursor, CharSequence argSignature,
                                              int fieldCount, int firstFieldIdx) throws IOException {
        
        if (SequenceExampleASchema.MSG_DAYSUMMARY_1 == cursor) {
            //begin message
 //      target.append(tab).append(TEMP_OBJ_NAME).append(" = new ").append(SequenceExampleA.class.getSimpleName()).append("();\n");
                      
            
            target.append(tab).append(WORKING_IDX).append(" = ");            
            Appendables.appendHexDigits(target, WORKING_MASK).append(" & (1+").append(WORKING_IDX).append(");\n");
            
        
            // SequenceExampleA.setAll(obj, user, year, month, date, sampleCount);
            Appendables.appendStaticCall(target.append(tab), SequenceExampleA.class, "setAll").append(WORKING_SPACE).append("[").append(WORKING_IDX).append("]").append(", ");
            Appendables.appendAndSkip(target, argSignature, "int ").append(");\n");

        } else {
            if (cursor < SequenceExampleASchema.FROM.tokensLen-1) {
                
                
                //start of the one and only sequence for this example
                
                Appendables.appendStaticCall(target.append(tab), SequenceExampleA.class, "setSample").append(WORKING_SPACE).append("[").append(WORKING_IDX).append("]").append(", ");
                
                //Need count?
                //LowLevelStageManager.interationIndex(navState);
                
                //   SequenceExampleA.setSample(obj, idx, id, time, measurement, action);
                Appendables.appendStaticCall(target, LowLevelStateManager.class, "interationIndex").append(stageMgrVarName).append("),");
                Appendables.appendAndSkip(target, Appendables.appendAndSkip(new StringBuilder(), argSignature, "long ")  , "int ");
                target.append(");\n");
                                
            }        
        
        } 
    }

    
    
}
