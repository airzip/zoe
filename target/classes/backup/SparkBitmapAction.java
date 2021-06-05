import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SparkBitmapAction extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("field",DataTypes.BinaryType,true));
        return DataTypes.createStructType(structFields);
    }
    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("field",DataTypes.BinaryType, true));
        return DataTypes.createStructType(structFields);
    }
    @Override
    public boolean deterministic() {
        return false;
    }
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,null);
    }
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        Object in = input.get(0);
        if (in == null) { return;}
        byte[] inBytes = (byte[]) in;
        Object out = buffer.get(0);
        if (out == null) {
            buffer.update(0,inBytes);
            return;
        }
        byte[] outBytes = (byte[]) out;
        byte[] result = outBytes;
        RoaringBitmap outRR = new RoaringBitmap();
        RoaringBitmap inRR = new RoaringBitmap();
        try {
            outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)));
            inRR.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)));
            outRR.or(inRR);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            outRR.serialize(new DataOutputStream(bos));
            result = bos.toByteArray();
        } catch (IOException e) { e.printStackTrace(); }
        buffer.update(0,result);
    }
    @Override
    public void merge(MutableAggregationBuffer buffer1,Row buffer2) {
        update(buffer1,buffer2);
    }
    @Override
    public Object evaluate(Row buffer) {
        long r = 0L;
        Object val = buffer.get(0);
        if (val != null) {
            RoaringBitmap rr = new RoaringBitmap();
            try {
                rr.deserialize(new DataInputStream(new ByteArrayInputStream((byte[]) val)));
                r = rr.getLongCardinality();
            } catch (IOException e) { e.printStackTrace(); }
        }
        return r;
    }
}
