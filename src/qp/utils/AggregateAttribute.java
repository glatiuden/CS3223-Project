package qp.utils;

public class AggregateAttribute {

    public final int attrIndex;
    public final int aggType;
    public final int attrType;
    private int count;
    private float sum;
    private Object aggVal;
    public final String colName;

    public AggregateAttribute(int attrIndex, int aggType, int attrType, String colName) {
        this.attrIndex = attrIndex;
        this.aggType = aggType;
        this.attrType = attrType;
        this.colName = colName;

        switch (aggType) {
        case Attribute.MAX:
        case Attribute.MIN:
            aggVal = null;
            break;
        case Attribute.SUM:
        case Attribute.COUNT:
        case Attribute.AVG:
            aggVal = 0;
            break;
        }
        this.sum = 0;
        this.count = 0;
    }

    /**
     * Takes in a Tuple and process the value at that tuple with its aggregation query
     * @param t Tuple to be processed
     */
    public void setAggVal(Tuple t) {
        Object val = t.dataAt(attrIndex);
        switch (attrType) {
            /* INT only supports MAX, MIN, SUM and COUNT. AVG projected type is REAL. */
            case Attribute.INT:
                int intVal = 0;
                //Guard clause to prevent parsing STRING into INT for COUNT operation
                if (val instanceof Number) {
                    intVal = ((Number) val).intValue();
                }
                switch (aggType) {
                case Attribute.MAX:
                    if (aggVal == null) {
                        aggVal = intVal;
                    } else {
                        aggVal = Math.max(intVal, (int) aggVal);
                    }
                    break;
                case Attribute.MIN:
                    if (aggVal == null) {
                        aggVal = intVal;
                    } else {
                        aggVal = Math.min(intVal, (int) aggVal);
                    }
                    break;
                case Attribute.SUM:
                    aggVal = intVal + (int) aggVal;
                    break;
                case Attribute.COUNT:
                    aggVal = (int) aggVal + 1;
                    break;
                }
                break;
            /* String only supports MAX and MIN operation since COUNT, AVG projected type is INT/REAL */
            case Attribute.STRING:
                String stringVal = val.toString();
                switch (aggType) {
                case Attribute.MAX:
                    if (aggVal == null || stringVal.compareTo((String) aggVal) > 0) {
                        aggVal = stringVal;
                    }
                    break;
                case Attribute.MIN:
                    if (aggVal == null || stringVal.compareTo((String) aggVal) < 0)
                        aggVal = stringVal;
                    break;
                }
                break;
            /* REAL only supports MAX, MIN, SUM and AVG. COUNT projected type is INT */
            case Attribute.REAL:
                float floatVal = ((Number) val).floatValue();
                switch (aggType) {
                case Attribute.MAX:
                    if (aggVal == null) {
                        aggVal = floatVal;
                    } else {
                        aggVal = Math.max(floatVal, (float) aggVal);
                    }
                    break;
                case Attribute.MIN:
                    if (aggVal == null) {
                        aggVal = floatVal;
                    } else {
                        aggVal = Math.min(floatVal, (float) aggVal);
                    }
                    break;
                case Attribute.SUM:
                    aggVal = floatVal + ((Number) aggVal).floatValue();
                    break;
                case Attribute.AVG:
                    sum = sum + floatVal;
                    count++;
                    aggVal = sum / count;
                    break;
                }
                break;
        }
    }

    public Object getAggVal() {
        return aggVal;
    }
}
