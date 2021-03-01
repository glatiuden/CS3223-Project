package qp.utils;

public class AggregateAttribute {

    private int attrIndex;
    private int aggType;
    private int attrType;
    private Object aggVal;
    private int count;
    private float sum;

    public AggregateAttribute(int attrIndex, int aggType, int attrType) {
        this.attrIndex = attrIndex;
        this.aggType = aggType;
        this.attrType = attrType;

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

    public void setAggVal(Tuple t) {
        Object val = t.dataAt(attrIndex);
        switch (attrType) {
        /** Note: INT only supports MAX, MIN, SUM and COUNT. AVG projected type is a type of REAL. **/
        case Attribute.INT:
            int intVal = 0;
            if (val instanceof Integer)
                intVal = (int)val;
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
                aggVal = (int)aggVal + 1;
                break;
            }
            break;
        /** Note: String only supports MAX and MIN operation since COUNT, AVG projected type is INT/REAL **/
        /** TODO: Should disallow SUM as summing string does not make sense **/
        case Attribute.STRING:
            String stringVal = val.toString();
            switch (aggType) {
            case Attribute.MAX:
                if (aggVal == null || stringVal.compareTo((String) aggVal) < 0)
                    aggVal = stringVal;
                break;
            case Attribute.MIN:
                if (aggVal == null || stringVal.compareTo((String) aggVal) > 0)
                    aggVal = stringVal;
                break;
            }
            break;
        /** Note: REAL only can use MAX, MIN, SUM and AVG. COUNT projected type is INT **/
        case Attribute.REAL:
            float floatVal = (float)val;
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
                aggVal = floatVal + (float) aggVal;
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

    public Object getAggregatedVal() {
        return aggVal;
    }
}


