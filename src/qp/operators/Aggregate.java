package qp.operators;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import qp.utils.AggregateAttribute;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * Subclass of Projection to support Aggregation Query
 **/
public class Aggregate extends Project {

    ArrayDeque<Tuple> outputTuples;
    Map<Integer, AggregateAttribute> aggregations;

    boolean isAllAggregate;     //Indicator used to differentiate if it is a pure aggregation query
    boolean isExecuted;         //Indicator to terminates the query early to avoid writing out duplicate tuples

    /**
     * Default constructor for Aggregate, which requires the same arguments as Project
     */
    public Aggregate(Operator base, ArrayList<Attribute> as, int type) {
        super(base, as, type);
        this.aggregations = new HashMap<>();
        this.outputTuples = new ArrayDeque<>();
        this.isAllAggregate = true;
        this.isExecuted = false;
    }

    /**
     * Opens the connection to the base operator
     * Also figures out what are the columns that has aggregation operation
     **/
    public boolean open() {
        /* set number of tuples per batch */
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        if (!base.open())
            return false;

        /* The following loop finds the index of the columns that are required from the base operator */
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];

        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attr.setType(baseSchema.getAttribute(index).getType());
            attrIndex[i] = index;
            if (attr.getAggType() != Attribute.NONE) {
                int attrProjectType = attr.getProjectedType();
                if (attrProjectType == Attribute.INVALID) {
                    System.out.println("Data type STRING is invalid for AVG/SUM operator.");
                    return false;
                }
                aggregations.put(index, new AggregateAttribute(index, attr.getAggType(), attr.getProjectedType()));
            } else {
                /* Is not Pure Aggregate Query */
                isAllAggregate = false;
            }
        }
        return true;
    }

    public Batch next() {
        if (!isExecuted) {
            while ((inbatch = base.next()) != null) {
                while (!inbatch.isEmpty()) {
                    Tuple tuple = inbatch.removeFirst();
                    for (AggregateAttribute aggAttr : aggregations.values()) {
                        aggAttr.setAggVal(tuple);
                    }
                    outputTuples.add(tuple);
                }
            }
            return processAgg();
        }
        return null;
    }

    private Batch processAgg() {
        if (outputTuples.isEmpty()) {
            base.close();
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull() && !outputTuples.isEmpty()) {
            Tuple tuple = outputTuples.removeFirst();
            ArrayList<Object> present = new ArrayList<>();
            for (int i = 0; i < attrset.size(); i++) {
                Attribute attr = attrset.get(i);
                int index = attrIndex[i];
                if (attr.getAggType() == Attribute.NONE) {
                    present.add(tuple.dataAt(index));
                } else {
                    present.add(aggregations.get(index).getAggVal());
                }
            }

            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
            if (isAllAggregate) {
                isExecuted = base.close();
                break;
            }
        }
        return outbatch;
    }
}
