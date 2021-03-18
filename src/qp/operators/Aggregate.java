package qp.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import qp.utils.AggregateAttribute;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Subclass of Projection to support Aggregation Query
 **/
public class Aggregate extends Operator {
    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    Batch inbatch;
    Batch outbatch;
    int[] attrIndex;
    int tuplesize;
    List<AggregateAttribute> aggrAttrList;
    Batch outputbatch;

    /**
     * Default constructor for Aggregate, which requires the same arguments as Project
     */
    public Aggregate(Operator base, ArrayList<Attribute> as, int tuplesize, int[] attrIndex, List<AggregateAttribute> aggrAttrList) {
        super(OpType.AGGREGATE);
        this.base = base;
        this.attrset = as;
        this.tuplesize = tuplesize;
        this.attrIndex = attrIndex;
        this.aggrAttrList = aggrAttrList;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     * Also figures out what are the columns that has aggregation operation
     **/
    public boolean open() {
        outputbatch = new Batch(Batch.getPageSize() / tuplesize);
        while ((inbatch = base.next()) != null) {
            while (!inbatch.isEmpty()) {
                Tuple tuple = inbatch.removeFirst();
                for (AggregateAttribute agg : aggrAttrList) {
                    agg.setAggVal(tuple);
                }
                outputbatch.add(tuple);
            }
        }
        return true;
    }

    public Batch next() {
        outbatch = new Batch(Batch.getPageSize() / tuplesize);
        if (outputbatch.isEmpty()) {
            base.close();
            return null;
        }

        while (!outbatch.isFull() && !outputbatch.isEmpty()) {
            Tuple tuple = outputbatch.removeFirst();
            ArrayList<Object> present = new ArrayList<>(tuple.data());
            boolean output = true;
            for (int j = 0; j < attrset.size(); j++) {
                Attribute attr = attrset.get(j);
                if (attr.getAggType() != Attribute.NONE) {
                    int aggType = attr.getAggType();
                    int index = attrIndex[j];

                    Optional<AggregateAttribute> aggAttr = aggrAttrList.stream()
                            .filter(x -> x.aggType == aggType && x.attrIndex == index)
                            .findFirst();

                    if (aggAttr.isPresent()) {
                        Object data = aggAttr.get().getAggVal();
                        if (attrset.indexOf(attr) == attrset.size() - 1) {
                            if (aggType == Attribute.MAX || aggType == Attribute.MIN) {
                                //If only performing MAX or MIN, we can check if this tuple contains the desired tuples
                                if (!present.contains(data)) {
                                    output = false;
                                }
                            }
                        }
                        present.add(data);
                    }
                }
            }

            if (output) {
                Tuple outtuple = new Tuple(present);
                if (!outbatch.isContains(outtuple)) { //Only unique tuples
                    outbatch.add(outtuple);
                }
            }
        }
        return outbatch;
    }

    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }
}
