package qp.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import qp.utils.AggregateAttribute;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Helper class to support Aggregation Computation
 **/
public class Aggregate extends Operator {
    Operator base;                          // Base table to project
    ArrayList<Attribute> attrset;           // Set of attributes to project
    Batch inbatch;                          // Buffer page for input
    Batch outbatch;                         // Buffer page for output
    Batch tempbatch;                        // Temporary buffer page for reading and populating aggregated values
    int[] attrIndex;                        // Set of attributes index to aggregate
    int tuplesize;                          // Size of tuple
    int batchsize;                          // Number of tuples per out batch
    List<AggregateAttribute> aggrAttrList;  // ArrayList of aggregated values

    /**
     * Default constructor for Aggregate, instantiated from Project
     */
    public Aggregate(Operator base, ArrayList<Attribute> as, int tuplesize, int[] attrIndex,
                     List<AggregateAttribute> aggrAttrList) {
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
     * Computes the aggregation value
     **/
    @Override
    public boolean open() {
        batchsize = Batch.getPageSize() / tuplesize;
        inbatch = new Batch(batchsize);

        while ((tempbatch = base.next()) != null) {
            while (!tempbatch.isEmpty()) {
                Tuple tuple = tempbatch.removeFirst();
                for (AggregateAttribute aggAttr: aggrAttrList) {
                    aggAttr.setAggVal(tuple);
                }
                inbatch.add(tuple);
            }
        }

        return true;
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (inbatch.isEmpty()) {
            close();
            return null;
        }

        while (!outbatch.isFull() && !inbatch.isEmpty()) {
            Tuple tuple = inbatch.removeFirst();
            // Extract out the attribute in list
            List<String> attList = base.getSchema().getAttList().stream().map(Attribute::getColName)
                    .collect(Collectors.toList());
            // Retain the original tuple's data
            ArrayList<Object> present = new ArrayList<>(tuple.data());
            boolean output = true;

            // Append the aggregated value to the end of the tuple
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
                        present.add(data);
                    }
                }

                // If we have reached the end of column population, decides which tuple to be output
                if (j == attrset.size() - 1) {
                    // There should be only one final output
                    // If there is a MIN operator, then we output the non-aggregated column that is from MIN.
                    if (aggrAttrList.stream().anyMatch(x -> x.aggType == Attribute.MIN || x.aggType == Attribute.MAX)) {
                        Optional<AggregateAttribute> min = aggrAttrList.stream().filter(x -> x.aggType == Attribute.MIN).findFirst();
                        if (min.isPresent()) {
                            AggregateAttribute minVal = min.get();
                            int baseArrIndex = attList.indexOf(minVal.colName);
                            if (!Objects.equals(present.get(baseArrIndex), minVal.getAggVal())) {
                                output = false;
                            }
                        } else {
                            Optional<AggregateAttribute> max = aggrAttrList.stream().filter(x -> x.aggType == Attribute.MAX).findFirst();
                            if (max.isPresent()) {
                                AggregateAttribute maxVal = max.get();
                                int baseArrIndex = attList.indexOf(maxVal.colName);
                                if (!Objects.equals(present.get(baseArrIndex), maxVal.getAggVal())) {
                                    output = false;
                                }
                            }
                        }
                    } else {
                        // Else for any other aggregation operator, just output the first tuple
                        output = outbatch.size() == 0;
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
