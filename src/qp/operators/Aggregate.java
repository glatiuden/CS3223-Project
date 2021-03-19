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
 * This class functions as a Helper Class to support the Aggregate Computation
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
     * Default constructor for Aggregate, which is instantiated from {@code Project.java}
     *
     * @param base         Base Operator
     * @param as           Projection's Attribute Set
     * @param tuplesize    Tuple Size
     * @param attrIndex    Projection's Attribute Index
     * @param aggrAttrList List of {@code AggregateAttribute}
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
     * Opens the connection to the base operator, loop through the tuples to compute the aggregation value.
     * Computed value are stored and updated as an attribute in {@code AggregateAttribute}.
     * The tuples are then copied over to input buffer - {@code inbatch} for processing.
     **/
    @Override
    public boolean open() {
        batchsize = Batch.getPageSize() / tuplesize;
        inbatch = new Batch(batchsize);

        while ((tempbatch = base.next()) != null) {
            while (!tempbatch.isEmpty()) {
                Tuple tuple = tempbatch.removeFirst();
                for (AggregateAttribute aggAttr : aggrAttrList) {
                    aggAttr.setAggVal(tuple);
                }
                inbatch.add(tuple);
            }
        }
        return true;
    }

    /**
     * Read next tuple from operator
     * It keeps the original tuple's data and appending the aggregate values as new columns to the end of the tuples
     * Performs until all the inbatch of tuples are processed.
     */
    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (inbatch.isEmpty()) {
            close();
            return null;
        }

        while (!outbatch.isFull() && !inbatch.isEmpty()) {
            Tuple tuple = inbatch.removeFirst();
            /* Extract out the Attribute Column Name and store in a list.
               This is then used later to obtain the object index in the data's ArrayList */
            List<String> attList = base.getSchema().getAttList().stream().map(Attribute::getColName)
                    .collect(Collectors.toList());

            // Retain the original tuple's data
            ArrayList<Object> present = new ArrayList<>(tuple.data());

            // Flag to indicate whether to write the output to the output buffer
            boolean output = true;

            // Append the aggregated value to the end of the tuple
            for (int j = 0; j < attrset.size(); j++) {
                Attribute attr = attrset.get(j);
                if (attr.getAggType() != Attribute.NONE) {
                    int aggType = attr.getAggType();
                    int index = attrIndex[j];

                    /* Obtain the AggregateAttribute that has the exact same attribute index and aggregation type */
                    Optional<AggregateAttribute> aggAttr = aggrAttrList.stream()
                            .filter(x -> x.aggType == aggType && x.attrIndex == index)
                            .findFirst();

                    /* If the AggregateAttribute exist, append it to the data's ArrayList */
                    if (aggAttr.isPresent()) {
                        Object data = aggAttr.get().getAggVal();
                        present.add(data);
                    }
                }

                // If we have reached the end of columns population process, decides which tuple to be output
                // There should be only one final output only.
                if (j == attrset.size() - 1) {
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
                            // If there is a MAX operator, then we output the non-aggregated column that is from MAX.
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
                if (!outbatch.isContains(outtuple)) { // Only unique tuples to be written out
                    outbatch.add(outtuple);
                }
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }
}
