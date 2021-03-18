/**
 * To project out the required attributes from the result
 **/

package qp.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import qp.utils.AggregateAttribute;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    Batch inbatch;
    Batch outbatch;
    int[] attrIndex;

    boolean isAggregation;
    boolean isExecuted;

    Aggregate aggregate;
    Tuple previousTuple;
    List<AggregateAttribute> aaList;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.isAggregation = false;
        this.previousTuple = null;
        this.isExecuted = false;
        this.aaList = new ArrayList<>();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
            if (attr.getAggType() != Attribute.NONE) {
                attr.setType(baseSchema.getAttribute(index).getType());
                isAggregation = true;

                int attrProjectType = attr.getProjectedType();
                if (attrProjectType == Attribute.INVALID) {
                    System.out.println("Data type STRING is invalid for AVG/SUM operator.");
                    return false;
                }

                aaList.add(new AggregateAttribute(index, attr.getAggType(), attrProjectType));
            }
        }

        if (isAggregation) {
            aggregate = new Aggregate(base, attrset, tuplesize, attrIndex, aaList);
            aggregate.open();
        }

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = isAggregation ? aggregate.next() : base.next();

        if (inbatch == null) {
            return null;
        }

        if (!isExecuted) {
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.get(i);
                ArrayList<Object> present = new ArrayList<>();
                for (int j = 0; j < attrset.size(); j++) {
                    Attribute attr = attrset.get(j);
                    int index = attrIndex[j];
                    int aggType = attr.getAggType();
                    if (aggType != Attribute.NONE) {
                        //Output Minimum Tuples if it is any of the aggregation type below
                        //Max and Min is handled by Aggregation.java itself
                        isExecuted = aggType == Attribute.SUM || aggType == Attribute.COUNT || aggType == Attribute.AVG;

                        //Search for the aggregation attribute by matching aggType and attrIndex
                        Optional<AggregateAttribute> aggAttr = aaList.stream()
                                .filter(x -> x.aggType == aggType && x.attrIndex == index)
                                .findFirst();

                        if (aggAttr.isPresent()) {
                            //If aggregation is present, then we retrieve the data which are appended to the end of the columns
                            Object data = basetuple.dataAt(base.getSchema().getNumCols() + aaList.indexOf(aggAttr.get()));
                            present.add(data);
                        }
                    } else {
                        Object data = basetuple.dataAt(attrIndex[j]);
                        present.add(data);
                    }
                }

                Tuple outtuple = new Tuple(present);
                if (isAggregation) {
                    if (!outbatch.isContains(outtuple)) { //Eliminates duplicates
                        outbatch.add(outtuple);
                    }

                    if (isExecuted && outbatch.size() == 1) {
                        break;
                    }
                } else {
                    outbatch.add(outtuple);
                }
            }
        }
        return outbatch;
    }

    /**
     * Read the next block of tuples from operator
     */
    public Batch getBlock(int sizeofblock) {
        outbatch = new Batch(sizeofblock);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
