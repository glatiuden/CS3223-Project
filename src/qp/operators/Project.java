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

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;                    // Buffer page for input
    Batch outbatch;                   // Buffer page for output

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;                       // Set of attributes index to aggregate

    boolean isAggregation;                 // Flag used to differentiate Project and Aggregate
    Aggregate aggregate;                   // Aggregate attribute to instantiate the helper class
    List<AggregateAttribute> aggrAttrList; // List of AggregationAttribute to find index from the incoming Aggregate Batch file

    /**
     * Default Constructor for Project which performs the Projection Query.
     * {@code isAggregation} is by default false.
     * @param base  Base Operator
     * @param as    Projection's Attribute Set
     * @param type  Type of {@code Operator}
     */
    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.isAggregation = false;
        this.aggrAttrList = new ArrayList<>();
    }

    /**
     * Getter for Base
     */
    public Operator getBase() {
        return base;
    }

    /**
     * Setter for Base
     */
    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Getter for AttrSet
     */
    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * Also figures out what are the columns to be projected from the base operator
     * If the column has an Aggregate Type, performs the pre-processing for Aggregation Query.
     **/
    public boolean open() {
        /* set number of tuples per batch */
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /* The following loop finds the index of the columns that are required from the base operator */
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
            /* The attribute has an aggregation type */
            if (attr.getAggType() != Attribute.NONE) {
                /* Sets the attribute type */
                attr.setType(baseSchema.getAttribute(index).getType());
                isAggregation = true; // Set the flag to true to perform the correct next() method
                int attrProjectType = attr.getProjectedType();
                if (attrProjectType == Attribute.INVALID) { // Invalid operation on this data type
                    System.out.println("Data type STRING is invalid for AVG/SUM operator.");
                    return false;
                }
                // Creates a new AggregateAttribute, added to a List which to be processed.
                aggrAttrList.add(new AggregateAttribute(index, attr.getAggType(), attrProjectType, attr.getColName()));
            }
        }

        if (isAggregation) {
            aggregate = new Aggregate(base, attrset, tuplesize, attrIndex, aggrAttrList);
            aggregate.open(); // Performs aggregation computation
        }

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /* all the tuples in the inbuffer goes to the output buffer */
        /* If this is an Aggregation Operation, calls the input buffer pages from Aggregate instead */
        inbatch = isAggregation ? aggregate.next() : base.next();

        if (inbatch == null) {
            return null;
        }

        // Performs aggregation or projection
        // Isolating both tuple calls to avoid disruption if there is any failure in processing.
        if (isAggregation) {
            aggregationNext();
        } else {
            projectNext();
        }
        return outbatch;
    }

    /**
     * Read next tuple from operator
     */
    private void projectNext() {
        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }
    }

    /**
     * Read next tuple from operator and determine which column and tuple to be written out.
     */
    private void aggregationNext() {
        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Attribute attr = attrset.get(j);
                int index = attrIndex[j];
                int aggType = attr.getAggType();
                /* The attribute has an aggregation type */
                if (aggType != Attribute.NONE) {
                    // Search for a AggregateAttribute that has a matching aggType and attrIndex
                    Optional<AggregateAttribute> aggAttr = aggrAttrList.stream()
                            .filter(x -> x.aggType == aggType && x.attrIndex == index)
                            .findFirst();

                    if (aggAttr.isPresent()) {
                        // If aggregation is present, then we retrieve the data which are appended to the end of the columns
                        Object data = basetuple.dataAt(base.getSchema().getNumCols() + aggrAttrList.indexOf(aggAttr.get()));
                        present.add(data);
                    }
                } else {
                    // Else, add the requested attribute normally
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
            }

            Tuple outtuple = new Tuple(present);
            if (!outbatch.isContains(outtuple)) { // Eliminates duplicates
                outbatch.add(outtuple);
            }
        }
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
        // If aggregate is not null, close it too.
        if (aggregate != null)
            aggregate.close();
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
