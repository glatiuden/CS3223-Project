package qp.operators;

import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;


public class SortMergeJoin extends Join {
    int batchsize;
    ExternalSort leftsort, rightsort;
    ArrayList<Integer> leftindex, rightindex;
    ArrayList<Tuple> backup = new ArrayList<>();

    Batch leftbatch, rightbatch, outbatch;
    int lcurs, rcurs;
    int tempcurs = -1;
    boolean eos;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
    }

    @Override
    public boolean open() {
        int tupleSize = schema.getTupleSize();
        this.batchsize = Batch.getPageSize() / tupleSize;

        if (batchsize < 1) {
            System.err.println("Error: Page size must be bigger than tuple size for joining.");
            return false;
        }

        for (Condition con : this.conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            this.leftindex.add(left.getSchema().indexOf(leftattr));
            this.rightindex.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eos = false;
        leftsort = new ExternalSort(left, numBuff, leftindex, "left");
        rightsort = new ExternalSort(right, numBuff, rightindex, "right");

        if (!leftsort.open() || !rightsort.open()) {
            return false;
        }

        leftbatch = leftsort.next();
        rightbatch = rightsort.next();

        return true;
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);
        while (leftbatch != null && rightbatch != null) {
            Tuple lefttuple = leftbatch.get(lcurs);
            Tuple righttuple = getRightTuple();

            if (tempcurs == -1) {
                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) < 0) {
                    lcurs++;
                    if (leftbatch != null && lcurs >= leftbatch.size()) {
                        leftbatch = leftsort.next();
                        lcurs = 0;
                    }
                    if (leftbatch == null) break;
                    lefttuple = leftbatch.get(lcurs);
                }

                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) > 0) {
                    rcurs++;
                    if (rightbatch != null && rcurs >= rightbatch.size() + backup.size()) {
                        backup.addAll(rightbatch.getTuples());
                        rightbatch = rightsort.next();
                    }
                    if (rightbatch == null) break;
                    righttuple = getRightTuple();
                }
                if (rcurs >= backup.size()) {
                    rcurs -= backup.size();
                }
                tempcurs = rcurs;
                backup.clear();
            }

            if (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) == 0) {
                outbatch.add(lefttuple.joinWith(righttuple));
                rcurs++;
                if (rightbatch != null && rcurs >= rightbatch.size() + backup.size()) {
                    backup.addAll(rightbatch.getTuples());
                    rightbatch = rightsort.next();
                }
                if (rightbatch == null) break;
                if (outbatch.isFull()) {
                    return outbatch;
                }
            } else {
                rcurs = tempcurs;
                lcurs++;
                if (leftbatch != null && lcurs >= leftbatch.size()) {
                    leftbatch = leftsort.next();
                    lcurs = 0;
                }
                if (leftbatch == null) break;
                tempcurs = -1;
            }
        }

        if (outbatch.isEmpty()) {
            close();
            return null;
        } else {
            return outbatch;
        }
    }

    private Tuple getRightTuple() {
        if (backup.size() == 0) {
            return rightbatch.get(rcurs);
        } else {
            if (rcurs < backup.size()) {
                return backup.get(rcurs);
            } else {
                return rightbatch.get(rcurs - backup.size());
            }
        }
    }

    @Override
    public boolean close() {
        leftsort.close();
        rightsort.close();
        return true;
    }
}
