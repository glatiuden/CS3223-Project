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
        return outbatch;
    }

    @Override
    public boolean close() {
        leftsort.close();
        rightsort.close();
        return true;
    }
}
