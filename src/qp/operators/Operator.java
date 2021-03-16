/**
 * This is base class for all the operators
 **/
package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Attribute;

import java.util.ArrayList;

public class Operator {

    int optype;             // Whether it is OpType.SELECT/ Optype.PROJECT/OpType.JOIN
    Schema schema;          // Schema of the result at this operator
    boolean isDistinct;     // Whether the query is distinct, false by default
    ArrayList<Attribute> orderbyList = new ArrayList<Attribute>(); // List of attributes to be sorted by
    boolean isDesc;         // Whether to be sort by descending, false by default

    public Operator(int type) {
        this.optype = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schm) {
        this.schema = schm;
    }

    public int getOpType() {
        return optype;
    }

    public void setOpType(int type) {
        this.optype = type;
    }

    public boolean isDistinct(){
        return isDistinct;
    }

    public void setIsDistinct(boolean flag){
        this.isDistinct = flag;
    }

    public boolean isOrderByQuery() {
        return orderbyList.size() > 0;
    }

    public ArrayList<Attribute> getOrderByList() {
        return orderbyList;
    }
    
    public void setOrderByList(ArrayList<Attribute> alist) {
        orderbyList = alist;
    }

    public void setIsDesc(boolean value) {
        isDesc = value;
    }

    public boolean IsDesc() {
        return isDesc;
    }

    public boolean open() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return true;
    }

    public Batch next() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return null;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        return new Operator(optype);
    }

}










