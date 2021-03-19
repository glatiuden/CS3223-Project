package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Helper class Block, which acts as multiple batches
 * Methods are similar to {@code Batch} except there is an additional ArrayList of batches
 */
public class Block implements Serializable {
    int maxSize;
    int pageSize;
    ArrayList<Batch> batches;
    ArrayList<Tuple> tuples;

    public Block(int numPage, int pageSize) {
        this.maxSize = numPage;
        this.pageSize = pageSize;
        this.batches = new ArrayList<>(maxSize);
        this.tuples = new ArrayList<>(maxSize * pageSize);
    }

    public ArrayList<Batch> getBatches() {
        return batches;
    }

    public void setBatches(ArrayList<Batch> batches) {
        this.batches = batches;
        for (int i = 0; i < batches.size(); i++) {
            for (int j = 0; j < batches.get(i).size(); j++) {
                tuples.add(batches.get(i).get(j));
            }
        }
    }

    public void addBatch(Batch batch) {
        if (!isFull()) {
            batches.add(batch);
            for (int i = 0; i < batch.size(); i++) {
                tuples.add(batch.get(i));
            }
        }
    }

    public ArrayList<Tuple> getTuples() {
        return tuples;
    }

    public void setTuples(ArrayList tupleList) {
        Batch batch = new Batch(pageSize);
        for (int i = 0; i < tupleList.size(); i++) {
            if (batch.isFull()) {
                batches.add(batch);
                batch = new Batch(pageSize);
            }
            batch.add((Tuple) tupleList.get(i));
            tuples.add((Tuple) tupleList.get(i));
        }

        if (!batch.isEmpty()) {
            batches.add(batch);
        }
    }

    public Tuple getTuple(int index) {
        return (Tuple) tuples.get(index);
    }

    public int getBatchSize() {
        return batches.size();
    }

    public int getTupleSize() {
        return tuples.size();
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }

    public boolean isFull() {
        return (batches.size() >= maxSize);
    }
}
