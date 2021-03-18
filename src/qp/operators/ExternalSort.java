package qp.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import qp.utils.Batch;
import qp.utils.Block;
import qp.utils.Tuple;

public class ExternalSort extends Operator {
    int batchsize, noOfBuffer, noOfAvailBuffer;
    String direction;
    Batch inbatch, outbatch;
    ArrayList<Integer> attrIndex;
    Comparator<Tuple> comparator;
    List<File> sortedFiles;
    ObjectInputStream inputStream;
    ObjectOutputStream outputStream;
    Operator base;

    public ExternalSort(Operator base, int noOfBuffer, ArrayList<Integer> attrIndex, String direction) {
        super(OpType.JOIN);
        this.base = base;
        this.noOfBuffer = noOfBuffer;
        this.noOfAvailBuffer = noOfBuffer - 1;
        this.direction = direction;
        this.attrIndex = attrIndex;
        this.comparator = (t1, t2) -> Tuple.compareTuples(t1, t2, attrIndex, attrIndex);
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    @Override
    public boolean open() {
        int tuplesize = base.getSchema().getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) {
            return false;
        }

        sortedFiles = new ArrayList<>();
        try {
            createRuns();
        } catch (IOException ex) {
            System.out.println("ERROR: " + ex.getMessage());
            return false;
        }

        if (sortedFiles.size() != 1) {
            return false;
        }

        File f = sortedFiles.get(0);
        try {
            inputStream = new ObjectInputStream(new FileInputStream(f));
        } catch (FileNotFoundException ex) {
            System.out.println("ERROR: File " + f.getName() + " not found");
        } catch (IOException ex) {
            System.out.println("ERROR: Unable to read file: " + f.getName());
        }
        return true;
    }

    private void createRuns() throws IOException {
        int noOfRuns = 0;
        inbatch = base.next();
        while (inbatch != null && !inbatch.isEmpty()) {
            Block run = new Block(noOfBuffer, batchsize);
            while (!run.isFull() && inbatch != null && !inbatch.isEmpty()) {
                run.addBatch(inbatch);
                inbatch = base.next();
            }
            noOfRuns++;

            ArrayList<Tuple> tuples = run.getTuples();
            tuples.sort(comparator);
            Block sortedRun = new Block(noOfBuffer, batchsize);
            sortedRun.setTuples(tuples);
            write(sortedRun, noOfRuns);
        }
        mergeRuns();
    }

    private void write(Block run, int noOfRuns) throws IOException {
        try {
            File f = new File(generateRunFileName(noOfRuns));
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(f));
            for (Batch batch : run.getBatches()) {
                out.writeObject(batch);
            }
            out.close();
            sortedFiles.add(f);
        } catch (IOException ex) {
            throw new IOException("Problem encountered when writing the file");
        }
    }

    private void mergeRuns() throws IOException {
        int noOfRuns = 0;
        List<File> result;
        while (sortedFiles.size() > 1) {
            result = new ArrayList<>();
            int noOfMergeRun = 0;

            int start;
            for (int i = 0; (start = (i * noOfAvailBuffer)) < sortedFiles.size(); i++) {
                int end = Math.min((i + 1) * noOfAvailBuffer, sortedFiles.size());
                result.add(merge(sortedFiles.subList(start, end), noOfRuns, noOfMergeRun));
                noOfMergeRun++;
            }
            sortedFiles.forEach(File::delete);
            noOfRuns++;
            sortedFiles = result;
        }
    }

    private File merge(List<File> runs, int numOfMergeRuns, int numOfMerges) throws IOException {
        int noOfRuns = runs.size();
        outbatch = new Batch(batchsize);

        if (runs.isEmpty()) {
            throw new IOException("There is no runs available.");
        }

        if (noOfRuns > noOfAvailBuffer) {
            throw new IOException("Exceed the number of available buffers (B-1)");
        }

        ArrayList<Batch> batches = new ArrayList<>();
        for (File f : runs) {
            try {
                ObjectInputStream stream = new ObjectInputStream(new FileInputStream(f));
                Batch batch;
                while ((batch = nextBatch(stream)) != null) {
                    batches.add(batch);
                }
            } catch (IOException e) {
                throw new IOException("There is a problem reading the temporary file.");
            }
        }

        /* Insert the tuples in ascending order */
        Queue<Tuple> inputTuples = new PriorityQueue<>(comparator);
        for (Batch batch : batches) {
            if (batch != null) {
                while (!batch.isEmpty()) {
                    inputTuples.add(batch.removeFirst());
                }
            }
        }

        /* Starts merging process */
        File file = new File(generateRunFileName(numOfMerges, numOfMergeRuns));
        outputStream = new ObjectOutputStream(new FileOutputStream(file, true));
        while (!inputTuples.isEmpty()) {
            Tuple currentTuple = inputTuples.poll();
            outbatch.add(currentTuple);

            /* Write out if the buffer page is full */
            if (outbatch.isFull()) {
                writeOut();
                outbatch.clear();
            }
        }

        if (!outbatch.isEmpty()) {
            writeOut();
        }

        outputStream.close();
        return file;
    }

    private void writeOut() throws IOException {
        try {
            outputStream.writeObject(outbatch);
            outputStream.reset();
        } catch (IOException ex) {
            throw new IOException("Problem writing to output file. " + ex.getMessage());
        }
    }

    private Batch nextBatch(ObjectInputStream stream) throws IOException {
        try {
            Batch batch = (Batch) stream.readObject();
            return batch.isEmpty() ? null : batch;
        } catch (ClassNotFoundException ex) {
            throw new IOException("Unable to serialize the object.");
        } catch (IOException ex) {
            return null;
        }
    }

    private String generateRunFileName(int noOfRuns) {
        return String.format("%s-SMTemp-%d", direction, noOfRuns);
    }

    private String generateRunFileName(int noOfMerge, int noOfMergeRun) {
        return String.format("%s-SMTemp-%d-%d", direction, noOfMerge, noOfMergeRun);
    }

    @Override
    public Batch next() {
        try {
            Batch batch = (Batch) inputStream.readObject();
            return batch.isEmpty() ? null : batch;
        } catch (ClassNotFoundException ex) {
            System.out.println("Unable to serialize the object.");
            return null;
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public boolean close() {
        try {
            for (File file : sortedFiles) {
                file.delete();
            }
            inputStream.close();
        } catch (IOException e) {
            System.out.println("Error in closing result file stream.");
        }
        return true;
    }
}
