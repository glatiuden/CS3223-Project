
/**
 * This is main driver program of the query processor
 **/

import qp.operators.Debug;
import qp.operators.Operator;
import qp.operators.Project;
import qp.optimizer.BufferManager;
import qp.optimizer.PlanCost;
import qp.optimizer.RandomOptimizer;
import qp.parser.Scaner;
import qp.parser.parser;
import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class QueryMain {

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile> <pagesize> <numbuffer>");
            System.exit(1);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Batch.setPageSize(getPageSize(args, in));

        SQLQuery sqlquery = getSQLQuery(args[0]);
        configureBufferManager(sqlquery.getNumJoin(), args, in);

        Operator root = getQueryPlan(sqlquery);
        printFinalPlan(root, args, in);
        executeQuery(root, args[1]);
    }

    /**
     * Get page size from arguments, if not provided request as input
     **/
    private static int getPageSize(String[] args, BufferedReader in) {
        int pagesize = -1;
        if (args.length < 3) {
            /** Enter the number of bytes per page **/
            System.out.println("enter the number of bytes per page");
            try {
                String temp = in.readLine();
                pagesize = Integer.parseInt(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else pagesize = Integer.parseInt(args[2]);
        return pagesize;
    }

    /**
     * Parse query from query file
     **/
    public static SQLQuery getSQLQuery(String queryfile) {
        /** Read query file **/
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }

        /** Scan the query **/
        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);

        /** Parse the query **/
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occured while parsing");
            System.exit(1);
        }

        return p.getSQLQuery();
    }

    /**
     * If there are joins then assigns buffers to each join operator while preparing the plan.
     * As buffer manager is not implemented, just input the number of buffers available.
     **/
    private static void configureBufferManager(int numJoin, String[] args, BufferedReader in) {
        if (numJoin != 0) {
            int numBuff = 1000;
            if (args.length < 4) {
                System.out.println("enter the number of buffers available");
                try {
                    String temp = in.readLine();
                    numBuff = Integer.parseInt(temp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else numBuff = Integer.parseInt(args[3]);
            BufferManager bm = new BufferManager(numBuff, numJoin);
        }

        /** Check the number of buffers available is enough or not **/
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per join operator ");
            System.exit(1);
        }
    }

    /**
     * Run optimiser and get the final query plan as an Operator
     **/
    public static Operator getQueryPlan(SQLQuery sqlquery) {
        Operator root = null;

        RandomOptimizer optimizer = new RandomOptimizer(sqlquery);
        Operator planroot = optimizer.getOptimizedPlan();

        if (planroot == null) {
            System.out.println("DPOptimizer: query plan is null");
            System.exit(1);
        }

        root = RandomOptimizer.makeExecPlan(planroot);

        return root;
    }

    /**
     * Print final Plan and ask user whether to continue
     **/
    private static void printFinalPlan(Operator root, String[] args, BufferedReader in) {
        System.out.println("----------------------Execution Plan----------------");
        if(root.isOrderByQuery()) {
            System.out.print("Orderby(");
        }
        if(root.isDistinct()){
            System.out.print("Distinct(");
        }
        Debug.PPrint(root);
        if(root.isDistinct()){
            System.out.print(")");
        }

        if(root.isOrderByQuery()) {
            System.out.print(", <");
            
            for (int k = 0; k < root.getOrderByList().size(); k++) {
                System.out.print(root.getOrderByList().get(k));
                if (k + 1 != root.getOrderByList().size())
                    System.out.print(", ");
            }
            System.out.print(">)");
        }
        PlanCost pc = new PlanCost();
        System.out.printf("\nExpected cost: %d\n", pc.getCost(root));
        if (args.length < 5) {
            /** Ask user whether to continue execution of the program **/
            System.out.println("enter 1 to continue, 0 to abort ");
            try {
                String temp = in.readLine();
                int flag = Integer.parseInt(temp);
                if (flag == 0) {
                    System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Execute query and print run statistics
     **/
    public static double executeQuery(Operator root, String resultfile) {
        long starttime = System.currentTimeMillis();
        if (root.open() == false) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultfile);
            System.exit(1);
        }

        /** Print the schema of the result **/
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);


        ArrayList<Integer> indexList = new ArrayList<>();
        ArrayList<Attribute> orderbyList = root.getOrderByList();

        /** Get list of indexes that points to the attributes to be sorted by**/
        if (orderbyList.size() > 0) {
            for (Attribute a : orderbyList) {
                for (int i = 0; i < schema.getAttList().size(); i++) {
                    if(a.toString().equals(schema.getAttList().get(i).toString()))
                        indexList.add(i); 
                }
            }
        }

        /** Print each tuple in the result **/
        Batch resultbatch;
        ArrayList<Tuple> printedTuples = new ArrayList<>();
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); ++i) {
                Tuple resultTuple = resultbatch.get(i);

                /** Check if tuple is distinct before printing **/
                if (root.isDistinct()) {
                    if(printedTuples.isEmpty()){
                        printedTuples.add(resultTuple);
                    }else{
                        boolean isSame = false;
                        for(Tuple printedTuple: printedTuples){
                            if (resultTuple.isEquals(printedTuple)) {
                                isSame = true;
                                break;
                            }
                        }
                        if(!isSame){
                            printedTuples.add(resultTuple);
                        }
                    }
                }else{
                    printedTuples.add(resultTuple);
                }
            }
        }

        
        /** Sort the tuples based on the attributes (indexes pointing to attributes) **/
        if (orderbyList.size() > 0) {
            Collections.sort(printedTuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    int compareResult = Tuple.compareTuples(t1, t2, indexList, indexList);
                    if (compareResult != 0) {
                        int multiplier = (root.IsDesc()) ? -1 : 1;
                        return multiplier * compareResult;
                    }
                    return 0;
                }
            });
        }

        // Print all tuples 
        for(Tuple t : printedTuples) {
            printTuple(t);
        }
        root.close();
        out.close();

        long endtime = System.currentTimeMillis();
        double executiontime = (endtime - starttime) / 1000.0;
        System.out.println("Execution time = " + executiontime);
        return executiontime;
    }

    protected static void printSchema(Schema schema) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};
        for (int i = 0; i < numAtts; ++i) {
            Attribute attr = schema.getAttribute(i);
            int aggregate = attr.getAggType();
            String tabname = attr.getTabName();
            String colname = attr.getColName();
            if (aggregate == 0) {
                out.print(tabname + "." + colname + "  ");
            } else {
                out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
            }
        }
        out.println();
    }

    protected static void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; ++i) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else if (data == null) {
                out.print("-NULL-\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }
}