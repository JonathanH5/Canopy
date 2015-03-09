package algorithm;

import java.util.*;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.*;
import org.apache.flink.util.Collector;

/**
 * Canopy clustering implementation for Apache Flink 0.8.1.
 */
public class Canopy{

    final String inputPath;

    final String outputPath;

    final float t1;

    final float t2;

    final int maxIterations;

    public Canopy(final String inputPath, final String outputPath, final float t1, final float t2, final int maxIterations) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.t1 = t1;
        this.t2 = t2;
        this.maxIterations = maxIterations;
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Data Structures ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * Represents a Tuple5(docId, isCenter, isInSomeT2, "canopyCenters", "words") Purpose: Reduces
     * the amount of generics usage in the doRun method.
     */
    public static class Document extends Tuple5<Integer, Boolean, Boolean, String, String> {

		private static final long serialVersionUID = -3183806920230373026L;

        public Document() {
            // default constructor needed for instantiation during serialization
        }

        public Document(Integer docId, Boolean isCenter, Boolean isInSomeT2, String canopyCenters, String words) {
            super(docId, isCenter, isInSomeT2, canopyCenters, words);
        }
    }


    // --------------------------------------------------------------------------------------------
    // --------------------------------- Algorithm ------------------------------------------------
    // --------------------------------------------------------------------------------------------

    public void run() throws Exception {
        doRun(inputPath, outputPath, t1, t2, maxIterations);
    }

    /**
     * runs the canopy clustering program with the given parameters
     * 
     * @param inputPath Input path
     * @param outputPath Output path
     * @param t1 Threshold T1 (Less similarity)
     * @param t2 Threshold T2 (More similarity)
     * @throws Exception
     */
    private static void doRun(String inputPath, String outputPath, float t1, float t2, int maxIterations) throws Exception {

        // setup environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setFloat("t1", t1);
        conf.setFloat("t2", t2);

        FilterFunction<Document> unmarkedFilter = new FilterUnmarked();

        // read the dataset and transform it to docs
        DataSet<Document> docs = env.readTextFile(inputPath).flatMap(new MassageBOW()).groupBy(0).reduceGroup(new DocumentReducer());
        // loop
        IterativeDataSet<Document> loop = docs.iterate(maxIterations);
        DataSet<Document> unmarked = loop.filter(unmarkedFilter);
        DataSet<Document> centerX = unmarked.reduce(new PickFirst());
        DataSet<Document> iterationX = loop.map(new MapToCenter()).withParameters(conf).withBroadcastSet(centerX, "center");
        DataSet<Document> loopResult = loop.closeWith(iterationX, unmarked);
        // create canopies
        DataSet<Tuple1<String>> canopies = loopResult.flatMap(new CreateInverted()).groupBy(0).reduceGroup(new CreateCanopies());

        canopies.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute canopy
        env.execute("Canopy");
    }

    /**
     * Maps lines of a uci bag-of-words dataset to a Tuple2. Ignores the word frequencies because
     * the distance metric for canopy clustering should be cheap (jaccard).
     */
    public static class MassageBOW implements FlatMapFunction<String, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 6766848442727620434L;

		@Override
        public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
            String[] splits = value.split(" ");
            if (splits.length < 2) {
                return;
            }
            out.collect(new Tuple2<Integer, String>(Integer.valueOf(splits[0]), splits[1]));
        }
    }

    /**
     * Creates Documents: Tuple5(docId, isCenter, isInSomeT2, "canopyCenters", "words")
     */
    public static class DocumentReducer implements GroupReduceFunction<Tuple2<Integer, String>, Document> {

		private static final long serialVersionUID = 6934852144495454374L;

		@Override
        public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Document> out) throws Exception {
        	Iterator<Tuple2<Integer, String>> i = values.iterator();
            Tuple2<Integer, String> first = i.next();
            Integer docId = first.f0;
            StringBuilder builder = new StringBuilder(first.f1);
            while (i.hasNext()) {
                builder.append("-").append(i.next().f1);
            }
            out.collect(new Document(docId, false, false, "", builder.toString()));
        }

    }

    /**
     * Filter all documents that are not in some T1 or T2 threshold of a canopy center.
     */
    public static class FilterUnmarked implements FilterFunction<Document> {

		private static final long serialVersionUID = -6428646876083176234L;

		@Override
        public boolean filter(Document value) throws Exception {
            return !value.f2 && value.f3.isEmpty();
        }
    }

    /**
     * Reduces a DataSet<Document> to a single value (limit 1).
     */
    public static class PickFirst implements ReduceFunction<Document> {

		private static final long serialVersionUID = -3071917770934403009L;

		@Override
        public Document reduce(Document v1, Document v2) throws Exception {
            return v1;
        }
    }

    /**
     * Marks each document with the values "isCenter", "isInSomeT2" and updates the "canopyIds".
     */
    @FunctionAnnotation.ConstantFieldsExcept("1,2,3")
    public static class MapToCenter extends AbstractRichFunction implements MapFunction<Document, Document> {

		private static final long serialVersionUID = -4673454036292517266L;

		private Document center;

        private float t1;

        private float t2;

        @Override
        public Document map(Document value) throws Exception {
            if (center != null) {
                final float similarity = computeJaccard(value.f4, center.f4);
                final boolean isEqual = value.f0.equals(center.f0);
                value.f1 = isEqual;
                value.f2 = isEqual || similarity > t2;
                if (!value.f3.contains(center.f0.toString() + ";") && (similarity > t1 || isEqual)) {
                    value.f3 += center.f0.toString() + ";";
                }
            }
            return value;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<Object> list = (List<Object>) getRuntimeContext().getBroadcastVariable("center");
            if (!list.isEmpty()) {
                center = (Document) list.get(0);
                t1 = parameters.getFloat("t1", 0.15f);
                t2 = parameters.getFloat("t2", 0.3f);
            }
        }
    }

    /**
     * Creates new tuples by splitting the "canopyIds" of the documents.
     */
    public static class CreateInverted implements FlatMapFunction<Document, Tuple2<String, Integer>> {

		private static final long serialVersionUID = -5116243761845158950L;

		@Override
        public void flatMap(Document value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String clusterID : value.f3.split(";")) {
                out.collect(new Tuple2<String, Integer>(clusterID, value.f0));
            }
        }
    }

    /**
     * Creates a comma separated string of canopies.
     */
    public static class CreateCanopies implements GroupReduceFunction<Tuple2<String, Integer>, Tuple1<String>> {

		private static final long serialVersionUID = 7768087934987707751L;

		@Override
        public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple1<String>> out) throws Exception {
        	Iterator<Tuple2<String, Integer>> i = values.iterator();
            StringBuilder builder = new StringBuilder();
            while (i.hasNext()) {
                builder.append(i.next().f1);
                if (i.hasNext()) {
                    builder.append(",");
                }
            }
            out.collect(new Tuple1<String>(builder.toString()));
        }
    }


    // --------------------------------------------------------------------------------------------
    // ------------------------------ Utility methods ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * Computes the Jaccard coefficient from two strings. Ideally from two lists but that cant be
     * done in flink right now.
     * 
     * @return the similarity of two documents.
     */
    public static float computeJaccard(String first, String second) {
        float joint = 0;
        List<String> firstList = Arrays.asList(first.split("-"));
        List<String> secondList = Arrays.asList(second.split("-"));

        for (String s : firstList) {
            if (secondList.contains(s))
                joint++;
        }

        return (joint / ((float) firstList.size() + (float) secondList.size() - joint));
    }
}
