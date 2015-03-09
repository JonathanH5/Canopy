package algorithm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCanopy {
	
	final static String inputPath = "src/test/resources/docword.kos.txt";
	final static String outputPath = "src/test/resources/result/canopy-output";
	final static float t1 = 0.06f;
	final static float t2 = 0.1f;
	final static int maxIterations = Integer.MAX_VALUE;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		Canopy c = new Canopy(inputPath, outputPath, t1, t2, maxIterations);
	    c.run();
		validate();
		File f = new File(outputPath);
		f.delete();
	}
	
	private void validate() throws IOException {
        ArrayList<String> list = new ArrayList<String>();
        readAllResultLines(list, outputPath);
        System.out.println("Canopy clustering ran fine. You may run k-means inside the clusters which look like:");
        System.out.println("...");
        for (String line : list.subList(5, 10)) {
            System.out.println(line);
        }
        System.out.println("...");	
	}
	
	private void readAllResultLines(List<String> target, String resultPath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(resultPath));
		String s = null;
		while ((s = reader.readLine()) != null) {
			target.add(s);
		}
		reader.close();
	}

}
