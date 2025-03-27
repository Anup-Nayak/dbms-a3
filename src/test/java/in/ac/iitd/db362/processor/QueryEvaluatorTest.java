package in.ac.iitd.db362.processor;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.io.CSVParser;
import in.ac.iitd.db362.parser.Parser;
import in.ac.iitd.db362.parser.QueryNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryEvaluatorTest {

    String filePath;

    int maxRowId;

    Catalog catalog;

    @BeforeEach
    public void setUp() {
        //Locate the test file
        URL url = this.getClass().getResource("/purchase-data.csv");
        File file = new File(url.getFile());
        assertTrue(file.exists());
        filePath = file.toPath().toString();
        catalog = Catalog.getInstance();
        maxRowId = 199;
    }

    @Test
    void test1() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("customer_id", Collections.singletonList("BPlusTree"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "customer_id = 10";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = List.of(9);

        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    void test2() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("customer_id", Collections.singletonList("BPlusTree"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "customer_id < 15";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(0,1,2,3,4,5,6,7,8,9,10,11,12,13);

        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test3() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("customer_id", Collections.singletonList("BPlusTree"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "customer_id > 194";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(194, 195, 196, 197, 198, 199);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test4() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("purchase_amount", Collections.singletonList("BPlusTree"));
        indexesToCreate.put("store_id", Collections.singletonList("BPlusTree"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "store_id = 10 AND purchase_amount < 10000";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = List.of(120);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test5() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("purchase_amount", Collections.singletonList("BPlusTree"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "7000 < purchase_amount < 10000";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(11, 87, 118, 120, 121, 136, 138, 173);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test6() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("purchase_amount", Collections.singletonList("BPlusTree"));
        indexesToCreate.put("store_id", Collections.singletonList("Hash"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "store_id = 27 AND (7000 < purchase_amount < 10000)";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(11, 173);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test7() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("purchase_amount", Collections.singletonList("BPlusTree"));
        indexesToCreate.put("store_id", Collections.singletonList("Bitmap"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "store_id = 27 AND (7000 < purchase_amount < 10000)";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(11, 173);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test8() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("customer_rating", Collections.singletonList("BPlusTree"));
        indexesToCreate.put("product_category", Collections.singletonList("Hash"));


        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "customer_rating > 4 AND (product_category = Furniture OR product_category = Clothing)";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(1, 7, 32, 73, 88, 89, 91, 101, 110, 113, 117, 128, 136, 154, 181, 183);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test9() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("product_category", Collections.singletonList("Hash"));
        indexesToCreate.put("purchase_amount", Collections.singletonList("BPlusTree"));
        indexesToCreate.put("store_id", Collections.singletonList("Bitmap"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "(NOT (store_id = 5)) AND product_category = Books AND purchase_amount < 50000";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(11, 15, 21, 43, 52, 77, 85, 103, 125, 187);
        assertEquals(expected, results, "Incorrect results");
    }

    @Test
    public void test10() throws IOException {
        catalog.clear();

        // Create a B+Tree on customer id
        Map<String, List<String>> indexesToCreate = new HashMap<>();
        indexesToCreate.put("product_category", Collections.singletonList("Bitmap"));
        indexesToCreate.put("store_id", Collections.singletonList("Hash"));

        CSVParser.parseCSV(filePath, ",", catalog, indexesToCreate, maxRowId);

        String query = "(NOT (NOT (store_id = 25))) AND (NOT (product_category = Toys))";
        List<Integer> results = evaluateQuery(query);
        List<Integer> expected = Arrays.asList(54, 58, 143, 170, 198);
        assertEquals(expected, results, "Incorrect results");
    }


    //Some helpers
    private List<Integer> evaluateQuery(String query) {
        QueryNode queryNode = Parser.parse(query);
        List<Integer> rowIds = QueryEvaluator.evaluateQuery(queryNode, maxRowId);
        Collections.sort(rowIds);
        return rowIds;
    }

}
