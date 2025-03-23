package in.ac.iitd.db362.index;

import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Starter code for a BitMap Index
 * Bitmap indexes are typically used for equality queries and rely on a BitSet.
 *
 * @param <T> The type of the key.
 */
public class BitmapIndex<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private String attribute;
    private int maxRowId;

    private Map<T, int[]> bitmaps;

    /**
     * Constructor
     * @param attribute
     * @param maxRowId
     */
    public BitmapIndex(String attribute, int maxRowId) {
        this.attribute = attribute;
        this.maxRowId = maxRowId;
        bitmaps = new HashMap<>();
    }

    /**
     * Create a empty bitmap for a given key
     * @param key
     */
    private void createBitmapForKey(T key) {
        int arraySize = (maxRowId + 31) / 32;
        bitmaps.putIfAbsent(key, new int[arraySize]);
    }


    /**
     * This has been done for you.
     * @param key The attribute value.
     * @param rowId The row ID associated with the key.
     */
    public void insert(T key, int rowId) {
        createBitmapForKey(key);
        int index = rowId / 32;
        int bitPosition = rowId % 32;
        bitmaps.get(key)[index] |= (1 << bitPosition);
    }


    @Override
    /**
     * This is only for completeness. Although one can delete a key, it will mess up rowIds
     * If a record is deleted, then an unset bit may lead to ambiguity (is false vs not exists)
     */
    public boolean delete(T key) {
        return false;
    }


    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using Bitmap index on attribute " + attribute + " for operator " + node.operator);
        // TODO: implement me
        if (node.isLeaf()) {
            // Leaf node → Simple predicate (e.g., department = HR)
            return search((T) node.value);
        }

        // Get results from left and right children (if they exist)
        List<Integer> leftResult = node.left != null ? evaluate(node.left) : new ArrayList<>();
        List<Integer> rightResult = node.right != null ? evaluate(node.right) : new ArrayList<>();

        Set<Integer> resultSet = new HashSet<>();

        switch (node.operator) {
            case "AND":
                resultSet.addAll(leftResult);
                resultSet.retainAll(rightResult); // Intersection
                break;
            case "OR":
                resultSet.addAll(leftResult);
                resultSet.addAll(rightResult); // Union
                break;
            case "NOT":
                Set<Integer> allRows = new HashSet<>();
                for (int i = 0; i < maxRowId; i++) {
                    allRows.add(i);
                }
                allRows.removeAll(leftResult); // Complement
                resultSet = allRows;
                break;
            default:
                throw new UnsupportedOperationException("Unknown operator: " + node.operator);
        }

        return new ArrayList<>(resultSet);
    }

    @Override
    public List<Integer> search(T key) {
    //TODO: Implement me!
        List<Integer> result = new ArrayList<>();
    
        // Get the bitmap for the given key
        int[] bitmapArray = bitmaps.get(key);
        if (bitmapArray == null) {
            return result; // Key does not exist in index
        }

        // Iterate over the bit array
        for (int i = 0; i < bitmapArray.length; i++) {
            int bitmap = bitmapArray[i]; // Get the integer storing bits
            for (int j = 0; j < 32; j++) {
                if ((bitmap & (1 << j)) != 0) { // Check if j-th bit is set
                    result.add(i * 32 + j); // Convert bit position to row ID
                }
            }
        }

        return result;
    }

    @Override
    public String prettyName() {
        return "BitMap Index";
    }
}