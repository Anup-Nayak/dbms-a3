package in.ac.iitd.db362.index;

import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Starter code for a BitMap Index
 * Bitmap indexes are typically used for equality queries and rely on a BitSet.
 *
 * @param <T> The type of the key.
 */
public class BitmapIndex<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private final Class<T> type;

    private String attribute;
    private int maxRowId;

    private Map<T, int[]> bitmaps;

    /**
     * Constructor
     *
     * @param type
     * @param attribute
     * @param maxRowId
     */
    public BitmapIndex(Class<T> type, String attribute, int maxRowId) {
        this.type = type;
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
        // Ensure the operator is EQUALS, as bitmap indexes do not support other comparisons
        if (node.operator != Operator.EQUALS) {
            throw new UnsupportedOperationException("Bitmap index only supports EQUALS operator");
        }

        // Convert node.value to the appropriate type
        T key;
        if (type == Integer.class) {
            key = (T) Integer.valueOf(node.value);
        } else if (type == String.class) {  
            key = (T) node.value;
        } else if (type == Double.class) {
            key = (T) Double.valueOf(node.value);
        } else if (type == LocalDate.class) {
            key = (T) LocalDate.parse(node.value);
        } else {
            throw new UnsupportedOperationException("Bitmap index does not support type " + type);
        }

        // Call the search function with the converted key
        return search(key);
        
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