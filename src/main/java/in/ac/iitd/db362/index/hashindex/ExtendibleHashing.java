package in.ac.iitd.db362.index.hashindex;

import in.ac.iitd.db362.index.Index;
import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;


/**
 * Starter code for Extendible Hashing
 * @param <T> The type of the key.
 */
public class ExtendibleHashing<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private final Class<T> type;

    private String attribute; // attribute that we are indexing

   // Note: Do not rename the variable! You can initialize it to a different value for testing your code.
    public static int INITIAL_GLOBAL_DEPTH = 10;


    // Note: Do not rename the variable! You can initialize it to a different value for testing your code.
    public static int BUCKET_SIZE = 4;

    private int globalDepth;

    // directory is the bucket address table backed by an array of bucket pointers
    // the array offset (can be computed using the provided hashing scheme) allows accessing the bucket
    private Bucket<T>[] directory;


    /** Constructor */
    @SuppressWarnings("unchecked")
    public ExtendibleHashing(Class<T> type, String attribute) {
        this.type = type;
        this.globalDepth = INITIAL_GLOBAL_DEPTH;
        int directorySize = 1 << globalDepth;
        this.directory = new Bucket[directorySize];
        for (int i = 0; i < directorySize; i++) {
            directory[i] = new Bucket<>(globalDepth);
        }
        this.attribute = attribute;
    }


    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using Hash index on attribute " + attribute + " for operator " + node.operator);
        // TODO: Implement me!
        if (node.operator != Operator.EQUALS) {
            throw new UnsupportedOperationException("Extendible Hashing index only supports EQUALS operator");
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

    private void splitBucket(int bucketIndex) {
        Bucket<T> oldBucket = directory[bucketIndex];
        int localDepth = oldBucket.localDepth;
    
        // Create new bucket
        Bucket<T> newBucket = new Bucket<>(localDepth + 1);
        oldBucket.localDepth++;
    
        // Redistribute keys
        List<T> tempKeys = new ArrayList<>();
        List<Integer> tempValues = new ArrayList<>();
    
        for (int i = 0; i < oldBucket.size; i++) {
            tempKeys.add(oldBucket.keys[i]);
            tempValues.add(oldBucket.values[i]);
        }
        
        oldBucket.size = 0; // Clear old bucket
        newBucket.size = 0; // New bucket starts empty
    
        for (int i = 0; i < tempKeys.size(); i++) {
            insert(tempKeys.get(i), tempValues.get(i));
        }
    
        // Update directory pointers
        int newIndex = bucketIndex ^ (1 << (localDepth)); // Toggle the last bit
        directory[newIndex] = newBucket;
    }

    @Override
    public void insert(T key, int rowId) {
        // TODO: Implement insertion logic with bucket splitting and/or doubling the address table
        // System.out.println("Inserting key: " + key + " with row ID: " + rowId);
        int bucketIndex;
        if (type == Integer.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((Integer) key, globalDepth);
        } else if (type == String.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((String) key, globalDepth);
        } else if (type == Double.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((Double) key, globalDepth);
        } else if (type == LocalDate.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((LocalDate) key, globalDepth);
        } else {
            throw new UnsupportedOperationException("Hashing does not support type " + type);
        }
        Bucket<T> bucket = directory[bucketIndex];

        // Traverse overflow buckets if primary bucket is full
        while (bucket.size == BUCKET_SIZE && bucket.next != null) {
            bucket = bucket.next;
            // System.out.println("Overflow bucket");
        }

        // If there is space, insert the key
        if (bucket.size < BUCKET_SIZE) {
            bucket.keys[bucket.size] = key;
            bucket.values[bucket.size] = rowId;
            bucket.size++;
            // System.out.println("Insertion Complete!");
            return;
        }

        // If bucket is full and localDepth < globalDepth, split the bucket
        if (bucket.localDepth < globalDepth) {
            splitBucket(bucketIndex);
            insert(key, rowId); // Retry insertion after split
            return;
        }

        // If the bucket is full and cannot be split, create an overflow bucket
        if (bucket.next == null) {
            bucket.next = new Bucket<>(bucket.localDepth); // Overflow bucket has same localDepth
        }
        bucket.next.keys[bucket.next.size] = key;
        bucket.next.values[bucket.next.size] = rowId;
        bucket.next.size++;
    }


    @Override
    public boolean delete(T key) {
        // TODO: (Bonus) Implement deletion logic with bucket merging and/or shrinking the address table
        return false;
    }


    @Override
    public List<Integer> search(T key) {
        // TODO: Implement search logic

        int bucketIndex;
        if (type == Integer.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((Integer) key, globalDepth);
        } else if (type == String.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((String) key, globalDepth);
        } else if (type == Double.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((Double) key, globalDepth);
        } else if (type == LocalDate.class) {
            bucketIndex = HashingScheme.getDirectoryIndex((LocalDate) key, globalDepth);
        } else {
            throw new UnsupportedOperationException("Hashing does not support type " + type);
        }
        Bucket<T> bucket = directory[bucketIndex];

        List<Integer> result = new ArrayList<>();

        while (bucket != null) { // Traverse overflow buckets if necessary
            for (int i = 0; i < bucket.size; i++) {
                if (bucket.keys[i].equals(key)) {
                    result.add(bucket.values[i]); // Collect row IDs
                }
            }
            bucket = bucket.next; // Move to overflow bucket if present
        }

        return result;
    }

    /**
     * Note: Do not remove this function!
     * @return
     */
    public int getGlobalDepth() {
        return globalDepth;
    }

    /**
     * Note: Do not remove this function!
     * @param bucketId
     * @return
     */
    public int getLocalDepth(int bucketId) {
        return directory[bucketId].localDepth;
    }

    /**
     * Note: Do not remove this function!
     * @return
     */
    public int getBucketCount() {
        return directory.length;
    }


    /**
     * Note: Do not remove this function!
     * @return
     */
    public Bucket<T>[] getBuckets() {
        return directory;
    }

    public void printTable() {
        // TODO: You don't have to, but its good to print for small scale debugging
    }

    @Override
    public String prettyName() {
        return "Hash Index";
    }

}