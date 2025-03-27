package in.ac.iitd.db362.index.bplustree;

import in.ac.iitd.db362.index.Index;
import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Starter code for BPlusTree Implementation
 * @param <T> The type of the key.
 */
public class BPlusTreeIndex<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private final Class<T> type;

    // Note: Do not rename this variable; the test cases will set this when testing. You can however initialize it with a
    // different value for testing your code.
    public static int ORDER = 10;

    // The attribute being indexed
    private String attribute;

    // Our Values are all integers (rowIds)
    private Node<T, Integer> root;
    private final int order; // Maximum children per node

    /** Constructor to initialize the B+ Tree with a given order */
    public BPlusTreeIndex(Class<T> type, String attribute) {
        this.type = type;
        this.attribute = attribute;
        this.order = ORDER;
        this.root = new Node<>();
        this.root.isLeaf = true;
    }

    @SuppressWarnings("unchecked")
    private int compare(T key1, T key2) {
        if (key1 instanceof Integer && key2 instanceof Integer) {
            return ((Integer) key1).compareTo((Integer) key2);
        } else if (key1 instanceof Double || key2 instanceof Double) {
            return ((Double) key1).compareTo((Double) key2);
        } else if (key1 instanceof String) {
            return ((String) key1).compareTo((String) key2);
        } else if (key1 instanceof LocalDate) {
            return ((LocalDate) key1).compareTo((LocalDate) key2);
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + key1.getClass());
        }
    }

    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using B+ Tree index on attribute " + attribute + " for operator " + node.operator);
        //TODO: Implement me!
        List<Integer> result = new ArrayList<>();

        // System.out.println("evaluate called!");

        // Convert node.value to appropriate type
        T key, secondKey = null;
        if (type == Integer.class) {
            key = (T) Integer.valueOf(node.value);
            if (node.operator == Operator.RANGE) {
                secondKey = (T) Integer.valueOf(node.secondValue);
            }
        } else if (type == String.class) {
            key = (T) node.value;
            if (node.operator == Operator.RANGE) {
                secondKey = (T) node.secondValue;
            }
        } else if (type == Double.class) {
            key = (T) Double.valueOf(node.value);
            if (node.operator == Operator.RANGE) {
                secondKey = (T) Double.valueOf(node.secondValue);
            }
        } else if (type == LocalDate.class) {
            key = (T) LocalDate.parse(node.value);
            if (node.operator == Operator.RANGE) {
                secondKey = (T) LocalDate.parse(node.secondValue);
            }
        } else {
            throw new UnsupportedOperationException("B+ Tree index does not support type " + type);
        }

        // Perform B+ Tree search based on operator type
        switch (node.operator) {
            case EQUALS:
                result = search(key);
                break;
            case LT:

                result = rangeQuery(null, false, key, false); // (-∞, key)
                break;
            case GT:
                result = rangeQuery(key, false, null, false); // (key, +∞)
                break;
            case RANGE:
                result = rangeQuery(key, true, secondKey, true); // [key, secondKey]
                break;
            default:
                throw new UnsupportedOperationException("Operator " + node.operator + " is not supported by B+ Tree index");
        }

        // System.out.println("evaluate finished!");

        return result;
    }


    @Override
    public void insert(T key, int rowId) {
        //TODO: Implement me!
        // System.out.println("Inserting key: " + key);
        Node<T, Integer> leaf = findLeafNode(root, key);
        // System.out.println("find leaf node: ok!");
        
        if(leaf.keys == null){
            leaf.keys = new ArrayList<>(); 
            leaf.values = new ArrayList<>(); 
            leaf.children = new ArrayList<>();
        }

        // Find the correct position using linear search
        int pos = findPosition(leaf.keys, key);

        // System.out.println("find position: ok!");
        // System.out.println("pos: " + pos);

        if (pos < leaf.keys.size() && compare(leaf.keys.get(pos), key) == 0) {
            // Key already exists, store in auxiliary child node
            // System.out.println("Key already exists!");
            if (leaf.children.get(pos) == null) {
                leaf.children.set(pos, new Node<>());
                leaf.children.get(pos).isLeaf = true;
                leaf.children.get(pos).values = new ArrayList<>();
                leaf.children.get(pos).keys = new ArrayList<>();

            }
            leaf.children.get(pos).values.add(rowId);
            return;
        }

        // Key does not exist, insert in sorted order
        leaf.keys.add(pos, key);
        leaf.values.add(pos, rowId);

        Node<T, Integer> newNode = new Node<>();
        newNode.keys = new ArrayList<>();
        newNode.values = new ArrayList<>();
        newNode.children = new ArrayList<>();
        leaf.children.add(pos, newNode); // Auxiliary child node
        leaf.children.get(pos).isLeaf = true;
        leaf.children.get(pos).values.add(rowId);
        // System.out.println("Successfully Inserted!");

        // If overflow, split the node
        if (leaf.keys.size() >= order) {
            splitLeaf(leaf);
            // System.out.println("splitleaf: ok!");
        }
    }

    @Override
    public boolean delete(T key) {
        //TODO: Bonus
        return false;
    }

    @Override
    public List<Integer> search(T key) {
        //TODO: Implement me!
        //Note: When searching for a key, use Node's getChild() and getNext() methods. Some test cases may fail otherwise!
        // System.out.println("search called!");
        Node<T, Integer> leaf = findLeafNode(root, key);
        int pos = findPosition(leaf.keys, key);
        // System.out.println("pos: " + pos);
        if (pos < leaf.keys.size() && compare(leaf.keys.get(pos), key) == 0) {
            return new ArrayList<>(leaf.children.get(pos).values); // Return values from auxiliary child node
        }
        return new ArrayList<>();
    }

    /** Helper: Find the correct leaf node for a key */
    private Node<T, Integer> findLeafNode(Node<T, Integer> node, T key) {
        while (!node.isLeaf) {
            // System.out.println(node.keys);
            int pos = findPosition(node.keys, key);
            // System.out.println("pos: " + pos);
            int childIndex = pos; // Correct child pointer
            node = node.getChild(childIndex);
        }
        return node;
    }

    /**
     * Function that evaluates a range query and returns a list of rowIds.
     * e.g., 50 < x <=75, then function can be called as rangeQuery(50, false, 75, true)
     * @param startKey
     * @param startInclusive
     * @param endKey
     * @param endInclusive
     * @return all rowIds that satisfy the range predicate
     */
    List<Integer> rangeQuery(T startKey, boolean startInclusive, T endKey, boolean endInclusive) {
        //TODO: Implement me!
        //Note: When searching, use Node's getChild() and getNext() methods. Some test cases may fail otherwise!
        // System.out.println("Range Query called!");
        List<Integer> result = new ArrayList<>();

        // case less than
        if (startKey == null) {
            Node<T,Integer> current = root;
            while (current != null && !current.isLeaf) {
                current = current.getChild(0);  // Move to the smallest key
            }

            // Traverse the tree and collect values where key < endKey
            while (current != null) {
                for (int i = 0; i < current.keys.size(); i++) {
                    T key = current.keys.get(i);
                    if (compare(key, endKey) < 0 || (endInclusive && compare(key, endKey) == 0)) {
                        result.addAll(current.children.get(i).values);
                    }
                }
                current = current.getNext(); // Move to next leaf node  
            }
            return result;
        }

        // case greater than
        if(endKey == null){
            Node<T,Integer> current = findLeafNode(root, startKey);
            while(current != null){
                for(int i = 0; i < current.keys.size(); i++){
                    T key = current.keys.get(i);
                    boolean greaterThanStart = compare(key, startKey) > 0 || (startInclusive && compare(key, startKey) == 0);
                    if(greaterThanStart){
                        result.addAll(current.children.get(i).values);
                    }
                }
                current = current.getNext();
            }
            return result;
        }

        Node<T, Integer> leaf = findLeafNode(root, startKey);

        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                T key = leaf.keys.get(i);
                // Use compare function
                boolean greaterThanStart = compare(key, startKey) > 0 || (startInclusive && compare(key, startKey) == 0);
                boolean lessThanEnd = compare(key, endKey) < 0 || (endInclusive && compare(key, endKey) == 0);
                if (greaterThanStart && lessThanEnd) {
                    result.addAll(leaf.children.get(i).values);
                }
            }
            leaf = leaf.getNext(); // Move to next leaf node
        }
        return result;
    }

    /**
     * Traverse leaf nodes and collect all keys in sorted order
     * @return all Keys
     */
    public List<T> getAllKeys() {
        // TODO: Implement me!
        List<T> allKeys = new ArrayList<>();
        Node<T, Integer> current = root;
        
        // Find leftmost leaf
        while (!current.isLeaf) {
            current = current.getChild(0);
        }
        
        // Traverse leaves
        while (current != null) {
            allKeys.addAll(current.keys);
            current = current.getNext();
        }
        
        return allKeys;
    }

    /**
     * Compute tree height by traversing from root to leaf
     * @return Height of the b+ tree
     */
    public int getHeight() {
        // TODO: Implement me!
        int height = 0;
        Node<T, Integer> node = root;
        while (!node.isLeaf) {
            node = node.getChild(0); // Move downwards
            height++;
        }
        return height;
    }


    /** Helper: Split a leaf node */
    private void splitLeaf(Node<T, Integer> leaf) {
        // System.out.println("splitLeaf called!");
        int mid = leaf.keys.size() / 2;
        Node<T, Integer> newLeaf = new Node<>();
        newLeaf.keys = new ArrayList<>(); 
        newLeaf.values = new ArrayList<>(); 
        newLeaf.children = new ArrayList<>();
        newLeaf.isLeaf = true;

        // Move half of the keys and values to the new leaf
        newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
        newLeaf.values.addAll(leaf.values.subList(mid, leaf.values.size()));
        newLeaf.children.addAll(leaf.children.subList(mid, leaf.children.size()));

        


        // System.out.println("leaf keys size: " + leaf.keys.size());
        leaf.keys.subList(mid, leaf.keys.size()).clear();
        leaf.values.subList(mid, leaf.values.size()).clear();
        leaf.children.subList(mid, leaf.children.size()).clear();
        // System.out.println("leaf keys size: " + leaf.keys.size());

        // System.out.println(newLeaf.keys);
        // System.out.println(leaf.keys);
        
        newLeaf.next = leaf.next;
        leaf.next = newLeaf;
        
        // Insert into parent
        if (leaf == root) {
            Node<T, Integer> newRoot = new Node<>();
            newRoot.keys = new ArrayList<>(); 
            newRoot.values = new ArrayList<>(); 
            newRoot.children = new ArrayList<>();
            newRoot.keys.add(newLeaf.keys.get(0));
            newRoot.children.add(leaf);
            newRoot.children.add(newLeaf);
            root = newRoot;
        } else {
            insertIntoParent(leaf, newLeaf.keys.get(0), newLeaf);
        }
        // System.out.println("splitLeaf finished!");
    }

    /** Helper: Insert into parent node */
    private void insertIntoParent(Node<T, Integer> oldNode, T key, Node<T, Integer> newNode) {
        // System.out.println("insertIntoParent called!");
        Node<T, Integer> parent = findParent(root, oldNode);
        // System.out.println("parent: " + parent.keys);
        int insertPos = findPosition(parent.keys, key);
        // System.out.println("insertPos: " + insertPos);
        // System.out.println("parent.children size before insert: " + parent.children.size());
        parent.keys.add(insertPos, key);
        parent.children.add(insertPos+1, newNode);

        if (parent.keys.size() >= order) {
            splitInternal(parent);
        }
        // System.out.println("insertIntoParent finished!");
    }

    /** Helper: Find the correct position using linear search */
    private int findPosition(List<T> keys, T key) {

        int i = 0;

        while(i<keys.size()){
            if (compare(keys.get(i), key) < 0) {
                i++;
            }else{
                break;
            }
        }
        
        return i; // If key is larger than all elements, insert at the end
    }

    /** Helper: Split an internal node */
    private void splitInternal(Node<T, Integer> node) {
        // System.out.println("splitInternal called!");
        // System.out.println("node keys: " + node.keys.size());
        // System.out.println("node children: " + node.children.size());
        int mid = node.keys.size() / 2;
        Node<T, Integer> newNode1 = new Node<>();
        newNode1.keys = new ArrayList<>(); 
        newNode1.values = new ArrayList<>(); 
        newNode1.children = new ArrayList<>();
        
        newNode1.keys.addAll(node.keys.subList(mid+1, node.keys.size()));
        newNode1.children.addAll(node.children.subList(mid+1, node.children.size()));

        T midKey = node.keys.get(mid);
        
        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid+1, node.children.size()).clear();
        
        

        if (node == root) {
            Node<T, Integer> newRoot = new Node<>();
            newRoot.keys = new ArrayList<>(); 
            newRoot.values = new ArrayList<>(); 
            newRoot.children = new ArrayList<>();
            newRoot.keys.add(midKey);
            newRoot.children.add(node);
            newRoot.children.add(newNode1);
            root = newRoot;
        } else {
            insertIntoParent(node, midKey, newNode1);
        }
        // System.out.println("splitInternal finished!");
    }

    /** Find parent node */
    private Node<T, Integer> findParent(Node<T, Integer> node, Node<T, Integer> child) {
        if (node == null || node.isLeaf) return null; // Base case: no parent found
        for (Node<T, Integer> n : node.children) {
            if (n == child) {
                return node; // Found the parent!
            }
        }
        // Recursively search in children
        for (Node<T, Integer> n : node.children) {
            Node<T, Integer> result = findParent(n, child);
            if (result != null) return result; // If found, return parent
        }

        return null; // If not found, return null
    }

    /**
     * Funtion that returns the order of the BPlusTree
     * Note: Do not remove this function!
     * @return
     */
    public int getOrder() {
        return order;
    }


    public String getAttribute() {
        return attribute;
    }

    public Node<T, Integer> getRoot() {
        return root;
    }


    @Override
    public String prettyName() {
        return "B+Tree Index";
    }
}
