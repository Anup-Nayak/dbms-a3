package in.ac.iitd.db362.processor;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.index.Index;
import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Starter code for Query Evaluator
 */
public class QueryEvaluator {

    protected static final Logger logger = LogManager.getLogger();

    /**
     * Note: do not change or remove this function! This method **must** be called from the evaluateQuery() method
     * when processing a leaf (predicate) node.
     * @param node
     * @return row IDs for which the predicate holds.
     */
    private static List<Integer> evaluatePredicate(QueryNode node) {
        logger.info("Evaluating predicate: " + node.attribute + " " + node.operator + " " + node.value
                + (node.operator == Operator.RANGE ? " and " + node.secondValue : ""));

        //Let's get an index to work with
        Catalog catalog = Catalog.getInstance();
        Index index = catalog.getIndex(node.attribute, node.operator);

        logger.info("Using " + index.prettyName());

        return index.evaluate(node);
    }

    /**
     * Evaluate the query represented by the parse tree.
     * For predicate (leaf) nodes, return a list of row IDs by calling evaluatePredicate() .
     * For boolean operators, performs set operations:
     * - AND: Intersection of left and right results.
     * - OR: Union of left and right results.
     * - NOT: Complement of the result (assume row IDs from 0 to maxRowId).
     *
     * @param node The current query node.
     * @param maxRowId The maximum row ID (min is assumed to be 0).
     * @return A list of row IDs that satisfy the query.
     */
    public static List<Integer> evaluateQuery(QueryNode node, int maxRowId) {
        // Note: When traversing the parse tree, for each leaf node you must call
        // the evalautePredicate(node) method that is provided.
        // TODO: Implement me!
        if (node == null) return new ArrayList<>();

        if (node.operator == Operator.AND || node.operator == Operator.OR) {
            // Postorder traversal: process children first
            List<Integer> leftResult = evaluateQuery(node.left, maxRowId);
            List<Integer> rightResult = evaluateQuery(node.right, maxRowId);

            if (node.operator == Operator.AND) {
                // Intersection for AND
                leftResult.retainAll(rightResult);
                return leftResult;
            } else {
                // Union for OR
                Set<Integer> resultSet = new HashSet<>(leftResult);
                resultSet.addAll(rightResult);
                return new ArrayList<>(resultSet);
            }
        } else if (node.operator == Operator.NOT) {
            // Postorder traversal for NOT
            List<Integer> result = evaluateQuery(node.left, maxRowId);
            Set<Integer> allRows = new HashSet<>();
            for (int i = 0; i <= maxRowId; i++) {
                allRows.add(i);
            }
            allRows.removeAll(result);
            return new ArrayList<>(allRows);
        } else {
            // Leaf node (predicate)
            return evaluatePredicate(node);
        }
    }


}
