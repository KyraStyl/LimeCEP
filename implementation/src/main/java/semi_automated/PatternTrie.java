package semi_automated;

import org.apache.kafka.common.protocol.types.Field;

import java.io.PrintWriter;
import java.util.*;

public class PatternTrie {

    private final TrieNode root = new TrieNode("\u0000", null);
    private String originalPattern = "";
    private int matches;
    private final Set<String> activatedPatterns = new HashSet<>();

    public PatternTrie() {}

    public void insert(String pattern, Boolean initialization) {
        TrieNode node = root;
        for (char c : pattern.toCharArray()) {
            node = node.addChild(String.valueOf(c));
            if (!initialization) {
                node.incrementCount();
//                matches = 0;
            }
        }
        node.markAsEnd();
        if (initialization) originalPattern = pattern;
    }

    /** Insert a pattern like "ABC" */
    public void insert(String pattern) {
        TrieNode node = root;
        for (char c : pattern.toCharArray()) {
            node = node.addChild(String.valueOf(c));
            node.incrementCount();
        }
        node.markAsEnd();
        matches++;
    }

    public void insertMultiple(String pattern, int count){
        TrieNode node = root;
        for (char c : pattern.toCharArray()) {
            node = node.addChild(String.valueOf(c));
            node.increaseCount(count);
        }
        node.markAsEnd();
        matches+=count;
    }

    public String getOriginalPattern() {
        return originalPattern;
    }

    /** Check if a pattern fully exists */
    public boolean exists(String pattern) {
        TrieNode node = findNode(pattern);
        return node != null && node.isEndOfPattern();
    }

    /** Get the TrieNode that represents the end of the prefix */
    public TrieNode findNode(String prefix) {
        TrieNode node = root;
        for (char c : prefix.toCharArray()) {
            node = node.getChild(String.valueOf(c));
            if (node == null) return null;
        }
        return node;
    }

    /**
     * Suggest extensions: all longer patterns that start with "pattern".
     * Example: "ABC" → "ABCD", "ABCEF", etc.
     */
    public List<String> suggestExtensions(String pattern) {
        TrieNode node = findNode(pattern);
        if (node == null) return Collections.emptyList();

        List<String> results = new ArrayList<>();
        dfs(pattern, node, results);
        results.remove(pattern);  // remove exact match
        return results;
    }

    /** DFS to enumerate all patterns rooted at a given node */
    private void dfs(String prefix, TrieNode node, List<String> out) {
        if (node.isEndOfPattern()) {
            out.add(prefix);
        }
        for (var entry : node.getChildren().entrySet()) {
            dfs(prefix + entry.getKey(), entry.getValue(), out);
        }
    }

    /**
     * Suggest alterations: change exactly ONE character.
     * Example: "ABC" → "ABD", "ABE", "ABF"
     */
    public List<String> suggestAlterations(String pattern) {
        TrieNode leaf = findNode(pattern);
        if (leaf == null || leaf.getParent() == null) return Collections.emptyList();

        TrieNode parent = leaf.getParent();
        String prefix = pattern.substring(0, pattern.length() - 1);

        List<String> alternatives = new ArrayList<>();

        for (String sibling : parent.getChildren().keySet()) {
            if (sibling == leaf.getValue()) continue; // skip original char

            TrieNode siblingNode = parent.getChild(sibling);
            if (siblingNode != null && siblingNode.isEndOfPattern()) {
                alternatives.add(prefix + sibling);
            }
        }

        return alternatives;
    }

    /**
     * Suggest both extension and alteration simultaneously.
     */
    public Map<String, List<String>> suggestAll(String pattern) {
        Map<String, List<String>> out = new HashMap<>();
        out.put("extensions", suggestExtensions(pattern));
        out.put("alterations", suggestAlterations(pattern));
        return out;
    }

    /**
     * Calculate confidence for a pattern: count(pattern) / count(parent).
     * For example, confidence of "ABC" = count(ABC) / count(AB)
     *
     * @param pattern The pattern to calculate confidence for
     * @return Confidence value between 0 and 1, or -1 if pattern doesn't exist or has no parent
     */
    public double calculateConfidence(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return -1;
        }

        TrieNode node = findNode(pattern);
        if (node == null || node.getParent() == null) {
            return -1;
        }

        if (matches==0)
            return 0;

        double freq = node.getCount();
        if (node.isEndOfPattern()) {
            double childcount = 0;
            for( TrieNode child : node.getChildren().values() )
                childcount += child.getCount();
            freq -= (childcount);
        }

        return freq / matches;
    }

//    public double calculateConfidence(String pattern) {
//
//        if (pattern == null || pattern.length() <= 1)
//            return 1;
//
//        TrieNode node = findNode(pattern);
//        if (node == null)
//            return 0;
//
//        TrieNode parent = node.getParent();
//        if (parent == null)
//            return 1;
//
//        double childSupport = node.getCount();
//        double parentSupport = parent.getCount();
//
//        if (parentSupport == 0)
//            return 0;
//
//        return childSupport / parentSupport;
//    }

    /**
     * Get all complete patterns in the trie with their confidence and support values.
     *
     * @return Map of pattern to [confidence, support]
     */
    public Map<String, double[]> getAllPatternStats() {
        Map<String, double[]> stats = new HashMap<>();
        collectPatternStats(root, "", stats);
        return stats;
    }

    public int getMatches() {
        return matches;
    }

    /**
     * Helper method to recursively collect pattern statistics.
     */
    private void collectPatternStats(TrieNode node, String prefix, Map<String, double[]> stats) {
        if (node.isEndOfPattern() && !prefix.isEmpty()) {
            double confidence = calculateConfidence(prefix);
            int support = node.getCount();
            stats.put(prefix, new double[]{confidence, support});
        }

        for (var entry : node.getChildren().entrySet()) {
            collectPatternStats(entry.getValue(), prefix + entry.getKey(), stats);
        }
    }

    /**
     * Prune branches based on confidence and support criteria.
     * Removes patterns where confidence > 0.5 AND support <= 1
     *
     * @return Number of patterns pruned
     */
    public int pruneByConfidenceAndSupport() {
        return pruneByConfidenceAndSupport(0.5, 1);
    }

    /**
     * Prune branches based on confidence and support criteria.
     * Removes patterns where confidence > confidenceThreshold AND support <= supportThreshold
     *
     * @param confidenceThreshold Confidence threshold (e.g., 0.5 for 50%)
     * @param supportThreshold Support threshold
     * @return Number of patterns pruned
     */
    public int pruneByConfidenceAndSupport(double confidenceThreshold, int supportThreshold) {
        List<String> toPrune = new ArrayList<>();

        // Collect patterns that meet pruning criteria
        Map<String, double[]> stats = getAllPatternStats();
        for (var entry : stats.entrySet()) {
            String pattern = entry.getKey();
            double confidence = entry.getValue()[0];
            int support = (int) entry.getValue()[1];

            if (confidence > confidenceThreshold && support <= supportThreshold) {
                toPrune.add(pattern);
            }
        }

        // Prune collected patterns -- not working yet
//        for (String pattern : toPrune) {
//            removePattern(pattern);
//        }

        return toPrune.size();
    }

    /**
     * Remove a pattern from the trie.
     * Only removes the end-of-pattern marker and the branch if it has no other children.
     * 
     * @param pattern The pattern to remove
     * @return true if the pattern was removed, false if it didn't exist
     */
    private boolean removePattern(String pattern) {
        TrieNode node = findNode(pattern);
        if (node == null || !node.isEndOfPattern()) {
            return false;
        }
        
        // Unmark this node as end of pattern
        node.unmarkAsEnd();
        
        // If this node has no children, remove it and walk up removing empty nodes
        if (node.getChildren().isEmpty()) {
            TrieNode current = node;
            TrieNode parent = current.getParent();
            
            while (parent != null && current.getChildren().isEmpty() && !current.isEndOfPattern()) {
                // Get the current node's value before moving up
                String valueToRemove = current.getValue();
                
                // Remove current from parent's children using the key
                parent.getChildren().remove(valueToRemove);
                
                // Move up the tree
                current = parent;
                parent = current.getParent();
            }
        }
        
        return true;
    }

    /**
     * Print all patterns with their confidence and support values.
     */
    public void printPatternStats(PrintWriter out) {
        out.println("=== Pattern Statistics ===");
        Map<String, double[]> stats = getAllPatternStats();

        // Sort by pattern for consistent output
        List<String> patterns = new ArrayList<>(stats.keySet());
        Collections.sort(patterns);

        for (String pattern : patterns) {
            double confidence = stats.get(pattern)[0];
            int support = (int) stats.get(pattern)[1];
            out.printf("Pattern: %s | Confidence: %.2f%% | Support: %d%n",
                    pattern, confidence * 100, support);
        }
        out.println("==========================");
    }

    /**
     * Print the entire trie structure in a tree format.
     */
    public void printTrie(PrintWriter out) {
        out.println("=== Pattern Trie Structure ===");
        printTrieHelper(root, "", true,out);
        out.println("==============================");
    }

    /**
     * Recursive helper to print the trie structure with tree-like formatting.
     * @param node Current node being printed
     * @param prefix Prefix for formatting (indentation and tree lines)
     * @param isLast Whether this is the last child of its parent
     */
    private void printTrieHelper(TrieNode node, String prefix, boolean isLast, PrintWriter out) {
        // Print current node
        String nodeValue = node.getValue().equals("\u0000") ? "[ROOT]" : node.getValue();
        String endMarker = node.isEndOfPattern() ? " --- " : "";
        String countInfo = node.getCount() > 0 ? " (count: " + node.getCount() + ")" : "";

        out.println(prefix + (isLast ? "└── " : "├── ") + nodeValue + endMarker + countInfo);

        // Prepare prefix for children
        String childPrefix = prefix + (isLast ? "    " : "│   ");

        // Get children and sort them for consistent output
        List<Map.Entry<String, TrieNode>> sortedChildren = new ArrayList<>(node.getChildren().entrySet());
        sortedChildren.sort(Map.Entry.comparingByKey());

        // Print each child
        for (int i = 0; i < sortedChildren.size(); i++) {
            boolean isLastChild = (i == sortedChildren.size() - 1);
            printTrieHelper(sortedChildren.get(i).getValue(), childPrefix, isLastChild,out);
        }
    }

    public Set<String> getActivatedPatterns() {
        return activatedPatterns;
    }

//    public Set<String> getActivatedPatterns() {
//
//        Set<String> result = new HashSet<>();
//        collectActivated(root, "", result);
//        return result;
//    }

    private void collectActivated(TrieNode node,
                                  String prefix,
                                  Set<String> result) {

        if (node.isEndOfPattern())
            result.add(prefix);

        for (var e : node.getChildren().entrySet()) {
            collectActivated(e.getValue(),
                    prefix + e.getKey(),
                    result);
        }
    }

    public void activatePattern(String pattern) {

        TrieNode node = root;

        for (char c : pattern.toCharArray()) {
            node = node.addChild(String.valueOf(c));
        }

        node.markAsEnd();
        activatedPatterns.add(pattern);
    }

    public int countActivatedPatterns() {
        return getActivatedPatterns().size();
    }
}
