package semi_automated;

import java.util.HashMap;
import java.util.Map;

public class TrieNode {

    private final String value;

    private final TrieNode parent;

    private final Map<String, TrieNode> children;

    private int count;

    private boolean isEndOfPattern;

    private final Map<String, Object> metadata;

    public TrieNode(String value, TrieNode parent) {
        this.value = value;
        this.parent = parent;
        this.children = new HashMap<>();
        this.metadata = new HashMap<>();
        this.count = 0;
        this.isEndOfPattern = false;
    }

    public TrieNode addChild(String c) {
        return children.computeIfAbsent(c, ch -> new TrieNode(ch, this));
    }

    public TrieNode getChild(String c) {
        return children.get(c);
    }

    public boolean hasChild(String c) {
        return children.containsKey(c);
    }

    public Map<String, TrieNode> getChildren() {
        return children;
    }

    public TrieNode getParent() {
        return parent;
    }

    public String getValue() {
        return value;
    }

    public void incrementCount() {
        this.count++;
    }

    public void increaseCount(int increment) {
        this.count += increment;
    }

    public int getCount() {
        return count;
    }

    public boolean isEndOfPattern() {
        return isEndOfPattern;
    }

    public void markAsEnd() {
        this.isEndOfPattern = true;
    }

    public void unmarkAsEnd() {
        this.isEndOfPattern = false;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void putMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    /**
     * Reconstruct the full pattern from this node by walking up to the root.
     */
    public String reconstructPattern() {
        StringBuilder sb = new StringBuilder();
        TrieNode node = this;

        while (node != null && !node.value.equals('\u0000')) {
            sb.append(node.value);
            node = node.parent;
        }

        return sb.reverse().toString();
    }

    /**
     * Print node information including value, count, pattern status, and metadata.
     */
    public void printNode() {
        System.out.println("=== TrieNode Info ===");
        System.out.println("Value: " + (value.equals("\u0000") ? "[ROOT]" : value));
        System.out.println("Count: " + count);
        System.out.println("Is End of Pattern: " + isEndOfPattern);
        System.out.println("Full Pattern: " + reconstructPattern());
        System.out.println("Children: " + children.keySet());

        if (!metadata.isEmpty()) {
            System.out.println("Metadata:");
            metadata.forEach((key, val) -> System.out.println("  " + key + ": " + val));
        }

        System.out.println("====================");
    }

}
