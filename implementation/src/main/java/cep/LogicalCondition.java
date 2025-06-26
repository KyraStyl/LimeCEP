package cep;

import java.util.ArrayList;
import java.util.List;

public class LogicalCondition {
    private Condition condition;  // A single condition (like a.x < b.y)
    private List<LogicalCondition> subConditions;  // For nested conditions
    private List<String> logicalOperators;  // "AND", "OR" operators between conditions

    // Constructor for a single condition
    public LogicalCondition(Condition condition) {
        this.condition = condition;
        this.subConditions = null;  // No subconditions for a simple condition
        this.logicalOperators = null;  // No operators for a simple condition
    }

    // Constructor for a group of sub-conditions
    public LogicalCondition(List<LogicalCondition> subConditions, List<String> logicalOperators) {
        this.subConditions = subConditions;
        this.logicalOperators = logicalOperators;
        this.condition = null;  // No individual condition for a composite condition
    }

    public Condition getCondition() {
        return condition;
    }

    // Method to get all the conditions (either a single or nested conditions)
    public List<LogicalCondition> getConditions() {
        List<LogicalCondition> conditions = new ArrayList<>();
        if (isSimpleCondition()) {
            conditions.add(this);  // Return itself if it's a simple condition
        } else if (subConditions != null) {
            for (LogicalCondition subCondition : subConditions) {
                conditions.addAll(subCondition.getConditions());  // Recursively get all subconditions
            }
        }
        return conditions;
    }

    // Method to evaluate the full logical condition including operators
    public String evaluateCondition() {
        if (isSimpleCondition()) {
            return condition.toString();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("(");  // Add parentheses for grouping conditions
            for (int i = 0; i < subConditions.size(); i++) {
                if (i > 0) {
                    sb.append(" ").append(logicalOperators.get(i - 1)).append(" ");
                }
                sb.append(subConditions.get(i).evaluateCondition());
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public boolean isSimpleCondition() {
        return condition != null;
    }

    @Override
    public String toString() {
        return evaluateCondition();  // Use evaluateCondition() to print the logical structure
    }
}
