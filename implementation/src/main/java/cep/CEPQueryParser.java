package cep;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CEPQueryParser {
    private Map<String, String> eventTypes = new HashMap<>();  // Stores event types and their associated variables

    // Method to parse a query from a file
    public CEPQuery parseFromFile(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder queryBuilder = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            queryBuilder.append(line).append("\n");
        }
        reader.close();

        // Pass the file content to the parseFromString method
        return parseFromString(queryBuilder.toString());
    }

    // Method to parse a query from a string
    public CEPQuery parseFromString(String query) {
        String pattern = null;
        LogicalCondition whereClause = null;
        String within = null;

        // Extract the pattern (between "SEQ(" and ")")
        int patternStart = query.indexOf("SEQ(") + 4;
        int patternEnd = query.lastIndexOf(")");
        if (patternStart > 0 && patternEnd > 0) {
            pattern = query.substring(patternStart, patternEnd).trim();
            parsePattern(pattern);
        }

        // Extract the WHERE clause (optional, between "WHERE" and "WITHIN")
        int whereStart = query.indexOf("WHERE");
        int withinStart = query.indexOf("WITHIN");

        if (whereStart > 0 && withinStart > 0) {
            String whereClauseStr = query.substring(whereStart + 5, withinStart).trim();
            whereClause = parseWhereClause(whereClauseStr);
        }

        // Extract the within clause (after "WITHIN")
        if (withinStart > 0) {
            within = query.substring(withinStart + 6).trim();
        }

        return new CEPQuery(pattern, whereClause, within);
    }

    // Method to parse the pattern and store event types and variables
    private void parsePattern(String pattern) {
        eventTypes.clear();  // Reset the event types
        String[] eventMappings = pattern.split(",");
        for (String eventMapping : eventMappings) {
            String[] parts = eventMapping.trim().split("\\s+");
            if (parts.length == 2) {
                String eventType = parts[0];
                String variable = parts[1];
                eventTypes.put(variable, eventType);
            }
        }
    }

    // Method to validate that all variables in conditions are defined in the pattern
    private boolean validateVariablesInConditions(LogicalCondition whereClause) {
        List<LogicalCondition> conditions = whereClause.getConditions();
        for (LogicalCondition condition : conditions) {
            Condition cepCondition = condition.getCondition();
            if (cepCondition != null) {
                String leftVariable = getVariableFromOperand(cepCondition.getLeftOperand());
                String rightVariable = getVariableFromOperand(cepCondition.getRightOperand());
                if ((leftVariable != null && !eventTypes.containsKey(leftVariable)) ||
                        (rightVariable != null && !eventTypes.containsKey(rightVariable))) {
                    return false;
                }
            }
        }
        return true;
    }

    // Helper to extract the variable from an operand (e.g., "a.x" -> "a")
    private String getVariableFromOperand(String operand) {
        if (operand != null && operand.contains(".")) {
            return operand.split("\\.")[0];  // Extract the variable part before the dot
        }
        return null;
    }

    // Method to parse the WHERE clause and handle nested conditions
    private LogicalCondition parseWhereClause(String whereClauseStr) {
        return parseLogicalConditions(whereClauseStr);
    }

    // Method to handle AND/OR logic, including nested conditions in parentheses
    private LogicalCondition parseLogicalConditions(String input) {
        List<LogicalCondition> conditions = new ArrayList<>();
        List<String> operators = new ArrayList<>();

        // Regex to match conditions
        Pattern conditionPattern = Pattern.compile("\\([^()]+\\)|\\w+\\.\\w+\\s*(<|>|==|!=|<=|>=)\\s*\\w+\\.\\w+|\\d+|'.*'");
        Matcher matcher = conditionPattern.matcher(input);

        while (matcher.find()) {
            String conditionStr = matcher.group();
            conditions.add(parseSingleCondition(conditionStr.trim()));

            // Look for operators between the conditions
            if (matcher.end() < input.length()) {
                String remainingStr = input.substring(matcher.end()).trim();
                if (remainingStr.startsWith("AND")) {
                    operators.add("AND");
                } else if (remainingStr.startsWith("OR")) {
                    operators.add("OR");
                }
            }
        }

        if (conditions.size() > 1) {
            return new LogicalCondition(conditions, operators);
        } else if (conditions.size() == 1) {
            return conditions.get(0);  // Return the single condition if no AND/OR
        }

        return null;
    }

    // Helper method to parse a single condition or nested conditions
    private LogicalCondition parseSingleCondition(String conditionStr) {
        if (conditionStr.startsWith("(") && conditionStr.endsWith(")")) {
            // If the condition is wrapped in parentheses, it's a nested condition
            return parseLogicalConditions(conditionStr.substring(1, conditionStr.length() - 1));
        }

        // Parse the individual condition using regex
        Pattern conditionPattern = Pattern.compile("(\\w+\\.\\w+)\\s*(<|>|==|!=|<=|>=)\\s*(\\w+\\.\\w+|\\d+|'.*')");
        Matcher matcher = conditionPattern.matcher(conditionStr);

        if (matcher.find()) {
            String leftOperand = matcher.group(1);
            String operator = matcher.group(2);
            String rightOperand = matcher.group(3);
            return new LogicalCondition(new Condition(leftOperand, operator, rightOperand));
        }

        return null;  // If no condition is found, return null
    }
}
