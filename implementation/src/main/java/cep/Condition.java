package cep;

public class Condition {
    private String leftOperand;
    private String operator;
    private String rightOperand;

    public Condition(String leftOperand, String operator, String rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    public String getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }

    public String getRightOperand() {
        return rightOperand;
    }

    @Override
    public String toString() {
        return leftOperand + " " + operator + " " + rightOperand;
    }
}