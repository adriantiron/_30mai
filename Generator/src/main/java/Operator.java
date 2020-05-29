public enum Operator {
    GT(">"),
    GTEQ(">="),
    L("<"),
    LEQ("<="),
    EQ("="),
    NEQ("!=")
    ;

    private String operatorString;

    Operator(String operatorString) {
        this.operatorString = operatorString;
    }

    public String getOperatorString() {
        return operatorString;
    }
}
