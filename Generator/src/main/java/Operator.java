public enum Operator {
    GT(">"),
    GTEQ(">="),
    L("<"),
    LEQ("<="),
    EQ("=")
    ;

    private String operatorString;

    Operator(String operatorString) {
        this.operatorString = operatorString;
    }

    public String getOperatorString() {
        return operatorString;
    }
}
