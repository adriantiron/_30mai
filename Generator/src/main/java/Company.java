public enum Company {
    GOOGLE("GOOGLE"),
    APPLE("APPLE"),
    MICROSOFT("MICROSOFT"),
    IBM("IBM"),
    ORACLE("ORACLE");

    private String companyName;

    Company(String companyName) {
        this.companyName = companyName;
    }

    public String getCompanyName() {
        return companyName;
    }


}
