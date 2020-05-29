import java.util.*;

public class GeneratorImpl implements Generator {

    private int companyNumber = 0;
    private int stockValueNumber = 0;
    private int changeNumber = 0;
    private int variationNumber = 0;
    private int dateNumber = 0;
    private int equalCompanyNumber = 0;

    private Random random = new Random();

    private Map<Company, Double> previousStocks = new HashMap<Company, Double>();

    @Override
    public List<Publication> generatePublications(PublicationGenerationParams params) {
        List<Publication> publications = new ArrayList<>();

        Publication.stockMin = params.getMinStockValue();
        Publication.stockMax = params.getMaxStockValue();

        if (params.getPublicationCount() < Company.values().length) {
            throw new RuntimeException("Publication count must be bigger than the number of companies");
        }
        //compute necessary constants
        double stockMean = computeMean(params.getMinStockValue(), params.getMaxStockValue());
        double stockStddev = computeStddev(params.getMinStockValue(), params.getMaxStockValue());

        //add initial stock values
        for (Company company : Company.values()) {
            double initialStock = nextStock(stockMean, stockStddev, params.getMinStockValue(), params.getMaxStockValue());
            publications.add(new Publication(company, initialStock, 0D, params.getStartingDate()));
            previousStocks.put(company, initialStock);
        }

        Date currentDate = params.getStartingDate();
        while (publications.size() < params.getPublicationCount()) {
            currentDate = addDaysToDate(currentDate, params.getDaysBetween());
            for (Company company : Company.values()) {
                double previousStock = previousStocks.get(company);
                double change = nextChange(previousStock, params.getMaxChangePercentage());
                publications.add(new Publication(company, previousStock + change, change, currentDate));
                previousStocks.put(company, previousStock + change);
            }
        }

        return publications;
    }

    @Override
    public List<Subscription> generateSubscriptions(SubscriptionGenerationParams params) {
        List<Subscription> subscriptions = new ArrayList<>();

        int companyTotal = (int)(params.getCompanyPercentage() * params.getSubscriptionCount());
        int stockValueTotal = (int)(params.getStockValuePercentage() * params.getSubscriptionCount());
        int changeTotal = (int)(params.getChangePercentage() * params.getSubscriptionCount());
        int variationTotal = (int)(params.getVariationPercentage() * params.getSubscriptionCount());
        int dateTotal = (int)(params.getDatePercentage() * params.getSubscriptionCount());
        int equalCompanyTotal = (int)(params.getEqualCompanyPercentage() * companyTotal);

        while (subscriptions.size() < params.getSubscriptionCount()) {
            Subscription subscription = new Subscription();

            if (random.nextDouble() < params.getCompanyPercentage()) {
                int companyIndex = random.nextInt(Company.values().length);
                Filter companyFilter;
                boolean equalUpdate = false;
                if (random.nextDouble() < params.getEqualCompanyPercentage() && equalCompanyNumber < equalCompanyTotal) {
                    companyFilter = new Filter(Operator.EQ, Company.values()[companyIndex], "company");
                    equalUpdate = true;
                } else {
                    companyFilter = new Filter(Operator.NEQ, Company.values()[companyIndex], "company");
                }

                if (companyNumber < companyTotal) {
                    subscription.getFilters().add(companyFilter);
                    if (equalUpdate) {
                        equalCompanyNumber++;
                    }
                    companyNumber++;
                    subscriptions.add(subscription);
                    continue;
                }
            }

            if (random.nextDouble() < params.getStockValuePercentage()) {
                double value = nextUniform(params.getPublicationParams().getMinStockValue(), params.getPublicationParams().getMaxStockValue());
                int operatorIndex = random.nextInt(Operator.values().length);
                Filter stockFilter = new Filter(Operator.values()[operatorIndex], value, "stockValue");

                if (stockValueNumber < stockValueTotal) {
                    subscription.getFilters().add(stockFilter);
                    stockValueNumber++;
                    subscriptions.add(subscription);
                    continue;
                }
            }

            if (random.nextDouble() < params.getChangePercentage()) {
                double maxChange = Math.abs(params.getPublicationParams().getMaxStockValue() * params.getPublicationParams().getMaxChangePercentage());
                double value = nextUniform(-maxChange, maxChange);
                int operatorIndex = random.nextInt(Operator.values().length);
                Filter changeFilter = new Filter(Operator.values()[operatorIndex], value, "change");

                if (changeNumber < changeTotal) {
                    subscription.getFilters().add(changeFilter);
                    changeNumber++;
                    subscriptions.add(subscription);
                    continue;
                }
            }

            if (random.nextDouble() < params.getVariationPercentage()) {
                double value = nextUniform(0D, 1D);
                int operatorIndex = random.nextInt(Operator.values().length);
                Filter variationFilter = new Filter(Operator.values()[operatorIndex], value, "variation");

                if (variationNumber < variationTotal) {
                    subscription.getFilters().add(variationFilter);
                    variationNumber++;
                    subscriptions.add(subscription);
                    continue;
                }
            }

            if (random.nextDouble() < params.getDatePercentage()) {
                Date value = new Date(nextUniform(params.getPublicationParams().getStartingDate().getTime(), params.getEndDate().getTime()));
                int operatorIndex = random.nextInt(Operator.values().length);
                Filter dateFilter = new Filter(Operator.values()[operatorIndex], value, "date");

                if (dateNumber < dateTotal) {
                    subscription.getFilters().add(dateFilter);
                    dateNumber++;
                    subscriptions.add(subscription);
                }
            }
        }

        while (companyNumber < companyTotal ||
                stockValueNumber < stockValueTotal ||
                changeNumber < changeTotal ||
                variationNumber < variationTotal ||
                dateNumber < dateTotal ||
                equalCompanyNumber < equalCompanyTotal) {

            for (Subscription subscription : subscriptions) {

                if (random.nextDouble() < params.getCompanyPercentage()) {
                    int companyIndex = random.nextInt(Company.values().length);
                    Filter companyFilter;
                    boolean equalUpdate = false;
                    if (random.nextDouble() < params.getEqualCompanyPercentage() && equalCompanyNumber < equalCompanyTotal) {
                        companyFilter = new Filter(Operator.EQ, Company.values()[companyIndex], "company");
                        equalUpdate = true;
                    } else {
                        companyFilter = new Filter(Operator.NEQ, Company.values()[companyIndex], "company");
                    }

                    if (subscription.contains("company") && equalUpdate && !subscription.hasEq("company")) {
                        subscription.update("company", companyFilter);
                        equalCompanyNumber++;
                    }
                    else if (!subscription.contains("company") && companyNumber < companyTotal) {
                        subscription.getFilters().add(companyFilter);
                        companyNumber++;
                    }
                }

                if (random.nextDouble() < params.getStockValuePercentage()) {
                    double value = nextUniform(params.getPublicationParams().getMinStockValue(), params.getPublicationParams().getMaxStockValue());
                    int operatorIndex = random.nextInt(Operator.values().length);
                    Filter stockFilter = new Filter(Operator.values()[operatorIndex], value, "stockValue");

                    if (!subscription.contains("stockValue") && stockValueNumber < stockValueTotal) {
                        subscription.getFilters().add(stockFilter);
                        stockValueNumber++;
                    }
                }

                if (random.nextDouble() < params.getChangePercentage()) {
                    double maxChange = Math.abs(params.getPublicationParams().getMaxStockValue() * params.getPublicationParams().getMaxChangePercentage());
                    double value = nextUniform(-maxChange, maxChange);
                    int operatorIndex = random.nextInt(Operator.values().length);
                    Filter changeFilter = new Filter(Operator.values()[operatorIndex], value, "change");

                    if (!subscription.contains("change") && changeNumber < changeTotal) {
                        subscription.getFilters().add(changeFilter);
                        changeNumber++;
                    }
                }

                if (random.nextDouble() < params.getVariationPercentage()) {
                    double value = nextUniform(0D, 1D);
                    int operatorIndex = random.nextInt(Operator.values().length);
                    Filter variationFilter = new Filter(Operator.values()[operatorIndex], value, "variation");

                    if (!subscription.contains("variation") && variationNumber < variationTotal) {
                        subscription.getFilters().add(variationFilter);
                        variationNumber++;
                    }
                }

                if (random.nextDouble() < params.getDatePercentage()) {
                    Date value = new Date(nextUniform(params.getPublicationParams().getStartingDate().getTime(), params.getEndDate().getTime()));
                    int operatorIndex = random.nextInt(Operator.values().length);
                    Filter dateFilter = new Filter(Operator.values()[operatorIndex], value, "date");

                    if (!subscription.contains("date") && dateNumber < dateTotal) {
                        subscription.getFilters().add(dateFilter);
                        dateNumber++;
                    }
                }

                subscription.sort();
            }
        }

        return subscriptions;
    }

    private double nextChange(double stock, double maxPercentage) {
        double maxChange = Math.abs(stock * maxPercentage);
        return 2 * maxChange * random.nextDouble() - maxChange;
    }

    private double nextUniform(double min, double max) {
        return min + (max - min) * random.nextDouble();
    }

    private long nextUniform(long min, long max) {
        return min + (max - min) * random.nextLong();
    }

    private double nextStock(double mean, double stddev, double min, double max) {
        double value = random.nextGaussian() * stddev + mean;
        if (value > max) {
            return max;
        } else if (value < min) {
            return min;
        }
        return value;
    }

    private double computeMean(double min, double max) {
        return (max + min) / 2;
    }

    private double computeStddev(double min, double max) {
        return (max - min) / 4;
    }

    private Date addDaysToDate(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        calendar.add(Calendar.DATE, days);

        return calendar.getTime();

    }
}
