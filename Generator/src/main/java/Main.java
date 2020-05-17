import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Generator generator = new GeneratorImpl();
        List<Publication> publications = generator.generatePublications(new PublicationGenerationParams());
        writePublicationsToFile(publications);
        Date endDate = publications.get(publications.size() - 1).getDate();
        List<Subscription> subscriptions = generator.generateSubscriptions(new SubscriptionGenerationParams
                (new PublicationGenerationParams(), endDate));
        writeSubscriptionsToFile(subscriptions);
    }

    private static void writePublicationsToFile(List<Publication> publications) {
        try {
            File fout = new File("publications.txt");
            FileOutputStream fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (Publication publication : publications) {
                bw.write(publication.toString());
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeSubscriptionsToFile(List<Subscription> subscriptions) {
        try {
            File fout = new File("subscriptions.txt");
            FileOutputStream fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (Subscription subscription : subscriptions) {
                bw.write(subscription.toString());
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
