package cis5550.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import cis5550.tools.Logger;

public class PageParsing {
    
    private static final Logger logger = Logger.getLogger(cis5550.jobs.PageParsing.class);
    // Static dictionary set
    private static final Set<String> ENGLISH_DICTIONARY;
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "am", "an", "the", "and", "but", "if", "or", "because", "as", "until", 
            "while", "of", "at", "by", "for", "with", "about", "against", "between",
            "into", "through", "during", "before", "after", "above", "below", "to", 
            "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how",
            "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
            "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "is",
            "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "can", "could", "shall", "should", "will", 
            "would", "may", "might", "must", "ought", "despite", "over", "during", 
            "within", "without", "beneath", "upon", "except", "across", "towards", 
            "besides", "regarding", "concerning", "like", "throughout", "against", 
            "among", "whilst", "per", "along", "via", "its", "your", "his", "her", 
            "their", "our", "whom", "whose", "which", "what", "whichever", "whatever", 
            "whoever", "whomever", "these", "those", "this", "that", "my", "mine", "its", "something", "anything", 
            "nothing", "everything", "seem", "become", "keep", "stay", "yet", "still", "already", 
            "also", "inside", "outside", "beyond", "near", "quite", "rather", "almost", "too", 
            "therefore", "however", "thus", "otherwise", "thing", "stuff", "lot", "various", 
            "several", "numerous", "certain", "always", "never", "often", "seldom", "rarely", 
            "today", "yesterday", "tomorrow", "now", "then", "annually", "daily", "weekly", 
            "monthly", "yearly", "maybe", "perhaps", "possibly", "actually", "kind", "sort", 
            "type", "part", "form"
        ));

    
    // dic file just loaded for once :)
    static {
        ENGLISH_DICTIONARY = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/words.txt"))) {
//            logger.info("loading the dictionary:)");
            String line;
            while ((line = br.readLine()) != null) {
                // Extract the word, ignoring affixes and add it to the set
                ENGLISH_DICTIONARY.add(line.trim().toLowerCase());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load dictionary", e);
        }
    }
    //Make sure to filter out all HTML tags, to remove all punctuation, and to
    //convert everything to lower case.
    public static String retainSpecificFields(String htmlContent) {
        StringBuilder relevantText = new StringBuilder();

        // Extract title
        addMatches(relevantText, htmlContent, "<title>(.*?)</title>");
        
        // Extract headers
        addMatches(relevantText, htmlContent, "<h[1-6]>(.*?)</h[1-6]>");

        // Extract main body content
        addMatches(relevantText, htmlContent, "<p>(.*?)</p>|<article>(.*?)</article>");

        // Extract metadata
//        addMatches(relevantText, htmlContent, "<meta name=\"description\" content=\"(.*?)\"|<meta name=\"keywords\" content=\"(.*?)\"");

        // Process the extracted text
        String res = relevantText.toString().replaceAll("<[^>]+>", "");

        return res;
    }

    private static void addMatches(StringBuilder relevantText, String htmlContent, String regex) {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(htmlContent);
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                if (matcher.group(i) != null) {
                    relevantText.append(matcher.group(i)).append(" ");
                }
            }
        }
    }

    public static List<String> processCleanedPage(String text) {
        String cleanedText = text.toLowerCase().replaceAll("\\p{Punct}", " ");
        return Arrays.stream(cleanedText.split("\\s+"))
            .filter(word -> !word.isEmpty() && word.length() < 10 && !STOP_WORDS.contains(word) && ENGLISH_DICTIONARY.contains(word))
            .collect(Collectors.toList());
    }
}

