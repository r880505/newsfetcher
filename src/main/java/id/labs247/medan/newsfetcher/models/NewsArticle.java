package id.labs247.medan.newsfetcher.models;

import java.util.List;

import lombok.Data;

@Data
public class NewsArticle {

    private String title;
    private String datePublished;
    private List<String> author;
    
}
