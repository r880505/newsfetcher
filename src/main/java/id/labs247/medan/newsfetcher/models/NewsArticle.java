package id.labs247.medan.newsfetcher.models;

import lombok.Data;

@Data
public class NewsArticle {

    private String title;
    private String image;
    private String author;
    private String publishedDate;

}
