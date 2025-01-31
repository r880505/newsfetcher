package id.labs247.medan.newsfetcher.models;

import lombok.Data;

@Data
public class CrawlExtraComment {

    private Long id;
    private Long mediaId;
    private String commentApi;
    private String requestMethod;
    private String requestBody;
    private String requestParam;
    private String articleIdSelect;
    private String cookie;
    private String selectorComment;
    
}
