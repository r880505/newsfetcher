package id.labs247.medan.newsfetcher.models;

import lombok.Data;

@Data
public class UrlFormat {

    private Long id;
    private String format;
    private int multiplier;
    private int substractor;
    private Long mediaId;

}

