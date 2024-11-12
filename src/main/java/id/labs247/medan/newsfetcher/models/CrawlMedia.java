package id.labs247.medan.newsfetcher.models;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class CrawlMedia {

    private Long mediaId;
    private String originalDomain;
    private String landingUrl;
    private LocalDateTime lastScheduled;
    private Integer scheduleMinutes;
    private LocalDateTime lastUpdate;
    private Integer status;
    private String mediaLogo;
    private String urlSelect;
    private String contentSelect;
    private Integer parseAuto;
    private Integer maxDepth;
    private String bacajugaSelect;
    private String imageSelect;
    private Long dateFormatId;
    private String pageParam;
    private Integer indexPageCount;
    private String extra;
    private Integer extraStatus;


}