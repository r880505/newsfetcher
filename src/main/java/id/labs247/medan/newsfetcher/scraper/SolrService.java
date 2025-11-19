package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
// import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;

import id.labs247.medan.newsfetcher.configs.SolrConfig;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import java.util.ArrayList;
import java.util.Map;

public class SolrService {

    private static final Logger logger = LogManager.getLogger(SolrService.class);
    private SolrClient solrClient;
    private static final ExecutorService executorService = Executors.newFixedThreadPool(500); 
    private Boolean solrIsSecure;
    private Boolean solrIsCluster;
    private String solrHost;
    private String solrZkHosts;
    private String solrCollection;
    private List<String> zkHosts;

    public void init() throws IOException {
        logger.info("Initializing Solr Client");
        this.solrClient = getSolrClient();
        this.checkConnection(solrClient);
    }

    private void checkConnection(SolrClient solr) {
        try {
            // Ping the Solr server
            SolrPingResponse pingResponse = solr.ping();
            logger.info("Ping Response: " + pingResponse.getStatus());
            logger.info("Solr is up and running!");
        } catch (SolrServerException | IOException e) {
            logger.info("Error pinging Solr: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Boolean getSolrIsCluster() {
        return SolrConfig.getSolrIsCluster();
    }

    private Boolean getSolrIsSecure() {
        return SolrConfig.getSolrIsSecure();
    }

    private List<String> getSolrZkHosts() {
        solrZkHosts = SolrConfig.getSolrZkHosts();
        return Arrays.stream(solrZkHosts.split("\\s+"))
            .filter(host -> !host.trim().isEmpty())
            .collect(Collectors.toList());
    }

    private String getSolrHost() {
        return SolrConfig.getSolrHost();
    }

    private String getSolrCollection() {
        return SolrConfig.getSolrCollection();
    }

    // Insert to Solr as single data
    public Future<Boolean> sendToSolr(SolrInputDocument solrDocument) {
        return executorService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    // Initailize solr client
                    SolrClient solr = solrClient;
                    
                    if(solrClient==null) {
                        solr = getSolrClient();
                    } 
                    
                    // Add the document to Solr
                    solr.add(solrDocument);
            
                    // Commit the changes
                    solr.commit();
            
                    logger.info("[DEBUG] Solr | Successfully inserted to Solr as single entry");
                    return true;
                } catch (IOException | SolrServerException e) {
                    logger.error("[ERROR] Solr | Failed to insert to Solr | " + e.getMessage(), e);
                    throw e;
                }
            }
        });
    }

    // Insert to Solr as list
    public Future<Boolean> sendToSolr(List<SolrInputDocument> solrDocuments) {
        return executorService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    // Initailize solr client
                    SolrClient solr = solrClient;
                    
                    if(solrClient==null) {
                        solr = getSolrClient();
                    } 
                    
                    // Add the document to Solr
                    solr.add(solrDocuments);
            
                    // Commit the changes
                    solr.commit();
            
                    logger.info("[DEBUG] Solr | Successfully inserted to Solr as list");
                    return true;
                } catch (IOException | SolrServerException e) {
                    logger.error("[ERROR] Solr | Failed to insert to Solr | " + e.getMessage(), e);
                    throw e;
                }
            }
        });
    }

    private SolrClient getSolrClient() throws IOException {
        solrIsCluster = getSolrIsCluster();
        solrIsSecure = getSolrIsSecure();
        if (solrClient == null) {
            synchronized (SolrService.class) {
                if (solrIsSecure) {
                    // HttpClientUtil.addConfigurer(new Krb5HttpClientConfigurer());
                }
                if (solrIsCluster) {
                    zkHosts = getSolrZkHosts();
                    solrCollection = getSolrCollection();
                    CloudSolrClient.Builder builder = new CloudSolrClient.Builder().withZkHost(zkHosts).withZkChroot("/solr");
                    CloudSolrClient solrServer = builder.build();
                    solrServer.setDefaultCollection(solrCollection);
                    solrServer.connect();
                    solrClient = solrServer;
                } else {
                    solrHost = getSolrHost();
                    solrClient = new HttpSolrClient.Builder(solrHost).build();
                }
            }
        }
        return solrClient;
    }
    
}
