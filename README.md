# Media Online Crawler
#### Description
Scrape news from popular news portal

### Requirements
- Java v-1.8 (Build)
- Apache Maven v-3.6.3
- Apache Kafka v-2.7.1
- Apache Zookeeper v-3.6.1
- Apache Solr v-9.5.0
- MySQL v-15.1 Distrib 10.6.12-MariaDB

### Build and Run
Note: before run this project, you must run Zookeeper, Kafka and Solr first, adjust Database (Postgres, MySQL or others), Kafka, Solr, url checker API as yours in application.properties

#### Run directly without build jar
Use this command if you want to run directly without build the jar file first

Clean package
```
mvn clean package
```
Run the project
```
mvn -X exec:java -Dexec.mainClass="id.labs247.medan.newsfetcher.MainApplication"
```

#### Run with jar file
Build jar file
```
mvn clean install
```
Run jar file
```
java -jar /path_to_jar_file/newsfetcher-1.0.0-SNAPSHOT.jar
```
If you use kerberos for authenticate, use this command
```
java \
-DkerberosPrincipal=your_kerberos_principal \
-DkerberosKeytab=/path/to/keytab/file/your_keytab.keytab \
-Djavax.security.auth.useSubjectCredsOnly=false \
-Djava.security.auth.login.config=/path/to/config/file/your_config.conf \
-jar /path/to/jar/file/newsfetcher-1.0.0-SNAPSHOT.jar

```
