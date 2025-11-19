# Use an official OpenJDK 11 runtime as a parent image
FROM eclipse-temurin:11-jre-alpine

 # Set the working directory in the container
WORKDIR /app

# Copy the JAR file into the container at /app
COPY newsfetcher-1.0.0-SNAPSHOT.jar /app/newsfetcher.jar

# Expose the specified port
EXPOSE 8166

# Set the time zone to match the host machine, also set for logo path
ENV TZ=Asia/Jakarta
ENV LOGO_PATH=/app/logos

# Install tzdata and set the time zone
RUN apk add --no-cache tzdata && \
    cp /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

# Run the JAR file with the specified command
CMD ["java", "-Djava.net.preferIPv4Stack=true", "-jar", "newsfetcher.jar"]