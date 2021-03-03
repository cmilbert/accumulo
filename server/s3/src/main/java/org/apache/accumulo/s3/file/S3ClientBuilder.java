package org.apache.accumulo.s3.file;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3ClientBuilder {

  public S3ClientBuilder() {}

  public static AmazonS3ClientBuilder builder(AWSCredentialsProvider credentialsProvider,
      AmazonS3ClientBuilder s3ClientBuilder) {
    if (s3ClientBuilder == null) {
      return AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider);
    } else {
      return s3ClientBuilder.withCredentials(credentialsProvider);
    }
  }
}
