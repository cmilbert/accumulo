package com.amazon.morocco.mss.file;

import java.io.IOException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

/**
 * Wrap calls to S3 and apply generic exception handling, including retires on service exceptions,
 * handling of eventual consistency, and promotion of RuntimeExceptions to IOExceptions
 *
 * @author afuchs
 */
public class S3ClientWrapper {

  private final AmazonS3 s3;

  public S3ClientWrapper(AmazonS3 s3) {
    this.s3 = s3;
  }

  private void throwIOExceptionIfNotMissing(SdkClientException e) throws IOException {
    if (e instanceof AmazonServiceException) {
      if (((AmazonServiceException) e).getStatusCode() == 404) {
        return;
      }
    }
    throw new IOException(e);
  }

  public ObjectListing listObjects(ListObjectsRequest listReq) throws IOException {
    try {
      return s3.listObjects(listReq);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public void deleteObjects(DeleteObjectsRequest req) throws IOException {
    try {
      s3.deleteObjects(req);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public boolean deleteObject(String bucketName, String objectName) throws IOException {
    try {
      s3.deleteObject(bucketName, objectName);
      return true;
    } catch (SdkClientException e) {
      throwIOExceptionIfNotMissing(e);
      return false;
    }
  }

  public ObjectMetadata getObjectMetadata(String bucketName, String objectName) throws IOException {
    try {
      return s3.getObjectMetadata(bucketName, objectName);
    } catch (SdkClientException e) {
      throwIOExceptionIfNotMissing(e);
      return null;
    }
  }

  public ObjectListing listObjects(String bucketName, String dirName) throws IOException {
    try {
      return s3.listObjects(bucketName, dirName);
    } catch (SdkClientException e) {
      throwIOExceptionIfNotMissing(e);
      return null;
    }
  }

  public PartListing listParts(ListPartsRequest listPartsRequest) throws IOException {
    try {
      return s3.listParts(listPartsRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest)
      throws IOException {
    try {
      s3.abortMultipartUpload(abortMultipartUploadRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public void completeMultipartUpload(CompleteMultipartUploadRequest completeRequest)
      throws IOException {
    try {
      s3.completeMultipartUpload(completeRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest req)
      throws IOException {
    try {
      return s3.listMultipartUploads(req);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public S3Object getObject(GetObjectRequest getObjectRequest) throws IOException {
    try {
      return s3.getObject(getObjectRequest);
    } catch (SdkClientException e) {
      throwIOExceptionIfNotMissing(e);
      return null;
    }
  }

  public S3Object getObjectThrowIfMissing(GetObjectRequest getObjectRequest) throws IOException {
    try {
      return s3.getObject(getObjectRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public InitiateMultipartUploadResult
      initiateMultipartUpload(InitiateMultipartUploadRequest request) throws IOException {
    try {
      return s3.initiateMultipartUpload(request);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public UploadPartResult uploadPart(UploadPartRequest partRequest) throws IOException {
    try {
      return s3.uploadPart(partRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws IOException {
    try {
      return s3.putObject(putObjectRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request)
      throws IOException {
    try {
      return s3.getCachedResponseMetadata(request);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }
}
