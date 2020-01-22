package com.cloud.aws.api.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class OperacionesConBucket {

	public static void main(String[] args) throws IOException {
		AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
		String bucketName = "hfuentepetestbucket";
		String key = "fichero-1";
		
		System.out.println("Creando bucket " + bucketName + "\n");
		s3client.createBucket(bucketName);
		System.out.println("Listando buckets");
		for (Bucket bucket : s3client.listBuckets()) {
			System.out.println(bucket.getName());
		}
		
		System.out.println("Subiendo fichero");
		File fichero = new File("fichero-a-subir-a-s3.txt");
		s3client.putObject(
				new PutObjectRequest(bucketName, key, fichero).withCannedAcl(CannedAccessControlList.PublicRead).withGeneralProgressListener(progressEvent -> System.out
						.print((progressEvent.getBytesTransferred() * 100 / fichero.length()) + "%")));
		
		
		System.out.println("Bajando fichero");
		S3Object object = s3client.getObject(new GetObjectRequest(bucketName, key));
		System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		while (true) {
			String linea = reader.readLine();
			if (linea == null)
				break;
			System.out.println(linea);
		}
		
		System.out.println("Lista de objetos en el bucket");
		ObjectListing objectListing = s3client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(2));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			System.out.println(" - " + objectSummary.getKey() + " " + "(size = " + objectSummary.getSize() + ")");
		}
		
		System.out.println("Borrando objetos");
		s3client.deleteObject(bucketName, key);
		
		System.out.println("Borrando bucket " + bucketName + "\n");
		s3client.deleteBucket(bucketName);
	}

}
