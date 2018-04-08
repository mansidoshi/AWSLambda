package com.amazonaws.lambda.demo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class S3LambdaSQSFunction implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	public S3LambdaSQSFunction() {
	}

	// Test purpose only.
	S3LambdaSQSFunction(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		String queueURL = "https://sqs.us-east-1.amazonaws.com/383840367298/s3LambdaSQSQueue.fifo";
		context.getLogger().log("Received event: " + event);

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		try {
			// S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
			// String contentType = response.getObjectMetadata().getContentType();
			// context.getLogger().log("CONTENT TYPE: " + contentType);
			String message = String.join("", bucket, "=", key, ":", key.toUpperCase());

			System.out.println("Sending a message to MyFifoQueue.fifo.\n");
			final SendMessageRequest sendMessageRequest = new SendMessageRequest(queueURL, message);
			sendMessageRequest.setMessageGroupId("messageId1");
			sendMessageRequest.setMessageDeduplicationId("messageDeduplicationId1");
			final SendMessageResult result = sqs.sendMessage(sendMessageRequest);
			return result.getMessageId();

		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", key, bucket));
			throw e;
		}
	}
}