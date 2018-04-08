package com.amazonaws.lambda.demo;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class SQSLambdaPolling implements RequestHandler<Object, String> {

	@Override
	public String handleRequest(Object input, Context context) {
		context.getLogger().log("Input: " + input);
		String queueURL = "https://sqs.us-east-1.amazonaws.com/383840367298/s3LambdaSQSQueue.fifo";
		AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueURL);
		List<Message> messages = sqs.receiveMessage(request).getMessages();
		context.getLogger().log("messages:" + messages);
		messages.forEach(message -> {
			String body = message.getBody();
			context.getLogger().log("body:" + body);
			processMessage(body);

			System.out.println("Deleting a message.\n");
			String messageReceiptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(queueURL, messageReceiptHandle));

		});

		return "Hello from Lambda!";
	}

	private void processMessage(String message) {
		AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
		String[] msg = message.split("=");
		String bucket = msg[0];
		String[] file = msg[1].split(":");

		CopyObjectRequest copy = new CopyObjectRequest(bucket, file[0], bucket, file[1]);
		client.copyObject(copy);

		DeleteObjectRequest delete = new DeleteObjectRequest(bucket, file[0]);
		client.deleteObject(delete);
	}

}
