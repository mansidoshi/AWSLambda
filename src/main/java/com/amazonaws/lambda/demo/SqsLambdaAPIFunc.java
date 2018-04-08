package com.amazonaws.lambda.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SqsLambdaAPIFunc implements RequestStreamHandler {

	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
		context.getLogger().log("Input: " + input);
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> messageMap = new HashMap<>();
		Map<String, String> bodyMap = new HashMap<>();
		try {
			messageMap = mapper.readValue(input, Map.class);
			context.getLogger().log("Input: " + messageMap);
			String body = messageMap.get("body");
			bodyMap = mapper.readValue(body, Map.class);
			context.getLogger().log("body: " + bodyMap);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String type = bodyMap.get("Type");
		if (type.equalsIgnoreCase("SubscriptionConfirmation")) {
			String token = bodyMap.get("Token");
			context.getLogger().log("token:" + token);
			confirmSubmission(token);
		} else if (type.equalsIgnoreCase("Notification")) {
			String message = bodyMap.get("Message");
			context.getLogger().log("message:" + message);
			processMessage(message);
		}

	}

	private void confirmSubmission(String token) {
		if (token != null) {
			AmazonSNS service = AmazonSNSClientBuilder.defaultClient();
			// Confirm subscription
			ConfirmSubscriptionRequest confirmReq = new ConfirmSubscriptionRequest()
					.withTopicArn("arn:aws:sns:us-east-1:383840367298:SqsApiS3Topic").withToken(token);
			service.confirmSubscription(confirmReq);
		}
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