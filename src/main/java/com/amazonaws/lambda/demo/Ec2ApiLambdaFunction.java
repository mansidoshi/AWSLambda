package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.time.Period;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.util.StringUtils;

public class Ec2ApiLambdaFunction implements RequestStreamHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

		LambdaLogger logger = context.getLogger();
		logger.log("Loading Java Lambda handler of ProxyWithStream");

		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		JSONParser parser = new JSONParser();
		String dob = null;
		JSONObject event;
		try {
			event = (JSONObject) parser.parse(reader);
			if (event.get("body") != null) {
				JSONObject body = (JSONObject) parser.parse((String) event.get("body"));
				if (body.get("dob") != null) {
					dob = (String) body.get("dob");
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		int month = Integer.valueOf(dob.substring(0, 2));
		int day = Integer.valueOf(dob.substring(2, 4));
		int year = Integer.valueOf(dob.substring(4));

		LocalDate today = LocalDate.now();
		LocalDate birthday = LocalDate.of(year, month, day);

		Period period = Period.between(birthday, today);

		// Now access the values as below
		System.out.println(period.getDays());
		System.out.println(period.getMonths());
		System.out.println(period.getYears());

		JSONObject responseJson = new JSONObject();
		String responseCode = "200";
		String age = StringUtils.join(",", String.valueOf(period.getYears()) + "years",
				String.valueOf(period.getMonths()) + "months", String.valueOf(period.getDays() + "days"));

		JSONObject responseBody = new JSONObject();
		responseBody.put("age", age);
		JSONObject headerJson = new JSONObject();
		headerJson.put("x-custom-header", "ec2-header");

		responseJson.put("isBase64Encoded", false);
		responseJson.put("statusCode", responseCode);
		responseJson.put("headers", headerJson);
		responseJson.put("body", responseBody.toString());

		logger.log(responseJson.toJSONString());
		OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
		writer.write(responseJson.toJSONString());
		writer.close();
	}
}
