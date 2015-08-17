package com.pivotal.example.xd;

import java.net.URI;

import io.pivotal.spring.cloud.service.common.GemfireServiceInfo;

import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;




public class GemFireClient {
	
	private static GemFireClient instance = null;
	private String userName;
	private String password;
	private URI[] locators;
	public GemFireClient() {
		
		// TODO Auto-generated constructor stub
		CloudFactory cloudFactory = new CloudFactory();
		Cloud cloud = cloudFactory.getCloud();
		GemfireServiceInfo myService = (GemfireServiceInfo) cloud.getServiceInfo("ut-gemfire");

		locators = myService.getLocators();
		userName = myService.getUsername();
		password = myService.getPassword();
	
	}

	public String toString(){
		String str = new String();
		str = str + " [ username: " + userName
		+ " ] "
		+ " [ password : " + password;
		return str;
	}
	
	public static GemFireClient getInstance() {
		if(instance == null){
			instance = new GemFireClient();
		}
		return instance;
	}
}
