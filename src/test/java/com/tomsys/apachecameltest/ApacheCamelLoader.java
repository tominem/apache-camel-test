package com.tomsys.apachecameltest;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Test;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class ApacheCamelLoader {
	
	@Test
	public void test_pgp_encypt() throws Exception {
		
		CamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				from("file:original?noop=true")
					.marshal()
					.pgp("public-key.gpg", "ewerton@example.com")
				.to("file:encrypt?fileName=encrypted.enc");
				
			}
			
		});

		context.start();
		Thread.sleep(5000);
		context.stop();
	}
	
	@Test
	public void test_pgp_decrypt() throws Exception {
		
		CamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				from("file:encrypt?noop=true;fileName=encrypted.enc")
					.unmarshal()
					.pgp("private-key.gpg", "ewerton@example.com", null)
					.log(LoggingLevel.DEBUG, "${body}")
				.to("file:decrypt?fileName=decrypted.csv").log(LoggingLevel.DEBUG, "${body}");
				
			}
			
		});

		context.start();
		Thread.sleep(5000);
		context.stop();
	}

}

