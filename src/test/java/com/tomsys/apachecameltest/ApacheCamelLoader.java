package com.tomsys.apachecameltest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApacheCamelLoader {
	
	@Autowired
	private CamelContext camelContext;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
//	@Test
//	public void test_pgp_encrypt() throws Exception {
//		
//		CamelContext context = new DefaultCamelContext();
//		context.addRoutes(new RouteBuilder() {
//
//			@Override
//			public void configure() throws Exception {
//
//				from("file:original?noop=true")
//					.marshal()
//					.pgp("public-key.gpg", "ewerton@example.com")
//				.to("file:encrypt?fileName=encrypted.enc");
//				
//			}
//			
//		});
//
//		context.start();
//		Thread.sleep(5000);
//		context.stop();
//	}
	
	@Test
	public void test_pgp_decrypt() throws Exception {
		
		doTheWholeThing();

		Integer countProducts = jdbcTemplate.queryForObject("Select count(id) from product", Integer.class);
		Integer countBrands = jdbcTemplate.queryForObject("Select count(id) from brand", Integer.class);
		
		assertThat(10).isEqualTo(countProducts);
		assertThat(7).isEqualTo(countBrands);
		
	}

	private void doTheWholeThing() throws Exception, InterruptedException {
		jdbcTemplate.execute("DELETE FROM product");
    	jdbcTemplate.execute("DELETE FROM brand");
		
		camelContext.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				from("file:encrypt?noop=true")
					.unmarshal()
					.pgp("private-key.gpg", "ewerton@example.com", null)
					.to("direct:csv");
				
				from("direct:csv")
					.routeId("csv-importer")
					.log("Calling csv transform")
					.unmarshal()
					.csv()
					.to("direct:insert-products");
			}
			
		});
		
		camelContext.start();
		Thread.sleep(5000);
		camelContext.stop();
	}

}

