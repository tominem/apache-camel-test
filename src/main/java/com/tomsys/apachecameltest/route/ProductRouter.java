package com.tomsys.apachecameltest.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class ProductRouter extends RouteBuilder{
	
	@Override
	public void configure() throws Exception {
		
		from("direct:insert-products")
			.log("Inserting new Products")
			.routeId("product-handler")
			.bean("productHandler", "insertProducts")
        	.to("mock:test");
	}
	
}
