package com.tomsys.apachecameltest;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;


public class CSVTest extends CamelTestSupport {
	
	@Test
	public void test() throws InterruptedException {
		MockEndpoint mockEndpoint = getMockEndpoint("mock:result");
		mockEndpoint.expectedMessageCount(3);
		
		assertMockEndpointsSatisfied();
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                
            	CsvDataFormat csvFormat = new CsvDataFormat();
            	csvFormat.setDelimiter('|');
            	csvFormat.setLazyLoad(true);
            	
            	from("file://src/test/resources/out?noop=true")
            		.unmarshal(csvFormat)
            		.convertBodyTo(List.class)
            		.split(body())
            		.validate(PredicateBuilder.and(simple("${body.size} == 2")))
            		.process(new Processor() {
						
						@Override
						public void process(Exchange exchange) throws Exception {
							System.out.println("Prop="+ exchange.getProperty("position.id"));
						}
					})
            	.to("mock:result");
            }
        };
	}

}
