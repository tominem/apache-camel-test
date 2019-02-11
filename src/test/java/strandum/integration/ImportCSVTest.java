package strandum.integration;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.bind.JAXBException;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.impl.PropertyPlaceholderDelegateRegistry;
import org.apache.camel.model.ModelHelper;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.dataformat.CsvDataFormat;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import strandum.business.service.impl.CodeServiceImpl.CodeServiceImpl;
import strandum.business.service.impl.PositionServiceImpl.PositionServiceImpl;
import strandum.persist.entity.impl.BECodeImpl;
import strandum.persist.entity.impl.BEPositionImpl;

public class ImportCSVTest extends CamelTestSupport{
	
	@Mock
	private CodeServiceImpl codeServiceImpl;

	@Mock
	private PositionServiceImpl positionServiceImpl;

	private String codeServiceRef;
	private String positionServiceRef;
	private String orgCsvProcessor;
	
	@Override
	protected void doPreSetup() throws Exception {
		MockitoAnnotations.initMocks(this);
		codeServiceRef      = "codeService";
		positionServiceRef  = "positionService";
		orgCsvProcessor     = "orgCsvFileProcessor";
	}
	
	@Override
	protected CamelContext createCamelContext() throws Exception {
		CamelContext context = super.createCamelContext();
		JndiRegistry registry = (JndiRegistry)((PropertyPlaceholderDelegateRegistry)context.getRegistry()).getRegistry();
		registry.bind(orgCsvProcessor, new OrgCsvFileProcessor(codeServiceImpl, positionServiceImpl));
		return context;
	}
	
	@Override
	protected void doPostSetup() throws Exception {
//		RoutesDefinition routes = context.loadRoutesDefinition(new FileInputStream("test/camel/route-definition1.xml"));
//		context.addRouteDefinitions(routes.getRoutes());
	}
	
	@Test
	public void readCSV() throws Exception {
		
		BECodeImpl[] codeInDatabase = {
			//First Line	
			
			getCode("A1","Market Management"), 	    //Division
			getCode("HL","Helpline"), 			    //Department 
			getCode("IE","IRL Park West"),          //Country
			getCode("3070000072","Product Devel"),  //Organization Unit
			
			//Second Line	
			
			getCode("A1","Market Management"), 	    //Division
			getCode("HL","Helpline"), 			    //Department 
			getCode("IE","IRL Park West"),          //Country
			getCode("3070000072","Product Devel"),  //Organization Unit

			//Third Line	
			
			getCode("A1","Market Management"), 	    //Division
			getCode("HL","Helpline"), 			    //Department 
			getCode("IE","IRL Park West"),          //Country
			getCode("3070000072","Product Devel"),  //Organization Unit

			//Fourth Line	
			
			getCode("A1","Market Management"), 	    //Division
			getCode("HL","Helpline"), 			    //Department 
			getCode("IE","IRL Park West"),          //Country
			getCode("3070000072","Product Devel"),  //Organization Unit

			//Fifth Line	
			
			getCode("A1","Market Management"), 	    //Division
			getCode("HL","Helpline"), 			    //Department 
			getCode("IE","IRL Park West"),          //Country
			getCode("3070000072","Product Devel"),  //Organization Unit

			//Sixth Line
			
			getCode("A2","IT Management"), 	       //Division
			getCode("HR","Human Resources"), 	   //Department 
			getCode("IE","IRL Park West"),         //Country
			getCode("3070000072","Product Devel")  //Organization Unit
		};
		
		BEPositionImpl[] positionsInDatabase = {
		    null,
		    null,
		    null,
		    null,
		    null,
		    null,
			getPosition(1, "Position 1", "Level1", "Level31", "Country1", "OrgUnit1"),
			getPosition(2, "Position 2", "Level1", "Level31", "Country1", "OrgUnit1"),
			getPosition(3, "Position 3", "Level1", "Level31", "Country1", "OrgUnit1")
		};
		
		when(codeServiceImpl.getCodeByTableIdCodeId(anyString(), anyString()))
			.thenReturn(codeInDatabase[0], getArrayForRange(codeInDatabase, 1, codeInDatabase.length));

		when(positionServiceImpl.findPositionByPositionId(anyInt()))
			.thenReturn(positionsInDatabase[0], getArrayForRange(positionsInDatabase, 1, positionsInDatabase.length));

		MockEndpoint mock = getMockEndpoint("mock:result");
		mock.expectedMessageCount(6);

		ArgumentCaptor<BECodeImpl> codeCaptor = ArgumentCaptor.forClass(BECodeImpl.class);

		ArgumentCaptor<BEPositionImpl> positionCaptor = ArgumentCaptor.forClass(BEPositionImpl.class);
		ArgumentCaptor<Integer> parentPositionCaptor = ArgumentCaptor.forClass(Integer.class);

		assertMockEndpointsSatisfied();

		verify(codeServiceImpl, times(codeInDatabase.length)).save(codeCaptor.capture());
		verify(positionServiceImpl, times(6)).savePosition(positionCaptor.capture(), parentPositionCaptor.capture());
		
		// check results from save method
		for (int i = 0; i < codeInDatabase.length; i++) {
			assertEquals(codeInDatabase[i], codeCaptor.getAllValues().get(i));
		}
		
		//dumpRouteXml();
		
	}
	
	private BEPositionImpl getPosition(int positionIdx, String description, String level1, String level3, String contry, String orgUnit) {
		BEPositionImpl position = new BEPositionImpl();
		position.setIndexId(positionIdx);
		position.setDescription(description);
		position.setLevel1(level1);
		position.setLevel3(level3);
		position.setCountry(contry);
		position.setOrgUnit(orgUnit);
		return position;
	}

	@SuppressWarnings("unchecked")
	private <T> T[] getArrayForRange(T[] array, int first, int length) {
		List<T> values = IntStream.range(first, length)
				.mapToObj(i -> array[i]).collect(Collectors.toList());
		
		Class<T[]> class1 = (Class<T[]>) array.getClass();
		T[] buffer = class1.cast(Array.newInstance(class1.getComponentType(), length-first));
		
		return values.toArray(buffer);
	}
	
	private void dumpRouteXml() {
		final StringBuffer buf = new StringBuffer();
		context.getRoutes().forEach(r -> {
			try {
				RouteDefinition def = context.getRouteDefinition(r.getId());
				if (def != null) {
					String xml = ModelHelper.dumpModelAsXml(context, def);
					buf.append(xml);
				}
			} catch (JAXBException e) {
				throw new RuntimeException(e);
			}
		});
		
		System.out.println(buf.toString());
	}

	private static BECodeImpl getCode(String id, String description) {
		return getCode(id, description, null);
	}
	
	private static BECodeImpl getCode(String id, String description, String changeUser) {
		BECodeImpl code = new BECodeImpl();
		code.setCodeId(id);
		code.setDescription(description);
		code.setChangeUser(changeUser);
		return code;
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		return new RouteBuilder() {
			

//			private void procSetHeader() {
//				from("direct:procedure-set-header")  
//					.id("direct:procedure-set-header")
//					.setProperty("division-code",          simple("${header.row[0]}"))
//			 		.setProperty("division-description",   simple("${header.row[1]}"))
//			 		.setProperty("department-code",        simple("${header.row[2]}"))
//			 		.setProperty("department-description", simple("${header.row[3]}"))
//			 		.setProperty("country-code",   		   simple("${header.row[4]}"))
//			 		.setProperty("country-description",    simple("${header.row[5]}"))
//			 		.setProperty("position-code")   	   .groovy("request.headers.get('row')[6] as Integer")
//			 		.setProperty("position-description",   simple("${header.row[7]}"))
//			 		.setProperty("parent-position-code")   .groovy("request.headers.get('row')[8] as Integer")
//			 		.setProperty("org-unit-code",   	   simple("${header.row[9]}"))
//			 		.setProperty("org-unit-description",   simple("${header.row[10]}"))
//			 		.setProperty("cost-code",   		   simple("${header.row[11]}"))
//			 		.setProperty("position-status",   	   simple("${header.row[12]}"))
//			 	.end();
//			}
			
			@Override
			public void configure() throws Exception {
				
				CsvDataFormat csv = new CsvDataFormat();
				csv.setLazyLoad(true);
				csv.setDelimiter("|");
				csv.setTrim(true);
				csv.setHeader(Arrays.asList(
					"division-code",         
					"division-description",  	
					"department-code",       	
					"department-description",	
					"country-code",   		  	
					"country-description",   	
					"position-code",   	  	
					"position-description",  	
					"parent-position-code",  	
					"org-unit-code",   	  	
					"org-unit-description",  	
					"cost-code",   		  	
					"position-status"   	  	
				));
				csv.setUseMaps(true);
				
				// define procedures routes
//				procSetHeader();
				
				// main route
//				from("file://test/out?noop=true")
				from("file://src/test/resources/out?noop=true")
					.id("import-csv-org-file")
					.log("fileName= ${header.camelFileName}")
				 	.unmarshal(csv)
				 	.convertBodyTo(List.class)
				 	.process(orgCsvProcessor)
				 	.to("mock:result");
//			 		.split()
//				 		.body()
//				 		.setProperty("current-line-idx", simple("${exchangeProperty.CamelSplitIndex}"))
//				 		.setProperty("current-line", simple("${body}"))
//				 		.choice()
//				 			.when(simple("${body.size} == 13"))
//					 			.setHeader("row", simple("${body}"))
//					 			.to("direct:procedure-set-header")
//					 			
//					 			.to("direct:handle-division")
//				 				
//				 				.endChoice()
//				 			.otherwise()
//				 			 	.log(LoggingLevel.ERROR, "Body is different than 13") //integrate with hook stages
//				 			 	.endChoice()
//				 		.end()
//				 		.to("mock:result") // calls mock:result
//			 		.end()
//			 		.log(LoggingLevel.INFO, "FINISHED !!!!!"); //maybe insert pending position
				
				// route to deal with division
//				from("direct:handle-division")
//					.to("direct:procedure-clean-code-props")
//					.setProperty("code-header", constant("division"))
//	 				.setProperty("code-table", constant("DIVISION"))
//	 				.setProperty("code-id", simple("${property.division-code}"))
//	 				.setProperty("code-description", simple("${property.division-description}"))
//	 				.setProperty("next-route", constant("direct:handle-department"))
// 				.to("direct:procedure-code-table-values");
//
//				// route to deal with department
//				from("direct:handle-department")
//					.to("direct:procedure-clean-code-props")
//					.setProperty("code-header", constant("department"))
//					.setProperty("code-table", constant("DEPARTMENT"))
//					.setProperty("code-id", simple("${property.department-code}"))
//					.setProperty("code-description", simple("${property.department-description}"))
//					.setProperty("next-route", constant("direct:handle-country"))
//				.to("direct:procedure-code-table-values");
//
//				// route to deal with country
//				from("direct:handle-country")
//					.to("direct:procedure-clean-code-props")
//					.setProperty("code-header", constant("country"))
//					.setProperty("code-table", constant("COUNTRIES"))
//					.setProperty("code-id", simple("${property.country-code}"))
//					.setProperty("code-description", simple("${property.country-description}"))
//					.setProperty("next-route", constant("direct:handle-org-unit"))
//				.to("direct:procedure-code-table-values");
//
//				// route to deal with org unit
//				from("direct:handle-org-unit")
//					.to("direct:procedure-clean-code-props")
//					.setProperty("code-header", constant("org-unit"))
//					.setProperty("code-table", constant("ORG_UNITS"))
//					.setProperty("code-id", simple("${property.org-unit-code}"))
//					.setProperty("code-description", simple("${property.org-unit-description}"))
//					.setProperty("code-value-a1", simple("${property.cost-code}"))
//					.setProperty("next-route", constant("direct:handle-position"))
//				.to("direct:procedure-code-table-values");
//				
//				// route to deal with positions
//				from("direct:handle-position")
//					.id("handle-with-position")
//					.setProperty("current-position", simple("null")) // initialize header
//					.setProperty("parent-position", simple("null"))  // initialize header
//				    .bean(positionServiceRef, "findPositionByPositionId(${property.position-code})")
//				    .log("return findPositionByPositionId(${property.position-code}) = ${body}")
//				    .setProperty("current-position", simple("${body}"))
//				    .bean(positionServiceRef, "findPositionByPositionId(${property.parent-position-code})")
//				    .log("return findPositionByPositionId(${property.parent-position-code}) = ${body}")
//				    .setProperty("parent-position", simple("${body}"))
//				    .choice()
//					    .when(simple("${property.parent-position} == null"))
//					    	.log("PORRA")
//					    	.to("direct:procedure-set-position-fields")  
//					    	.to("direct:procedure-check-parent-position-recursively") 
//					    	.endChoice()
//				    	.when(simple("${property.current-position} == null or ${property.current-position} != null and ${property.parent-position} != null"))
//				    		.to("direct:procedure-set-position-fields") 
//				    		.to("direct:procedure-save-position")  		
//				    		.endChoice()
//				    	.otherwise()
//				    		.log(LoggingLevel.ERROR, "There is not parent position ${property.parent-position-code} for the current position ${property.position-code}")
//				    		.endChoice()
//				    .end()
//				.end();
//				
//				// route store in memory pending-positions
//				from("direct:procedure-check-parent-position-recursively")
//					.id("procedure-check-parent-position-recursively")
//					.setProperty("target-parent-position-code").simple("${property.parent-position-code}")
//					.log("FILE= ${header.camelFileName}")
//					.setHeader("csvFile", simple("${header.camelFileName}"))
//					.from("file://test/out?noop=true&fileName=org.csv")
//					.unmarshal(csv)
//					.convertBodyTo(List.class)
////					.split()
////						.body()
////						.log("read the file from line ${property.current-line-idx}: ${body}")
////						.setHeader("row", simple("${body}"))
////						.to("direct:procedure-set-header") //set headers again
////						.choice()
////							.when(simple("${exchangeProperty.CamelSplitIndex} > ${property.current-line-idx}"))
////								.to("direct:procedure-check-parent-position")
////								.endChoice()
////							.otherwise()
////								.setHeader("row", simple("${property.current-line}"))
////								.to("direct:procedure-set-header") //set headers again
////								.endChoice()
////						.end()
////					.end()
//				.end();
//				
//				from("direct:procedure-check-parent-position")
//					.id("procedure-check-parent-position")
//					.choice()
//						.when(simple("${property.position-code} == ${property.target-parent-position-code}"))
//							.to("direct:handle-position")
//							.setHeader("row", simple("${property.current-line}"))
//							.to("direct:procedure-set-header")
//							.to("direct:handle-position") //save current position
//							.endChoice()
//					.end()
//				.end();
//				
//				// route to set position fields into either an existing position or a new one
//				from("direct:procedure-set-position-fields")
//					.id("procedure-set-position-fields")
//					.choice()
//						.when(simple("${property.current-position} == null"))
//							.setProperty("current-position").groovy("new strandum.persist.entity.impl.BEPositionImpl()")
//							.log("new position instantiated")
//						.endChoice()
//					.end()
//					.script().groovy("exchange.properties.get('current-position').setIndexId(exchange.properties.get('position-code'))") 			//Position Code
//					.script().groovy("exchange.properties.get('current-position').setDescription(exchange.properties.get('position-description'))") //Position Description
//					.script().groovy("exchange.properties.get('current-position').setLevel1(exchange.properties.get('division-code'))")   	        //Division Code
//					.script().groovy("exchange.properties.get('current-position').setLevel3(exchange.properties.get('department-code'))")           //Department Code
//					.script().groovy("exchange.properties.get('current-position').setCountry(exchange.properties.get('country-code'))")             //Country Code
//					.script().groovy("exchange.properties.get('current-position').setOrgUnit(exchange.properties.get('org-unit-code'))")            //Division Code
//					.log("${property.current-position}")
//				.end();
//				
//				// route to save position
//				from("direct:procedure-save-position")
//					.id("procedure-procedure-save-position")
//					.bean(positionServiceRef, "savePosition(${property.current-position}, ${property.parent-position.getIndexId()})")
//				.end();
//				
//				from("direct:procedure-code-table-values")
//				 	.id("procedure-code-table-values")
//				 	.bean(codeServiceRef, "getCodeByTableIdCodeId(${property.code-table}, ${property.code-id}})")
//				 	.choice()
//				 		.when(simple("${body} != null"))
//				 			.setHeader("${property.code-header}", body())
//				 			.script().groovy("request.headers.get(exchange.properties.get('code-header')).setDescription(exchange.properties.get('code-description'))")
//				 			.script().groovy("request.headers.get(exchange.properties.get('code-header')).setValueA1(exchange.properties.get('code-value-a1'))")
//				 			.setProperty("code-param", header("${property.code-header}"))
//				 			.endChoice()
//				 		.otherwise()
//					 		.setHeader("${property.code-header}")
//			 					.groovy("def strandum.persist.entity.impl.BECodeImpl code = new strandum.persist.entity.impl.BECodeImpl();"
//			 						  +	"code.setCodeId(exchange.properties.get('code-id')); "
//			 						  +	"code.setDescription(exchange.properties.get('code-description')); "
//			 						  +	"code.setValueA1(exchange.properties.get('code-value-a1')); "
//				 					  + "return code;")
//			 				.setProperty("code-param", header("${property.code-header}"))
//			 				.endChoice()
//				 	.end()
//					.bean(codeServiceRef, "save(${property.code-param})")
//					.log("code-table= ${property.code-table} - " + codeServiceRef + ".save(${property.code-param})")
//					.recipientList(simple("${property.next-route}"))
//				 .end();
//				
//				from("direct:procedure-clean-code-props")
//					.setProperty("code-header", 	 simple("null"))
//					.setProperty("code-table", 		 simple("null"))
//					.setProperty("code-id", 		 simple("null"))
//					.setProperty("code-description", simple("null"))
//					.setProperty("code-value-a1", 	 simple("null"))
//				.end();
				 	
			}
		};
	}
	
}
