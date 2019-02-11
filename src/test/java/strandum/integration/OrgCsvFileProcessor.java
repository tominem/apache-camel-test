package strandum.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import strandum.business.service.impl.CodeServiceImpl.CodeServiceImpl;
import strandum.business.service.impl.PositionServiceImpl.PositionServiceImpl;
import strandum.persist.entity.impl.BECodeImpl;
import strandum.persist.entity.impl.BEPositionImpl;

public class OrgCsvFileProcessor implements Processor{
	
	private CodeServiceImpl codeServiceImpl;
	
	private PositionServiceImpl positionServiceImpl;
	
	public OrgCsvFileProcessor() {
	}
	
	@Autowired
	public OrgCsvFileProcessor(CodeServiceImpl codeServiceImpl, PositionServiceImpl positionServiceImpl) {
		this.codeServiceImpl = codeServiceImpl;
		this.positionServiceImpl = positionServiceImpl;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Exchange exchange) throws Exception {
		List<Map<String, Object>> data = (List<Map<String, Object>>) exchange.getIn().getBody();
		List<Integer> linesToIgnore = new ArrayList<Integer>();
		
		for (int i = 0; i < data.size(); i++) {
			
			Map<String, Object> line = data.get(i);
			
			if (linesToIgnore.contains(i) == false) {
				extractLine(data, line, linesToIgnore);
			}
			
		}
		
	}

	private void extractLine(List<Map<String, Object>> data, Map<String, Object> line, List<Integer> linesToIgnore) {
		//division
		BECodeImpl division = dealWithCodeTableValues("DIVISION", 
				line.get("division-code").toString(), 
				line.get("division-description").toString(),
				null);

		//department
		BECodeImpl department = dealWithCodeTableValues("DEPARTMENT", 
				line.get("department-code").toString(), 
				line.get("department-description").toString(),
				null);
		
		//countries
		BECodeImpl country = dealWithCodeTableValues("COUNTRIES", 
				line.get("country-code").toString(), 
				line.get("country-description").toString(),
				null);

		//org unit
		BECodeImpl orgUnit = dealWithCodeTableValues("ORG_UNITS", 
				line.get("org-unit-code").toString(), 
				line.get("org-unit-description").toString(),
				line.get("cost-code").toString());
		
		int positionIdx = Integer.valueOf(line.get("position-code").toString());
		String postionDescription = line.get("position-description").toString();
		int parentPositionIdx = Integer.valueOf(line.get("parent-position-code").toString());
		
		dealWithPosition(positionIdx, 
				postionDescription, 
				division.getCodeId(), 
				department.getCodeId(), 
				country.getCodeId(), 
				orgUnit.getCodeId(), 
				parentPositionIdx,
				data,
				line,
				linesToIgnore);
	}

	private void dealWithPosition(int positionIdx, String postionDescription, String divisionId, String departmentId, String countryId, String orgUnitId, int parentPositionIdx, List<Map<String,Object>> data, Map<String, Object> line, List<Integer> linesToIgnore) {
		BEPositionImpl currPosition = positionServiceImpl.findPositionByPositionId(positionIdx);
		BEPositionImpl parentPosition = positionServiceImpl.findPositionByPositionId(parentPositionIdx);
		
		if (currPosition == null) {
			currPosition = new BEPositionImpl();
			currPosition.setIndexId(positionIdx);
		}
		
		currPosition.setDescription(postionDescription);
		currPosition.setLevel1(divisionId);
		currPosition.setLevel3(departmentId);
		currPosition.setCountry(countryId);
		currPosition.setOrgUnit(orgUnitId);
		
		if(parentPosition != null) {
		
			positionServiceImpl.savePosition(currPosition, parentPosition.getIndexId());

		} else {
			int ppIdx = checkParentPosition(data, parentPositionIdx);
			
			if (ppIdx == -1) {
				throw new RuntimeException(String.format("Parent position %d not found for position %d", parentPositionIdx, currPosition.getIndexId()));
			}
			
			line = data.get(ppIdx);
			extractLine(data, line, linesToIgnore);
			linesToIgnore.add(ppIdx);
		}
		
	}

	private int checkParentPosition(List<Map<String, Object>> data, int parentPositionIdx) {
		return IntStream.range(0, data.size())
				.filter(i -> Integer.valueOf(data.get(i).get("position-code").toString()).equals(parentPositionIdx))
				.findFirst()
				.orElse(-1);
	}

	private BECodeImpl dealWithCodeTableValues(String table, String codeId, String description, String valueA1) {
		BECodeImpl codeFound = codeServiceImpl.getCodeByTableIdCodeId(table, codeId);
		if (codeFound == null) {
			codeFound = new BECodeImpl();
			codeFound.setCodeId(codeId);
		}
		codeFound.setDescription(description);
		if (valueA1 != null) {
			codeFound.setValueA1(valueA1);
		}

		codeServiceImpl.save(codeFound);
		
		return codeFound;
	}
	
}
