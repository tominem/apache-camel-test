package com.tomsys.apachecameltest.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ProductHandler {
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	private String INSERT_BRAND_SQL = "INSERT INTO brand (name) VALUES (?) ON DUPLICATE KEY UPDATE name = VALUES(name)";
	private String SELECT_BRAND_BY_NAME_SQL = "SELECT id FROM brand WHERE name = ?";
	private String INSERT_PRODUCT_SQL = "INSERT INTO product (id, name, brand_id) VALUES (?, ?, ?)";
	
	@Transactional
	public void insertProducts(List<List<String>> data) {
		
		for (List<String> line : data) {
			int brandId = insertOrUpdateBrandAndReturnKey(line);
			insertProduct(line, brandId);
		}
		
	}

	private void insertProduct(List<String> line, int brandId) {
		jdbcTemplate.update(INSERT_PRODUCT_SQL, new PreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, Integer.parseInt(line.get(0)));
                ps.setString(2, line.get(1));
                ps.setInt(3, brandId);
			}
		});
	}

	private int insertOrUpdateBrandAndReturnKey(List<String> line) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		String brandName = line.get(2);
		jdbcTemplate.update(new PreparedStatementCreator() {
			
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps =
		                con.prepareStatement(INSERT_BRAND_SQL, new String[] {"id"});
					ps.setString(1, brandName);
	            return ps;
			}
		}, keyHolder);
		
		if (keyHolder.getKey() == null) {
			Integer id = jdbcTemplate.queryForObject(SELECT_BRAND_BY_NAME_SQL, new String[] {brandName}, Integer.class);
			return id;
		}
		
		return keyHolder.getKey().intValue();
	}

}
