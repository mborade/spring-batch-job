package io.spring.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import io.spring.domain.Foo;

public class FooRowMapper<T> implements RowMapper<Foo> {

	@Override
	public Foo mapRow(ResultSet rs, int rowNum) throws SQLException {
		Foo foo = new Foo();
		foo.setNcbi_id(rs.getInt("ncbi_id"));
		foo.setSpecies(rs.getString("species"));
        return foo;
	}

}
