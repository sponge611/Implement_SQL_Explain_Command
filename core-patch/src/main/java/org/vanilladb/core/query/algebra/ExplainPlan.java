package org.vanilladb.core.query.algebra;


import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {
	
	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;
	

	
	public ExplainPlan(Plan p) {
		this.p = p;
		this.schema.addField("query-plan", Type.VARCHAR(500));
		
	}
	@Override
	public Scan open() {
		Scan s = p.open();
		
		return new ExplainScan(s);
	}

	@Override
	public long blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return schema;
	}

	@Override
	public Histogram histogram() {
		// TODO Auto-generated method stub
		hist = new Histogram();
		return hist;
	}

	@Override
	public long recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

}
