/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The Scan class corresponding to a table. A table scan is just a wrapper for a
 * RecordFile object; most methods just delegate to the corresponding RecordFile
 * methods.
 */
public class TableScan implements UpdateScan {
	private RecordFile rf;
	private Schema schema;
	private long blockAccess = 0;
	private long recordsOutput = 0;
	private String tablename;

	/**
	 * Creates a new table scan, and opens its corresponding record file.
	 * 
	 * @param ti
	 *            the table's metadata
	 * @param tx
	 *            the calling transaction
	 */
	public TableScan(TableInfo ti, Transaction tx) {
		rf = ti.open(tx, true);
		schema = ti.schema();
		this.tablename = ti.tableName();
	}
	
	public TableScan(TableInfo ti, Transaction tx, long blockAccess, long recordsOutput) {
		rf = ti.open(tx, true);
		schema = ti.schema();
		this.blockAccess = blockAccess;
		this.recordsOutput = recordsOutput;
		this.tablename = ti.tableName();
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		rf.beforeFirst();
	}

	@Override
	public boolean next() {
		return rf.next();
	}

	@Override
	public void close() {
		rf.close();
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return rf.getVal(fldName);
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

	// UpdateScan methods

	/**
	 * Sets the value of the specified field, as a Constant.
	 * 
	 * @param val
	 *            the constant to be set. Will be casted to the correct type
	 *            specified in the schema of the table.
	 * 
	 * @see UpdateScan#setVal(java.lang.String, Constant)
	 */
	@Override
	public void setVal(String fldName, Constant val) {
		rf.setVal(fldName, val);
	}

	@Override
	public void delete() {
		rf.delete();
	}

	@Override
	public void insert() {
		rf.insert();
	}

	@Override
	public RecordId getRecordId() {
		return rf.currentRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		rf.moveToRecordId(rid);
	}

	@Override
	public String TraverseScanForMeta(int level) {
		String space_str = " ";
		for(int i =0; i < 2*level; i++) {
			space_str = space_str + " ";
		}
		String explain_str = space_str + "->TablePlan: on(" + this.tablename + ") (#blks=" + this.blockAccess + ", #records=" + this.recordsOutput + ")\n" ;
		return explain_str;
	}
}
