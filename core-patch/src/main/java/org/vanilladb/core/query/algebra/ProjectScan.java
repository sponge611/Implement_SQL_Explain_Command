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

import java.util.Collection;

import org.vanilladb.core.sql.Constant;

/**
 * The scan class corresponding to the <em>project</em> relational algebra
 * operator. All methods except hasField delegate their work to the underlying
 * scan.
 */
public class ProjectScan implements Scan {
	private Scan s;
	private Collection<String> fieldList;
	private long blockAccess = 0;
	private long recordsOutput = 0;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 * 
	 * @param s
	 *            the underlying scan
	 * @param fieldList
	 *            the list of field names
	 */
	public ProjectScan(Scan s, Collection<String> fieldList) {
		this.s = s;
		this.fieldList = fieldList;
	}
	
	public ProjectScan(Scan s, Collection<String> fieldList, long blockAccess, long recordsOutput) {
		this.s = s;
		this.fieldList = fieldList;
		this.blockAccess = blockAccess;
		this.recordsOutput = recordsOutput;
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName))
			return s.getVal(fldName);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return fieldList.contains(fldName);
	}

	@Override
	public String TraverseScanForMeta(int level) {
		String space_str = " ";
		for(int i =0; i < 2*level; i++) {
			space_str = space_str + " ";
		}
		String explain_str = space_str + "->ProjectPlan: (#blks=" + this.blockAccess + ", #records=" + this.recordsOutput + ")\n" ;
		explain_str = explain_str + s.TraverseScanForMeta(level+1);
		return explain_str;
	}
}
