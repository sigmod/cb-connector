//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.utils;

public enum AttributeTypeCode {
	STRING("String", 1), INTEGER("Integer", 2), DOUBLE("Double", 3), BOOLEAN(
			"Boolean", 4), DATETIME("DateTime", 5), DATE("Date", 6), DECIMAL(
			"Decimal", 7), BINARY("Binary", 8), TIME("Time", 9), SHORT("Short",
			10), LONG("Long", 11), BIGINT("BigInteger", 12), FLOAT("Float", 13);

	private String dataTypeName;
	private int dataTypeId;

	AttributeTypeCode(String dataTypeName, int dataTypeId) {
		this.dataTypeName = dataTypeName;
		this.dataTypeId = dataTypeId;
	}

	public String getDataTypeName() {
		return this.dataTypeName;
	}

	public int getDataTypeId() {
		return this.dataTypeId;
	}

	public static AttributeTypeCode fromValue(String value) {
		for (AttributeTypeCode c : AttributeTypeCode.values()) {
			if (c.getDataTypeName().equals(value)) {
				return c;
			}
		}
		throw new IllegalArgumentException(value);
	}

}
