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
	ARRAY(1), BIGINT(2), BOOLEAN(3), STRING(4), CHAR(5), DECIMAL(6), DOUBLE(7), INT(
			8), INTEGER(9), OBJECT(10), SMALLINT(11), TINYINT(12), VARCHAR(13), NULL(
			14);

	private int id = 0;

	private AttributeTypeCode(int id) {
		this.id = id;
	}

	public int id() {
		return id;
	}
}
