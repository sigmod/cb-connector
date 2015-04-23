//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.utils;

public class CBUtils {
	public static int getPrecisionForDatatype(final String typeName) {
		int precision = 15;
		switch (AttributeTypeCode.valueOf(typeName)) {
		case ARRAY:
			precision = 255;
			break;
		case TINYINT:
			precision = 1;
			break;
		case BOOLEAN:
			precision = 10;
			break;
		case STRING:
			precision = 255;
			break;
		case CHAR:
			precision = 255;
			break;
		case VARCHAR:
			precision = 255;
			break;
		case DECIMAL:
			precision = 28;
			break;
		case INT:
		case INTEGER:
			precision = 10;
			break;
		case SMALLINT:
			precision = 5;
			break;
		case BIGINT:
			precision = 19;
			break;
		case DOUBLE:
			precision = 15;
			break;
		case OBJECT:
			precision = 255;
			break;
		default:
			throw new IllegalStateException("Unknown type: " + typeName);
		}
		return precision;
	}

	public static int getScaleForDatatype(final String typeName) {
		int iScale = 0;

		AttributeTypeCode type = AttributeTypeCode.valueOf(typeName);
		if (type == AttributeTypeCode.DECIMAL)
			iScale = 10;
		else if (type == AttributeTypeCode.DOUBLE)
			iScale = 5;

		return iScale;
	}
}
