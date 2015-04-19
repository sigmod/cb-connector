//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.utils;

import com.couchbase.connector.constant.CBConstants;

public class CBUtils {
	public static int getPrecisionForDatatype(final String datatype) {
		int precision = 15;
		
	//	Array has value similar to the string precision 
		if (datatype.equalsIgnoreCase(CBConstants.ARRAY))
			precision = 255;
		else if (datatype.equalsIgnoreCase(CBConstants.TINYINT))
	    	precision = 1;
		else if (datatype.equalsIgnoreCase(CBConstants.BOOLEAN))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.STRING))
			precision = 255;
		else if (datatype.equalsIgnoreCase(CBConstants.CHAR))
			precision = 255;
		else if (datatype.equalsIgnoreCase(CBConstants.VARCHAR))
			precision = 255;
		else if (datatype.equalsIgnoreCase(CBConstants.DECIMAL))
			precision = 28;
		else if (datatype.equalsIgnoreCase(CBConstants.INT))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.SMALLINT))
			precision = 5;
		else if (datatype.equalsIgnoreCase(CBConstants.BIGINT))
			precision = 19;
		else if (datatype.equalsIgnoreCase(CBConstants.DOUBLE))
			precision = 15;
		
		return precision;
	}

	public static int getScaleForDatatype(final String datatype) {
		int iScale = 0;

		if (datatype.equalsIgnoreCase("DECIMAL"))
			iScale = 10;
		else if (datatype.equalsIgnoreCase("DOUBLE"))
			iScale = 5;

		return iScale;
	}
}
