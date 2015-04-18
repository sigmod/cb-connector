package com.couchbase.connector.constant;

import java.util.HashMap;

import com.couchbase.connector.utils.AttributeTypeCode;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class CBUtils {

	private static HashMap<AttributeTypeCode, JavaDataType> mapToolkitToJavaDataTypeConversion = new HashMap<AttributeTypeCode, JavaDataType>();
	private static HashMap<JavaDataType, AttributeTypeCode> mapJavaToToolkitDataTypeConversion = new HashMap<JavaDataType, AttributeTypeCode>();

	public static int getPrecisionForDatatype(final String datatype) {
		int precision = 15;

		if (datatype.equalsIgnoreCase(CBConstants.BOOLEAN))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.STRING))
			precision = 255;
		else if (datatype.equalsIgnoreCase(CBConstants.DECIMAL))
			precision = 28;
		else if (datatype.equalsIgnoreCase(CBConstants.INTEGER))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.DATETIME))
			precision = 26;
		else if (datatype.equalsIgnoreCase(CBConstants.DATE))
			precision = 19;
		else if (datatype.equalsIgnoreCase(CBConstants.BINARY))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.LONG))
			precision = 19;
		else if (datatype.equalsIgnoreCase(CBConstants.SHORT))
			precision = 10;
		else if (datatype.equalsIgnoreCase(CBConstants.BIGINT))
			precision = 19;
		else if (datatype.equalsIgnoreCase(CBConstants.DOUBLE))
			precision = 15;
		else if (datatype.equalsIgnoreCase(CBConstants.FLOAT))
			precision = 10;

		return precision;
	}

	public static int getScaleForDatatype(final String datatype) {
		int iScale = 0;

		if (datatype.equalsIgnoreCase("DECIMAL"))
			iScale = 10;
		else if (datatype.equalsIgnoreCase("DOUBLE"))
			iScale = 5;
		else if (datatype.equalsIgnoreCase("FLOAT"))
			iScale = 5;

		return iScale;
	}
}