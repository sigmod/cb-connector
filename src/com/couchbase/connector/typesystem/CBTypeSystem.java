//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.typesystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.couchbase.connector.utils.AttributeTypeCode;
import com.couchbase.connector.utils.CBUtils;
import com.informatica.cloud.api.adapter.typesystem.DataType;
import com.informatica.cloud.api.adapter.typesystem.ITypeSystem;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class CBTypeSystem implements ITypeSystem {

	HashMap<DataType, List<JavaDataType>> dataTypeMap;
	List<DataType> nativeDataTypes;

	@Override
	public Map<DataType, List<JavaDataType>> getDatatypeMapping() {
		/**
		 * @returns a mapping of endpoint datatypes to Java datatypes
		 */
		if (dataTypeMap == null) {
			dataTypeMap = new HashMap<DataType, List<JavaDataType>>();
			for (DataType dt : getNativeDataTypes()) {
				try {
					dataTypeMap.put(dt, getJavaDataTypesFor(dt.getName()));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return dataTypeMap;
	}

	@Override
	public List<DataType> getNativeDataTypes() {
		/**
		 * @returns the datatypes used in the SIMBA JDBC driver
		 */
		if (nativeDataTypes == null) {
			nativeDataTypes = new ArrayList<DataType>();
			for (AttributeTypeCode type : AttributeTypeCode.values()) {
				int id = type.getDataTypeId();
				String name = type.getDataTypeName();
				DataType dt = new DataType(name, id);
				dt.setDefaultPrecision(CBUtils.getPrecisionForDatatype(name));
				nativeDataTypes.add(dt);
			}
		}
		return nativeDataTypes;
	}

	public List<JavaDataType> getJavaDataTypesFor(String dataTypeName)
			throws Exception {
		ArrayList<JavaDataType> listOfJavaDataTypes = new ArrayList<JavaDataType>();
		
		switch(AttributeTypeCode.fromValue(dataTypeName)){
		case ARRAY:
			listOfJavaDataTypes.add(JavaDataType.JAVA_STRING);
			break;
			
		case INT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_INTEGER);
			listOfJavaDataTypes.add(JavaDataType.JAVA_BIGINTEGER);
			break;
			
		case BIGINT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BIGINTEGER);
			break;
			
		case SMALLINT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_INTEGER);
			break;
			
		case TINYINT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_INTEGER);
			break;

		case BOOLEAN:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BOOLEAN);
			break;

		case DECIMAL:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BIGDECIMAL);
			listOfJavaDataTypes.add(JavaDataType.JAVA_DOUBLE);
			listOfJavaDataTypes.add(JavaDataType.JAVA_FLOAT);
			break;

		case STRING:
			listOfJavaDataTypes.add(JavaDataType.JAVA_STRING);
			break;
			
		case CHAR:
			listOfJavaDataTypes.add(JavaDataType.JAVA_STRING);
			break;
			
		case VARCHAR:
			listOfJavaDataTypes.add(JavaDataType.JAVA_STRING);
			break;
			
		case DOUBLE:
			listOfJavaDataTypes.add(JavaDataType.JAVA_DOUBLE);
			break;
				
		default:
			throw new Exception();

		}
		return listOfJavaDataTypes;
	}

}
