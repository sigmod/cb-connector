//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.plugin.CBPlugin;
import com.couchbase.connector.utils.AttributeTypeCode;
import com.couchbase.connector.utils.CBUtils;
import com.informatica.cloud.api.adapter.metadata.CreateRecordResult;
import com.informatica.cloud.api.adapter.metadata.DataPreviewException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldInfo;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.FilterSerializationException;
import com.informatica.cloud.api.adapter.metadata.IDefineMetadata;
import com.informatica.cloud.api.adapter.metadata.IExtWrtMetadata;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.MetadataCreateException;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.metadata.Relationship;
import com.informatica.cloud.api.adapter.typesystem.DataType;
import com.informatica.cloud.api.adapter.typesystem.ITypeSystem;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class CBMetadata implements IMetadata, IDefineMetadata, IExtWrtMetadata {

	private CBPlugin plugin;
	private CBConnection connection;
	private List<RecordInfo> lstRecordInfo = new ArrayList<RecordInfo>();

	public CBMetadata(CBPlugin cbPlugin, CBConnection cbConnection) {
		this.plugin = cbPlugin;
		this.connection = cbConnection;
	}

	@Override
	public CreateRecordResult createRecord(RecordInfo recordInfo,
			List<Field> fieldList) throws MetadataCreateException {
		CreateRecordResult createRecordResult = new CreateRecordResult();
		List<Field> targetFields = new ArrayList<Field>();
		RecordInfo targetRecordInfo = new RecordInfo();
		targetRecordInfo.setRecordName(recordInfo.getRecordName());
		targetRecordInfo.setInstanceName(recordInfo.getRecordName());
		targetRecordInfo.setCatalogName(recordInfo.getCatalogName());
		targetFields.addAll(fieldList);
		createRecordResult = new CreateRecordResult();
		createRecordResult.setRecordInfo(targetRecordInfo);
		createRecordResult.setFields(targetFields);
		return createRecordResult;
	}

	@Override
	public List<RecordInfo> getAllRecords() throws MetadataReadException {
		try {
			Connection jdbcConnection = getJDBCConnection();
			DatabaseMetaData metadata = jdbcConnection.getMetaData();
			ResultSet rs = metadata.getTables(null, null, "%", null);
			ResultSetMetaData rsmd = rs.getMetaData();
			int metadataColumnNumber = rsmd.getColumnCount();
			int tableNameIndex = -1;
			int nameSpaceIndex = -1;

			// Gets the column index for "TABLE_NAME" in a metadata row.
			for (int mdColumnIndex = 1; mdColumnIndex <= metadataColumnNumber; ++mdColumnIndex) {
				if (rsmd.getColumnLabel(mdColumnIndex).equals("TABLE_NAME")) {
					tableNameIndex = mdColumnIndex;
				}
				if (rsmd.getColumnLabel(mdColumnIndex).equals("TABLE_SCHEM")) {
					nameSpaceIndex = mdColumnIndex;
				}
			}

			// If we cannot find the "TABLE_NAME" column, throws an
			// exception.
			if (tableNameIndex < 0) {
				throw new MetadataReadException(
						"Cannot find the table name field in a metadata row.");
			}

			// Iterates over all metadata rows and put the name space name
			// and table name for each metadata row.
			while (rs.next()) {
				String nameSpaceName = rs.getString(nameSpaceIndex);
				String tableName = rs.getString(tableNameIndex);

				// TODO (yingyi): Due to the current limitation of the Simba
				// JDBC driver, we can not fetch rows for the table "default",
				// nor for tables outside the "default" name space.
				if (tableName.equals("default")
						|| !nameSpaceName.equals("default")) {
					continue;
				}

				// Constructs the record info object.
				RecordInfo recordInfo = new RecordInfo();
				recordInfo.setCatalogName(nameSpaceName);
				recordInfo.setInstanceName(tableName);
				recordInfo.setRecordName(tableName);
				lstRecordInfo.add(recordInfo);
			}
			return lstRecordInfo;
		} catch (Exception e) {
			throw new MetadataReadException(e);
		}
	}

	@Override
	public String[][] getDataPreview(RecordInfo recordInfo, int arg1,
			List<FieldInfo> lstFieldInfo) throws DataPreviewException {
		List<String[]> previewRows = new ArrayList<String[]>();

		/**
		 * Gets the name of the table to be reviewed.
		 */
		String tableName = recordInfo.getRecordName();

		if (tableName == null) {
			throw new DataPreviewException("table name could not be null!");
		}

		try {
			Connection jdbcConnection = getJDBCConnection();
			if (!verifyRecordInfoExistence(recordInfo, jdbcConnection)) {
				return new String[0][0];
			}

			/**
			 * Builds the query string.
			 */
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append("select top 20 * ");

			// TODO(yingyi): Simba JDBC driver has sporadic failures for
			// projection
			// queries, change back when Simba fixes this.
			// for (FieldInfo field : lstFieldInfo) {
			// queryBuilder.append("`" + field.getDisplayName() + "`, ");
			// }
			// queryBuilder.delete(queryBuilder.length() - 2,
			// queryBuilder.length());
			queryBuilder.append(" from `");
			queryBuilder.append(tableName);
			queryBuilder.append("`");

			/**
			 * Runs the preview query and put results into the
			 * sArrDataPreviewRowData array.
			 */
			Statement stmt = jdbcConnection.createStatement();
			ResultSet rs = stmt.executeQuery(queryBuilder.toString());
			while (rs.next()) {
				int fieldNumber = rs.getMetaData().getColumnCount();
				String[] previewRow = new String[fieldNumber];
				String fieldValue = "null";
				for (int columnIndex = 1; columnIndex <= fieldNumber; columnIndex++) {
					try {
						fieldValue = rs.getString(columnIndex);
					} catch (Exception aoe) {
						fieldValue = "null";
					}
					previewRow[columnIndex - 1] = fieldValue;
				}
				previewRows.add(previewRow);
			}
		} catch (Exception e) {
			throw new DataPreviewException(e);
		}
		return previewRows.toArray(new String[0][]);
	}

	@Override
	public List<Field> getFields(RecordInfo recordInfo, boolean refreshFields)
			throws MetadataReadException {
		ITypeSystem typeSystem = plugin.getRegistrationInfo().getTypeSystem();
		// TODO(yingyi): we currently do not support metadata cache, therefore,
		// we ignore the refreshFields parameter.
		List<Field> lstFields = new ArrayList<Field>();
		try {
			Connection jdbcConnection = getJDBCConnection();
			if (!verifyRecordInfoExistence(recordInfo, jdbcConnection)) {
				throw new MetadataReadException("Table not found "
						+ recordInfo.getRecordName());
			}

			String nameSpaceName = recordInfo.getCatalogName();
			String tableName = recordInfo.getInstanceName();
			DatabaseMetaData metadata = jdbcConnection.getMetaData();
			ResultSet schema = metadata.getColumns(null, nameSpaceName,
					tableName, null);
			while (schema.next()) {
				String columnName = schema.getString(4);
				String typeName = schema.getString(6);
				Field field = new Field();
				field.setDisplayName(columnName);
				field.setLabel(columnName);
				field.setUniqueName(columnName);
				field.setDescription(columnName);
				field.setContainingRecord(recordInfo);
				field.setFilterable(false);
				field.setKey(false);
				field.setReadOnly(true);
				AttributeTypeCode typeCode = AttributeTypeCode
						.valueOf(typeName);
				DataType nativeType = new DataType(typeCode.name(),
						typeCode.id());
				field.setDatatype(nativeType);
				List<JavaDataType> candidateJavaTypes = typeSystem
						.getDatatypeMapping().get(nativeType);
				field.setJavaDatatype(candidateJavaTypes.get(0));
				field.setPrecision(CBUtils.getPrecisionForDatatype(typeName));
				field.setScale(CBUtils.getPrecisionForDatatype(typeName));
				lstFields.add(field);
			}
			if (lstFields.isEmpty()) {
				throw new IllegalStateException(
						"list of fields is empty for table "
								+ recordInfo.getRecordName());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new MetadataReadException(e);
		}
		return lstFields;
	}

	@Override
	public List<RecordInfo> filterRecords(Pattern arg0)
			throws MetadataReadException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String[] getReadOpDesigntimeAttribValues(String[] names,
			RecordInfo recordInfo) throws MetadataReadException {
		String[] sReadOpDesAttr = new String[names.length];
		return sReadOpDesAttr;
	}

	@Override
	public String[] getRecordAttributeValue(String[] names,
			RecordInfo recordInfo) throws MetadataReadException {
		String[] attrValues = new String[names.length];
		return attrValues;
	}

	@Override
	public String[] getWriteOpDesigntimeAttribValues(String[] names,
			RecordInfo recordInfo) throws MetadataReadException {
		String[] sArray = new String[0];
		return sArray;
	}

	@Override
	public String serializeFilterCriteria(List<FilterInfo> names,
			RecordInfo recordInfo) throws FilterSerializationException {
		String string = new String();
		return string;
	}

	@Override
	public List<Field> getErrorOutputFields(RecordInfo arg0,
			List<Relationship> arg1) throws MetadataReadException {
		/**
		 * Defines the error fields in the target that can return error
		 * responses
		 */
		List<Field> field = new ArrayList<Field>();
		Field f = new Field();
		f.setUniqueName("ErrorMessage");
		f.setDisplayName("ErrorMessage");
		f.setLabel("ErrorMessage");
		f.setDatatype(new DataType(AttributeTypeCode.STRING.name(),
				AttributeTypeCode.STRING.id()));
		f.setJavaDatatype(JavaDataType.JAVA_STRING);
		f.setPrecision(200);
		field.add(f);
		return field;
	}

	@Override
	public List<Field> getOutputFields(RecordInfo arg0, List<Relationship> arg1)
			throws MetadataReadException {
		return new ArrayList<Field>();
	}

	/**
	 * Verify if the table represented by the record info exists in the
	 * metadata.
	 * 
	 * @param recordInfo
	 *            the table representation in the informatica framework.
	 * @param jdbcConnection
	 *            the JDBC connection.
	 * @return true the table exits; otherwise, false.
	 * @throws SQLException
	 */
	private boolean verifyRecordInfoExistence(RecordInfo recordInfo,
			Connection jdbcConnection) throws SQLException {
		DatabaseMetaData metadata = jdbcConnection.getMetaData();
		ResultSet tableRs = metadata.getTables(null,
				recordInfo.getCatalogName(), recordInfo.getRecordName(),
				new String[] { "TABLE" });
		if (!tableRs.next()) {
			return false;
		}
		return true;
	}

	private Connection getJDBCConnection() {
		return connection.getConnection();
	}

}
