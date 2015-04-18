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
import com.couchbase.connector.typesystem.CBTypeSystem;
import com.couchbase.connector.utils.AttributeTypeCode;
import com.informatica.cloud.api.adapter.common.ELogMsgLevel;
import com.informatica.cloud.api.adapter.common.ILogger;
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
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class CBMetadata implements IMetadata, IDefineMetadata, IExtWrtMetadata {

	private CBPlugin plugin;
	private CBConnection connection;
	private List<RecordInfo> lstRecordInfo = new ArrayList<RecordInfo>();
	private ILogger logger;

	public CBMetadata(CBPlugin cbPlugin, CBConnection cbConnection) {
		this.plugin = cbPlugin;
		this.connection = cbConnection;
		this.logger = cbPlugin.getLogger();
	}

	@Override
	public CreateRecordResult createRecord(RecordInfo recordInfo,
			List<Field> fieldList) throws MetadataCreateException {
		CreateRecordResult createRecordResult = new CreateRecordResult();
		List<Field> targetFields = new ArrayList<Field>();
		recordInfo.setLabel(recordInfo.getRecordName());
		recordInfo.setCatalogName("default");
		try {
			createRecordResult.setFields(fieldList);
			targetFields = getFields(recordInfo, false);
		} catch (Exception e) {
			e.printStackTrace();
			logger.logMessage(
					"CSVFileMetadata",
					"createRecord",
					ELogMsgLevel.INFO,
					"Error occured while creating target file: "
							+ e.getMessage());
			throw new MetadataCreateException(
					"Error occured while creating target file: "
							+ e.getMessage());
		}
		createRecordResult = new CreateRecordResult();
		createRecordResult.setRecordInfo(recordInfo);
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
					tableNameIndex = mdColumnIndex;
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
			}
			return lstRecordInfo;
		} catch (Exception e) {
			throw new MetadataReadException(e);
		}
	}

	private Connection getJDBCConnection() throws SQLException,
			MetadataReadException {
		Connection jdbcConnection = connection.getConnection();
		if (jdbcConnection == null || jdbcConnection.isClosed()) {
			throw new MetadataReadException(
					"The JDBC connection is unavailable.");
		}
		return jdbcConnection;
	}

	@Override
	public String[][] getDataPreview(RecordInfo recordInfo, int arg1,
			List<FieldInfo> lstFieldInfo) throws DataPreviewException {
		String[][] previewRows = new String[20][lstFieldInfo.size()];

		/**
		 * Gets the name of the table to be reviewed.
		 */
		String tableName = recordInfo.getInstanceName();

		/**
		 * If no fields to display, return immediately.
		 */
		if (lstFieldInfo.size() == 0) {
			return previewRows;
		}

		/**
		 * Builds the query string.
		 */
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("select ");
		int fieldNumber = lstFieldInfo.size();
		int fieldIndex = 0;
		for (; fieldIndex < fieldNumber - 1; ++fieldIndex) {
			queryBuilder.append(lstFieldInfo.get(fieldIndex).getDisplayName());
			queryBuilder.append(",");
		}
		queryBuilder.append(lstFieldInfo.get(fieldIndex).getDisplayName());
		queryBuilder.append(" from ");
		queryBuilder.append(tableName);
		queryBuilder.append(" limit 20");

		/**
		 * Runs the preview query and put results into the
		 * sArrDataPreviewRowData array.
		 */
		try {
			Connection jdbcConnection = getJDBCConnection();
			Statement stmt = jdbcConnection.createStatement();
			ResultSet rs = stmt.executeQuery(queryBuilder.toString());
			int rowIndex = 0;
			while (rs.next()) {
				for (int columnIndex = 1; columnIndex <= fieldNumber; columnIndex++) {
					previewRows[rowIndex][columnIndex - 1] = rs
							.getString(columnIndex);
				}
				++rowIndex;
			}
		} catch (Exception e) {
			throw new DataPreviewException(e);
		}
		return previewRows;
	}

	@Override
	public List<Field> getFields(RecordInfo recordInfo, boolean refreshFields)
			throws MetadataReadException {
		CBTypeSystem typeSystem = (CBTypeSystem) plugin.getRegistrationInfo()
				.getTypeSystem();

		// TODO(yingyi): we currently do not support metadata cache, therefore,
		// we ignore the refreshFields parameter.
		List<Field> lstFields = new ArrayList<Field>();
		try {
			String nameSpaceName = recordInfo.getCatalogName();
			String tableName = recordInfo.getInstanceName();
			Connection jdbcConnection = getJDBCConnection();
			DatabaseMetaData metadata = jdbcConnection.getMetaData();
			ResultSet schema = metadata.getColumns(nameSpaceName, null,
					tableName, null);
			ResultSetMetaData schemaMetadata = schema.getMetaData();
			int schemaColumnNumber = schemaMetadata.getColumnCount();
			while (schema.next()) {
				for (int schemaFieldIndex = 1; schemaFieldIndex <= schemaColumnNumber; ++schemaFieldIndex) {
					String columnName = schema.getString(4);
					String typeName = schema.getString(6);
					Field field = new Field();
					field.setDisplayName(columnName);
					field.setLabel(columnName);
					field.setFilterable(false);
					List<JavaDataType> candidateJavaTypes = typeSystem
							.getJavaDataTypesFor(typeName);
					field.setJavaDatatype(candidateJavaTypes.get(0));
				}
			}
		} catch (Exception e) {
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
		f.setDatatype(new DataType(AttributeTypeCode.STRING.getDataTypeName(),
				AttributeTypeCode.STRING.getDataTypeId()));
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

}
