//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.read;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.plugin.CBPlugin;
import com.informatica.cloud.api.adapter.common.ELogMsgLevel;
import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class CBRead implements IRead {
	private CBConnection connection;
	private ILogger logger;

	private RecordInfo primaryRecordInfo;
	private List<FilterInfo> filterInfoList = new ArrayList<FilterInfo>();
	private List<Field> fieldList = new ArrayList<Field>();
	private List<JavaDataType> fieldTypes = new ArrayList<JavaDataType>();

	public CBRead(CBPlugin cbPlugin, CBConnection cbConnection) {
		this.connection = cbConnection;
		this.logger = cbPlugin.getLogger();
	}

	@Override
	public void initializeAndValidate() throws InitializationException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setChildRecords(List<RecordInfo> arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setMetadataVersion(PluginVersion arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOperationAttributes(Map<String, String> runtimeAttribs) {
		// TODO Auto-generated method stub
	}

	@Override
	public void setPrimaryRecord(RecordInfo primaryRecordInfo) {
		this.primaryRecordInfo = primaryRecordInfo;
	}

	@Override
	public void setRecordAttributes(RecordInfo recordInfo,
			Map<String, String> tgtDesigntimeAttribs) {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean read(IOutputDataBuffer outputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			ReadException, DataConversionException, FatalRuntimeException {
		boolean bStatus = false;
		if (fieldList.size() == 0) {
			return bStatus;
		}
		if (outputDataBuffer != null) {
			/**
			 * Gets the name of the table to be reviewed.
			 */
			String tableName = primaryRecordInfo.getInstanceName();

			/**
			 * Builds the query string.
			 */
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append("select * ");

			// TODO(yingyi): Simba JDBC driver has sporadic failures for
			// projection
			// queries, change back when Simba fixes this.
			// for (Field field : fieldList) {
			// queryBuilder.append("`" + field.getDisplayName() + "`, ");
			// }
			// queryBuilder.delete(queryBuilder.length() - 2,
			// queryBuilder.length());
			queryBuilder.append(" from `");
			queryBuilder.append(tableName);
			queryBuilder.append("`");

			System.out.println(queryBuilder.toString());
			/**
			 * Runs the select * query and put results into the
			 * sArrDataPreviewRowData array.
			 */
			try {
				Connection jdbcConnection = getJDBCConnection();
				Statement stmt = jdbcConnection.createStatement();
				ResultSet rs = stmt.executeQuery(queryBuilder.toString());
				while (rs.next()) {
					int fieldNumber = rs.getMetaData().getColumnCount();
					Object[] row = new Object[fieldNumber];
					Object fieldValue = null;
					for (int columnIndex = 1; columnIndex <= fieldNumber; columnIndex++) {
						try {
							JavaDataType fieldType = JavaDataType.JAVA_STRING;
							if (columnIndex < fieldTypes.size()) {
								fieldType = fieldTypes.get(columnIndex);
							}
							if (fieldType == null) {
								fieldType = JavaDataType.JAVA_STRING;
							}
							switch (fieldType) {
							case JAVA_DOUBLE:
							case JAVA_FLOAT:
								fieldValue = rs.getDouble(columnIndex);
								break;
							case JAVA_BOOLEAN:
								fieldValue = rs.getBoolean(columnIndex);
								break;
							case JAVA_INTEGER:
							case JAVA_SHORT:
								fieldValue = rs.getInt(columnIndex);
								break;
							case JAVA_BIGINTEGER:
							case JAVA_LONG:
								fieldValue = rs.getLong(columnIndex);
								break;
							default:
								fieldValue = rs.getString(columnIndex);
							}
						} catch (Exception aoe) {
							fieldValue = null;
						}
						row[columnIndex - 1] = fieldValue;
					}
					outputDataBuffer.setData(row);
				}
				bStatus = true;
			} catch (Exception e) {
				e.printStackTrace();
				logger.logMessage("JDBC connection error", "",
						ELogMsgLevel.ERROR, e.getLocalizedMessage());
				throw new FatalRuntimeException(e);
			}
		}
		return bStatus;
	}

	@Override
	public void setAdvancedFilters(AdvancedFilterInfo arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setFieldList(List<Field> fieldList) {
		this.fieldList.clear();
		this.fieldList.addAll(fieldList);
		for (Field field : fieldList) {
			fieldTypes.add(field.getJavaDatatype());
		}
	}

	@Override
	public void setFilters(List<FilterInfo> filterInfoList) {
		this.filterInfoList.clear();
		if (filterInfoList != null && filterInfoList.size() > 0) {
			this.filterInfoList.addAll(filterInfoList);
		}
	}

	@Override
	public void setRelatedRecords(List<RecordInfo> arg0) {
		// TODO Auto-generated method stub

	}

	/**
	 * Gets the JDBC connection from the IConnection instance.
	 * 
	 * @return the SQL JDBC connection.
	 * @throws SQLException
	 * @throws MetadataReadException
	 */
	private Connection getJDBCConnection() throws SQLException,
			MetadataReadException {
		Connection jdbcConnection = connection.getConnection();
		if (jdbcConnection == null || jdbcConnection.isClosed()) {
			throw new MetadataReadException(
					"The JDBC connection is unavailable.");
		}
		return jdbcConnection;
	}

}
