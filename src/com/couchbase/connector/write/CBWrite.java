//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.write;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.plugin.CBPlugin;
import com.couchbase.connector.utils.CBPopulateSuccessAndErrorBuffer;
import com.couchbase.connector.utils.CBSuccessAndErrorBuffer;
import com.informatica.cloud.api.adapter.common.ELogMsgLevel;
import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IWrite2;
import com.informatica.cloud.api.adapter.runtime.SessionStats;
import com.informatica.cloud.api.adapter.runtime.SessionStats.Operation;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;

public class CBWrite implements IWrite2 {

	private final CBConnection connection;
	private final CBPlugin plugin;
	private final ILogger logger;

	private RecordInfo primaryRecordInfo;
	private Map<String, Map<String, String>> recordAttributes = new HashMap<String, Map<String, String>>();
	private Map<String, String> writeOperationAttributes = new HashMap<String, String>();
	private List<Field> inputFieldList;
	private List<Field> outputFieldList;
	private List<Field> errorOutputFieldList;
	private Operation operation;
	public CBSessionStats sessionStats = new CBSessionStats();
	public CBSuccessAndErrorBuffer successAndErrorBuffer = new CBSuccessAndErrorBuffer();
	private CBPopulateSuccessAndErrorBuffer populateBuffer = new CBPopulateSuccessAndErrorBuffer();

	public CBWrite(CBPlugin csvFilePlugin, CBConnection csvConnection) {
		connection = csvConnection;
		plugin = csvFilePlugin;
		logger = plugin.getLogger();
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
		this.writeOperationAttributes.putAll(runtimeAttribs);
	}

	@Override
	public void setPrimaryRecord(RecordInfo primaryRecordInfo) {
		this.primaryRecordInfo = primaryRecordInfo;
	}

	@Override
	public void setRecordAttributes(RecordInfo recordInfo,
			Map<String, String> tgtDesigntimeAttribs) {
		this.recordAttributes.put(recordInfo.getRecordName(),
				tgtDesigntimeAttribs);
	}

	@Override
	public void delete(IInputDataBuffer iInputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		if (iInputDataBuffer != null) {
			logger.logMessage("CSVFileWrite", "delete ", ELogMsgLevel.ERROR,
					"'Delete' Operation is not supported....!");
			throw new FatalRuntimeException(
					"'Delete' Operation is not supported....!");
		}
	}

	@Override
	public void insert(IInputDataBuffer iInputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		if (iInputDataBuffer != null) {
			setOperation(Operation.INSERT);
			try {

				Map<String, String> mapNextLine = new HashMap<String, String>();

				while (iInputDataBuffer.hasMoreRows()) {
					Object[] rowData = iInputDataBuffer.getData();
					for (int iCount = 0; iCount < inputFieldList.size(); iCount++) {
						String sValue = (rowData[iCount] == null ? null
								: rowData[iCount].toString());
						mapNextLine.put(inputFieldList.get(iCount)
								.getUniqueName(), sValue);
					}
					populateBuffer.populateOutputBuffer(rowData,
							successAndErrorBuffer);
					incrementSuccessRowsForOp(1);
					incrementProcessedRowsForOp(1);
				}
			} catch (IOException e1) {
				e1.printStackTrace();
				logger.logMessage("CSVFileWrite", "insert", ELogMsgLevel.INFO,
						"Error occured while writing data: " + e1.getMessage());
				throw new FatalRuntimeException(
						"Error occured while writing data: " + e1.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				logger.logMessage("CSVFileWrite", "insert", ELogMsgLevel.INFO,
						"Error occured while writing data: " + e.getMessage());
				throw new FatalRuntimeException(
						"Error occured while writing data: " + e.getMessage());
			}
		}

	}

	@Override
	public void update(IInputDataBuffer iInputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		if (iInputDataBuffer != null) {
			logger.logMessage("CSVFileWrite", "update ", ELogMsgLevel.ERROR,
					"'Delete' Operation is not supported....!");
			throw new FatalRuntimeException(
					"'Update' Operation is not supported....!");
		}
	}

	@Override
	public void upsert(IInputDataBuffer iInputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		if (iInputDataBuffer != null) {
			logger.logMessage("CSVFileWrite", "upsert ", ELogMsgLevel.ERROR,
					"'Delete' Operation is not supported....!");
			throw new FatalRuntimeException(
					"'Upsert' Operation is not supported....!");
		}
	}

	@Override
	public List<SessionStats> deinit() throws ConnectionFailedException,
			ReflectiveOperationException, WriteException,
			DataConversionException, FatalRuntimeException {
		return sessionStats.getCumulativeSessionStats();
	}

	@Override
	public List<SessionStats> getCumulativeOperationStats() {
		return sessionStats.getCumulativeSessionStats();
	}

	@Override
	public void setErrorGroupBuffer(IOutputDataBuffer errorGroupBuffer) {
		successAndErrorBuffer.setErrorGroupBuffer(errorGroupBuffer);
	}

	@Override
	public void setErrorOutputFieldList(List<Field> errorOutputFieldList) {
		this.errorOutputFieldList = errorOutputFieldList;
		Object[] errorRowData = new Object[this.errorOutputFieldList.size()];
		successAndErrorBuffer.setErrorRowData(errorRowData);
	}

	@Override
	public void setInputFieldList(List<Field> fieldList) {
		this.inputFieldList = fieldList;
	}

	@Override
	public void setOutputFieldList(List<Field> outputFieldList) {
		this.outputFieldList = outputFieldList;
	}

	@Override
	public void setOutputGroupBuffer(IOutputDataBuffer outputGroupBuffer) {
		successAndErrorBuffer.setOutputGroupBuffer(outputGroupBuffer);
	}

	// Helper methods added for IWrite2::
	// Start::
	protected SessionStats getSessionStatsForOperation() {
		Operation op = getOperation();
		switch (op) {
		case DELETE:
			return sessionStats.getDeleteStats();
		case INSERT:
			return sessionStats.getInsertStats();
		case UPDATE:
			return sessionStats.getUpdateStats();
		case UPSERT:
			return sessionStats.getUpsertStats();
		default:
			throw new UnsupportedOperationException(
					"Cannot update Stats for Operation:" + op);
		}
	}

	protected Operation getOperation() {
		return operation;
	}

	protected void setOperation(Operation operation) {
		this.operation = operation;
	}

	protected void incrementSuccessRowsForOp(int successRows) {
		SessionStats sessionStats = getSessionStatsForOperation();
		sessionStats.incrementSuccessRowsCount(successRows);
	}

	protected void incrementProcessedRowsForOp(int processedRows) {
		SessionStats sessionStats = getSessionStatsForOperation();
		sessionStats.incrementProcessedRowsCount(processedRows);
	}
}
