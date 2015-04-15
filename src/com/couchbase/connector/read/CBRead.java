package com.couchbase.connector.read;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.supercsv.io.CsvMapReader;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.constant.CBConstants;
import com.couchbase.connector.plugin.CBPlugin;
import com.informatica.cloud.api.adapter.common.ELogMsgLevel;
import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;

public class CBRead implements IRead {

	private CBPlugin plugin;
	private CBConnection connection;
	private ILogger logger;
	
	private RecordInfo primaryRecordInfo;
	private List<FilterInfo> filterInfoList = new ArrayList<FilterInfo>();
	private List<Field> fieldList = new ArrayList<Field>();
	
	public CBRead(CBPlugin csvFilePlugin, CBConnection csvConnection) {
		this.plugin = csvFilePlugin;
		this.connection = csvConnection;
		this.logger = csvFilePlugin.getLogger();
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
	public void setRecordAttributes(RecordInfo recordInfo, Map<String, String> tgtDesigntimeAttribs) {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean read(IOutputDataBuffer outputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			ReadException, DataConversionException, FatalRuntimeException {
		boolean bStatus = false;
		if(outputDataBuffer != null){
			try {
				FileInputStream fileInputStream = new FileInputStream(connection.sDirectory + File.separator + primaryRecordInfo.getRecordName() + ".csv");// Reading file..
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream, CBConstants.CHARSET_UTF_8));
				CsvPreference csvPreference = new CsvPreference.Builder('"',connection.sDelimeter.charAt(0), "\r\n").build();
				Tokenizer tokenizer = new Tokenizer(bufferedReader, csvPreference);
				CsvMapReader csvMapReader = new CsvMapReader(tokenizer, csvPreference);
				
				String[] headerLine = csvMapReader.getHeader(true);
				Map<String,String> mapNextLine = new HashMap<String,String>();
				Object[] rowData = new String[fieldList.size()];
				while((mapNextLine = csvMapReader.read(headerLine)) != null){
					for(int iCount=0; iCount<fieldList.size(); iCount++){
						rowData[iCount] = mapNextLine.get(fieldList.get(iCount).getUniqueName());
					}
					outputDataBuffer.setData(rowData);
				}
				csvMapReader.close();
				tokenizer.close();
				bufferedReader.close();
				fileInputStream.close();
				bStatus = true;
			} catch (IOException e) {
				bStatus = false;
				e.printStackTrace();
				logger.logMessage("CSVFileMetadata", "getDataPreview", ELogMsgLevel.INFO, "Error occured while reading data: "+e.getMessage());
				throw new FatalRuntimeException("Error occured while reading data: "+e.getMessage());
			}
		}else{
			throw new FatalRuntimeException("No data available! OutputDataBuffer is null!!");
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
		//this.fieldList.addAll(fieldList);
		this.fieldList = fieldList;
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

}
