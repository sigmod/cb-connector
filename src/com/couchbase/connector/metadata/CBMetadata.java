package com.couchbase.connector.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.supercsv.io.CsvMapReader;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.constant.CBConstants;
import com.couchbase.connector.plugin.CBPlugin;
import com.couchbase.connector.utils.AttributeTypeCode;
import com.couchbase.connector.utils.CBUtils;
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

public class CBMetadata implements IMetadata, IDefineMetadata,
		IExtWrtMetadata {

	private CBPlugin plugin;
	private CBConnection connection;
	private List<RecordInfo> lstRecordInfo = new ArrayList<RecordInfo>();
	private ILogger logger;

	public CBMetadata(CBPlugin csvFilePlugin,
			CBConnection csvConnection) {
		this.plugin = csvFilePlugin;
		this.connection = csvConnection;
		this.logger = csvFilePlugin.getLogger();
	}

	@Override
	public CreateRecordResult createRecord(RecordInfo recordInfo,
			List<Field> fieldList) throws MetadataCreateException {
		CreateRecordResult createRecordResult = new CreateRecordResult();
		List<Field> targetFields = new ArrayList<Field>();
		recordInfo.setLabel(recordInfo.getRecordName());
		recordInfo.setCatalogName("default");
		try {
			FileWriter targetFileWriter = new FileWriter(connection.sDirectory
					+ File.separator + recordInfo.getRecordName() + ".csv");
			String sNewLineChar = System.getProperty("line.separator");
			String header = "";
			for (int iFieldCount = 0; iFieldCount < fieldList.size(); iFieldCount++) {
				header = header + fieldList.get(iFieldCount).getDisplayName()
						+ connection.sDelimeter;
			}
			if (header.endsWith(",")) {
				header = header.substring(0, header.length() - 1);
			}
			targetFileWriter.write(header + sNewLineChar);
			targetFileWriter.close();
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

		File fileDirectory = new File(connection.sDirectory);
		if (fileDirectory.exists() && fileDirectory.canRead()) {
			if (fileDirectory.isDirectory()) {
				File[] filesInDirectory = fileDirectory.listFiles();
				for (File file : filesInDirectory) {
					String sFileName = file.getName();
					if (sFileName.contains(".")) {
						sFileName = sFileName.substring(0,
								sFileName.indexOf('.'));
					}
					RecordInfo recordInfo = new RecordInfo();
					recordInfo.setRecordName(sFileName);
					recordInfo.setLabel(file.getName());
					recordInfo.setCatalogName("Standard Records");
					lstRecordInfo.add(recordInfo);
				}
			} else {
				throw new MetadataReadException(
						"The path provided is not a valid directory path! "
								+ connection.sDirectory);
			}
		} else {
			throw new MetadataReadException(
					"The path provided does not exist OR cannot be read!");
		}

		return lstRecordInfo;
	}

	@Override
	public String[][] getDataPreview(RecordInfo recordInfo, int arg1,
			List<FieldInfo> lstFieldInfo) throws DataPreviewException {
		String[][] sArrDataPreviewRowData = new String[10][lstFieldInfo.size()];
		FieldInfo fieldInfo = null;
		List<Field> lstDataPreviewFields = new ArrayList<Field>();
		List<Field> lstFields;
		try {
			lstFields = getFields(recordInfo, false);
			for (Field field : lstFields) {
				fieldInfo = new FieldInfo();
				fieldInfo.setDisplayName(field.getDisplayName());
				fieldInfo.setUniqueName(field.getUniqueName());
				lstFieldInfo.add(fieldInfo);
				lstDataPreviewFields.add(field);
			}
			FileInputStream fileInputStream = new FileInputStream(
					connection.sDirectory + File.separator
							+ recordInfo.getRecordName() + ".csv");
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(fileInputStream,
							CBConstants.CHARSET_UTF_8));
			CsvPreference csvPreference = new CsvPreference.Builder('"',
					connection.sDelimeter.charAt(0), "\r\n").build();
			Tokenizer tokenizer = new Tokenizer(bufferedReader, csvPreference);
			CsvMapReader csvMapReader = new CsvMapReader(tokenizer,
					csvPreference);

			String[] headerLine = csvMapReader.getHeader(true);
			Map<String, String> mapNextLine = new HashMap<String, String>();
			String[] sArrRow = new String[lstFields.size()];
			int iRowCount = 0;
			while ((mapNextLine = csvMapReader.read(headerLine)) != null) {
				if (iRowCount == 10) {
					break;
				}
				for (int iCount = 0; iCount < lstFields.size(); iCount++) {
					sArrRow[iCount] = mapNextLine.get(lstFields.get(iCount)
							.getUniqueName());
				}
				sArrDataPreviewRowData[iRowCount++] = sArrRow.clone();
			}
			csvMapReader.close();
			tokenizer.close();
			bufferedReader.close();
			fileInputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.logMessage(
					"CSVFileMetadata",
					"getDataPreview",
					ELogMsgLevel.INFO,
					"Error occured during Data Preview: "
							+ e.getMessage());
			throw new DataPreviewException(
					"Error occured during Data Preview: "
							+ e.getMessage());
		}
		return sArrDataPreviewRowData;
	}

	@Override
	public List<Field> getFields(RecordInfo recordInfo, boolean arg1)
			throws MetadataReadException {
		List<Field> lstFields = new ArrayList<Field>();
		try {
			FileInputStream fileInputStream = new FileInputStream(
					connection.sDirectory + File.separator
							+ recordInfo.getRecordName() + ".csv");
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(fileInputStream,
							CBConstants.CHARSET_UTF_8));
			CsvPreference csvPreference = new CsvPreference.Builder('"',
					connection.sDelimeter.charAt(0), "\r\n").build();
			Tokenizer tokenizer = new Tokenizer(bufferedReader, csvPreference);
			CsvMapReader csvMapReader = new CsvMapReader(tokenizer,
					csvPreference);

			String[] headerLine = csvMapReader.getHeader(true);
			for (String sHeader : headerLine) {
				Field field = new Field();
				field.setContainingRecord(recordInfo);
				field.setUniqueName(sHeader);
				field.setDisplayName(sHeader);
				field.setDescription(sHeader);
				field.setLabel(sHeader);
				field.setFilterable(false);

				field.setJavaDatatype(JavaDataType.JAVA_STRING);

				AttributeTypeCode attributeTypeCode = AttributeTypeCode.STRING;

				DataType dt = new DataType(attributeTypeCode.getDataTypeName(),
						attributeTypeCode.getDataTypeId());
				dt.setDefaultPrecision(CBUtils
						.getPrecisionForDatatype(attributeTypeCode
								.getDataTypeName()));
				dt.setDefaultScale(CBUtils
						.getScaleForDatatype(attributeTypeCode
								.getDataTypeName()));

				field.setPrecision(CBUtils
						.getPrecisionForDatatype(attributeTypeCode
								.getDataTypeName()));
				field.setScale(CBUtils
						.getScaleForDatatype(attributeTypeCode
								.getDataTypeName()));

				field.setDatatype(dt);
				lstFields.add(field);
				csvMapReader.close();
			}
			csvMapReader.close();
			tokenizer.close();
			bufferedReader.close();
			fileInputStream.close();

		} catch (IOException e) {
			e.printStackTrace();
			logger.logMessage("CSVFileMetadata", "getFields",
					ELogMsgLevel.INFO, "Error occured while reading Metadata: "
							+ e.getMessage());
			throw new MetadataReadException(
					"Error occured while reading Metadata: " + e.getMessage());
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
