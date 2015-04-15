package com.couchbase.connector.utils;

import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;

public class CBPopulateSuccessAndErrorBuffer {

	public void populateOutputBuffer(Object[] data,
			CBSuccessAndErrorBuffer successAndErrorBuffer)
			throws DataConversionException, FatalRuntimeException {
		successAndErrorBuffer.getOutputGroupBuffer().setData(data);
	}

	public void populateErrorOutputBuffer(Object[] data,
			String exceptionMessage,
			CBSuccessAndErrorBuffer successAndErrorBuffer)
			throws DataConversionException, FatalRuntimeException {
		Object[] errorRowData = successAndErrorBuffer.getErrorRowData();
		errorRowData[0] = exceptionMessage;
		int iIndex = 1;
		for (Object value : data) {
			errorRowData[iIndex] = value;
			iIndex++;
		}
		populateErrorOutputBuffer(errorRowData, successAndErrorBuffer);
	}

	public void populateErrorOutputBuffer(Object[] errorRowData,
			CBSuccessAndErrorBuffer successAndErrorBuffer)
			throws DataConversionException, FatalRuntimeException {

		successAndErrorBuffer.getErrorGroupBuffer().setData(errorRowData);
	}

}
