//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
