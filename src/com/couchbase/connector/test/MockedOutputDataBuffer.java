package com.couchbase.connector.test;

import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;

public class MockedOutputDataBuffer implements IOutputDataBuffer {

	@Override
	public void setData(Object[] row) throws DataConversionException,
			FatalRuntimeException {
		for (Object field : row) {
			System.out.print(field + "|");
		}
		System.out.println();
	}

}
