package com.couchbase.connector.utils;

import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;

public class CBSuccessAndErrorBuffer {
	private IOutputDataBuffer errorGroupBuffer;
	private IOutputDataBuffer outputGroupBuffer;

	private Object[] errorRowData;

	public IOutputDataBuffer getErrorGroupBuffer() {
		return errorGroupBuffer;
	}

	public void setErrorGroupBuffer(IOutputDataBuffer errorGroupBuffer) {
		this.errorGroupBuffer = errorGroupBuffer;
	}

	public IOutputDataBuffer getOutputGroupBuffer() {
		return outputGroupBuffer;
	}

	public void setOutputGroupBuffer(IOutputDataBuffer outputGroupBuffer) {
		this.outputGroupBuffer = outputGroupBuffer;
	}

	public Object[] getErrorRowData() {
		return errorRowData;
	}

	public void setErrorRowData(Object[] errorRowData) {
		this.errorRowData = errorRowData;
	}
}
