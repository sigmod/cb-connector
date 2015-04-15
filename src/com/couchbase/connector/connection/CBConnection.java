package com.couchbase.connector.connection;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.connector.constant.CBConstants;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.plugin.InvalidArgumentException;

public class CBConnection implements IConnection {

	Map<String, String> connAttribs = new HashMap<String, String>();
	private boolean bConnectStatus = false;
	public String sDirectory = null;
	public String sDelimeter = null;

	@Override
	public boolean connect() throws InsufficientConnectInfoException,
			ConnectionFailedException {
		if(connAttribs != null && !connAttribs.isEmpty() && connAttribs.size() > 0){
			sDirectory = connAttribs.get(CBConstants.CONN_ATTRIB_DIRECTORY);
			sDelimeter = connAttribs.get(CBConstants.CONN_ATTRIB_DELIMITER);
			
			if (sDelimeter == null) {
				sDelimeter = CBConstants.DEFAULT_DELIMITER;
			}
			if(sDirectory != null){
				File file = new File(sDirectory);
				if (file != null && file.exists() && file.canRead()) {
					if (file.isDirectory()) {
						bConnectStatus = true;
					} else {
						throw new ConnectionFailedException(
								"Provided path is not a valid directory path! "
										+ sDirectory);
					}
				} else {
					throw new ConnectionFailedException(
							"The path provided does not exists OR cannot be read!");
				}
			}else{
				bConnectStatus = false;
			}
		}else{
			bConnectStatus = false;
		}
		return bConnectStatus;
	}

	@Override
	public boolean disconnect() {
		return bConnectStatus;
	}

	@Override
	public void setConnectionAttributes(Map<String, String> connParams) {
		this.connAttribs.clear();
		this.connAttribs.putAll(connParams);
	}

	@Override
	public boolean validate() throws InvalidArgumentException {
		return false;
	}

}
