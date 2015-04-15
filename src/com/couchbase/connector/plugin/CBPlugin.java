package com.couchbase.connector.plugin;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.connector.connection.CBConnection;
import com.couchbase.connector.metadata.CBMetadata;
import com.couchbase.connector.metadata.CBRegistrationInfo;
import com.couchbase.connector.read.CBRead;
import com.couchbase.connector.write.CBWrite;
import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.metadata.Capability;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.plugin.IExtWrtPlugin;
import com.informatica.cloud.api.adapter.plugin.IPlugin;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.IWrite;
import com.informatica.cloud.api.adapter.runtime.IWrite2;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;

public class CBPlugin implements IPlugin, IExtWrtPlugin {

	private IRegistrationInfo csvRegInfo;
	private IConnection csvConnection;
	private IMetadata csvMetadata;
	private ILogger logger;
	private OperationContext context;
	private IRead csvReader;
	private IWrite2 csvWriter;
	
	@Override
	public List<Capability> getCapabilities() {
		List<Capability> lstCapability = 	new	ArrayList<Capability>();
		lstCapability.add(Capability.SINGLE_OBJECT_READ);
		lstCapability.add(Capability.SUPPORTS_CREATE_RECORD);
		lstCapability.add(Capability.EXTENDED_WRITE);
		return lstCapability;
	}

	@Override
	public IConnection getConnection() {
		if(csvConnection == null){
			csvConnection = new CBConnection();
		}
		return csvConnection;
	}

	@Override
	public IMetadata getMetadata(IConnection conn)
			throws InitializationException {
		csvMetadata = new CBMetadata(this, (CBConnection)conn);
		return csvMetadata;
	}

	@Override
	public IRead getReader(IConnection conn) throws InitializationException {
		csvReader = new CBRead(this, (CBConnection)conn);
		return csvReader;
	}

	@Override
	public IRegistrationInfo getRegistrationInfo() {
		csvRegInfo = new CBRegistrationInfo();
		return csvRegInfo;
	}

	@Override
	public PluginVersion getVersion() {
		return new PluginVersion(1, 0, 1);
	}

	@Override
	public IWrite getWriter(IConnection conn) throws InitializationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setContext(OperationContext context) {
		this.context = context;
	}

	@Override
	public void setLogger(ILogger logger) {
		this.logger = logger;
	}

	public ILogger getLogger(){
		return this.logger;
	}

	@Override
	public IWrite2 getExtendedWriter(IConnection conn) {
		csvWriter = new CBWrite(this, (CBConnection)conn);
		return csvWriter;
	}
}
