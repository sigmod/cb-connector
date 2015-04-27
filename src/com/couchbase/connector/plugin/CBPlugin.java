//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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

@SuppressWarnings("deprecation")
public class CBPlugin implements IPlugin, IExtWrtPlugin {

	private ILogger logger;
	private CBConnection connection;

	@Override
	public List<Capability> getCapabilities() {
		List<Capability> lstCapability = new ArrayList<Capability>();
		lstCapability.add(Capability.SINGLE_OBJECT_READ);
		lstCapability.add(Capability.SUPPORTS_CREATE_RECORD);
		lstCapability.add(Capability.EXTENDED_WRITE);
		return lstCapability;
	}

	@Override
	public IConnection getConnection() {
		if (connection == null) {
			connection = new CBConnection();
			return connection;
		} else {
			return connection.clone();
		}
	}

	@Override
	public IMetadata getMetadata(IConnection conn)
			throws InitializationException {
		return new CBMetadata(this, (CBConnection) conn);
	}

	@Override
	public IRead getReader(IConnection conn) throws InitializationException {
		return new CBRead(this, (CBConnection) conn);
	}

	@Override
	public IRegistrationInfo getRegistrationInfo() {
		return new CBRegistrationInfo();
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

	}

	@Override
	public void setLogger(ILogger logger) {
		this.logger = logger;
	}

	public ILogger getLogger() {
		return this.logger;
	}

	@Override
	public IWrite2 getExtendedWriter(IConnection conn) {
		return new CBWrite(this, (CBConnection) conn);
	}
}
