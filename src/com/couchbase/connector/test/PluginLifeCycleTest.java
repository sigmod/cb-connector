//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.couchbase.connector.plugin.CBPlugin;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.connection.StandardAttributes;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;

public class PluginLifeCycleTest {

	static final Map<String, String> connectionAttributeMap = new HashMap<String, String>();

	private void initConnectionAttributeMap() {
		connectionAttributeMap
				.put(StandardAttributes.connectionUrl.getName(),
						"jdbc:couchbase://localhost:8093;queryEnabled=1;logLevel=6;logPath=log");
		connectionAttributeMap.put(StandardAttributes.username.getName(),
				"Administrator");
		connectionAttributeMap.put(StandardAttributes.password.getName(),
				"password");
	}

	@Test
	public void lifeCycleTest() throws Exception {
		initConnectionAttributeMap();
		CBPlugin plugin = new CBPlugin();

		// Creates a connection.
		IConnection connection = plugin.getConnection();

		// Sets the connection attributes map.
		connection.setConnectionAttributes(connectionAttributeMap);

		// Establishes the connection.
		if (!connection.connect()) {
			throw new IllegalStateException("Connection is not established.");
		}

		// Validates the connection.
		if (connection.validate()) {
			System.out.println("connection established");
		}

		IMetadata metadata = plugin.getMetadata(connection);
		List<RecordInfo> metadataRecords = metadata.getAllRecords();

		for (RecordInfo record : metadataRecords) {
			List<Field> fields = metadata.getFields(record, true);
			System.out.println(record.getInstanceName());
			for (Field field : fields) {
				System.out.print(field.getDisplayName() + ":"
						+ field.getDatatype().getName() + "|");
			}
			System.out.println();
		}

		connection.disconnect();
	}

}
