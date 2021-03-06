//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.couchbase.connector.constant.CBConstants;
import com.couchbase.connector.typesystem.CBTypeSystem;
import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.StandardAttributes;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordAttribute;
import com.informatica.cloud.api.adapter.metadata.TransformationInfo;
import com.informatica.cloud.api.adapter.typesystem.ITypeSystem;

public class CBRegistrationInfo implements IRegistrationInfo {

	private ArrayList<ConnectionAttribute> connAttribs;
	private ITypeSystem cbTypeSystem;
	private UUID id = UUID.fromString("10d8f7e2-a2cc-11e4-89d3-123b93f75cba");

	@Override
	public List<ConnectionAttribute> getConnectionAttributes() {
		if (connAttribs == null) {
			connAttribs = new ArrayList<ConnectionAttribute>();
			connAttribs.add(StandardAttributes.connectionUrl);
			connAttribs.add(StandardAttributes.username);
			connAttribs.add(StandardAttributes.password);
		}
		return connAttribs;
	}

	@Override
	public List<FieldAttribute> getFieldAttributes()
			throws MetadataReadException {
		ArrayList<FieldAttribute> listOfFieldAttrs = new ArrayList<FieldAttribute>();
		FieldAttribute a = new FieldAttribute();
		a.setName(CBConstants.REQUIRED_LEVEL);
		a.setDescription("Specifies the data entry requirement level of data entry enforced for the attribute.");
		listOfFieldAttrs.add(a);
		return listOfFieldAttrs;
	}

	@Override
	public String getName() {
		return CBConstants.PLUGIN_NAME;
	}

	@Override
	public String getPluginDescription() {
		return CBConstants.PLUGIN_DESC;
	}

	@Override
	public String getPluginShortName() {
		return CBConstants.PLUGIN_NAME;
	}

	@Override
	public UUID getPluginUUID() {
		return this.id;
	}

	@Override
	public List<RecordAttribute> getReadOperationAttributes()
			throws MetadataReadException {
		ArrayList<RecordAttribute> listOfRecordAttribs = new ArrayList<RecordAttribute>();
		return listOfRecordAttribs;
	}

	@Override
	public List<RecordAttribute> getRecordAttributes()
			throws MetadataReadException {
		ArrayList<RecordAttribute> listOfRecordAttribs = new ArrayList<RecordAttribute>();
		return listOfRecordAttribs;
	}

	@Override
	public List<RecordAttribute> getTransformationAttributes(
			TransformationInfo arg0) throws MetadataReadException {
		ArrayList<RecordAttribute> listOfRecordAttribs = new ArrayList<RecordAttribute>();
		return listOfRecordAttribs;
	}

	@Override
	public List<TransformationInfo> getTransformationOperations() {
		return new ArrayList<TransformationInfo>();
	}

	@Override
	public ITypeSystem getTypeSystem() {
		if (cbTypeSystem == null) {
			cbTypeSystem = new CBTypeSystem();
		}
		return cbTypeSystem;
	}

	@Override
	public List<RecordAttribute> getWriteOperationAttributes()
			throws MetadataReadException {
		ArrayList<RecordAttribute> listOfRecordAttribs = new ArrayList<RecordAttribute>();
		return listOfRecordAttribs;
	}

}
