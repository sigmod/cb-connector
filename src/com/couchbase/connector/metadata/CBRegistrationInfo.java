package com.couchbase.connector.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.couchbase.connector.constant.CBConstants;
import com.couchbase.connector.typesystem.CBTypeSystem;
import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.ConnectionAttributeType;
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
			
			connAttribs.add(new ConnectionAttribute(CBConstants.CONN_ATTRIB_DIRECTORY,
					ConnectionAttributeType.ALPHABET_TYPE
							| ConnectionAttributeType.NUMERIC_TYPE
							| ConnectionAttributeType.SYMBOLS_TYPE, null, null,
					true, CBConstants.CONN_ATTRIB_DIRECTORY_NUMBER));
			
			//Configuration parameter for file delimiter
			connAttribs.add(new ConnectionAttribute(
					CBConstants.CONN_ATTRIB_DELIMITER,
					ConnectionAttributeType.ALPHABET_TYPE
							| ConnectionAttributeType.NUMERIC_TYPE
							| ConnectionAttributeType.SYMBOLS_TYPE, CBConstants.DEFAULT_DELIMITER, null,
					false, CBConstants.CONN_ATTRIB_DELIMITER_NUMBER));
			
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
