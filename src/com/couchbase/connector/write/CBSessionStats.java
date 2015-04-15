package com.couchbase.connector.write;

import java.util.ArrayList;
import java.util.List;

import com.informatica.cloud.api.adapter.runtime.SessionStats;
import com.informatica.cloud.api.adapter.runtime.SessionStats.Operation;

public class CBSessionStats {
	private final List<SessionStats> cumulativeSessionStats = new ArrayList<SessionStats>();
	private final SessionStats insertStats = new SessionStats(Operation.INSERT,
			0, 0);
	private final SessionStats updateStats = new SessionStats(Operation.UPDATE,
			0, 0);
	private final SessionStats deleteStats = new SessionStats(Operation.DELETE,
			0, 0);
	private final SessionStats upsertStats = new SessionStats(Operation.UPSERT,
			0, 0);

	public CBSessionStats() {
		cumulativeSessionStats.add(insertStats);
		cumulativeSessionStats.add(updateStats);
		cumulativeSessionStats.add(upsertStats);
		cumulativeSessionStats.add(deleteStats);
	}

	public List<SessionStats> getCumulativeSessionStats() {
		return cumulativeSessionStats;
	}

	public SessionStats getInsertStats() {
		return insertStats;
	}

	public SessionStats getUpdateStats() {
		return updateStats;
	}

	public SessionStats getDeleteStats() {
		return deleteStats;
	}

	public SessionStats getUpsertStats() {
		return upsertStats;
	}

}
