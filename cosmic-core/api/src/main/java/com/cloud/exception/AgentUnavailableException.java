package com.cloud.exception;

import com.cloud.host.Host;
import com.cloud.legacymodel.exceptions.ResourceUnavailableException;

public class AgentUnavailableException extends ResourceUnavailableException {
    public AgentUnavailableException(final long agentId) {
        this("Unable to reach host.", agentId);
    }

    public AgentUnavailableException(final String msg, final long agentId) {
        this(msg, agentId, null);
    }

    public AgentUnavailableException(final String msg, final long agentId, final Throwable cause) {
        super("Host " + agentId + ": " + msg, Host.class, agentId, cause);
    }
}
