package com.cloud.api.command.user.firewall;

import com.cloud.acl.SecurityChecker.AccessType;
import com.cloud.api.response.AccountResponse;
import com.cloud.api.response.FirewallRuleResponse;
import com.cloud.api.response.SuccessResponse;
import com.cloud.event.EventTypes;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.network.rules.FirewallRule;
import org.apache.cloudstack.api.ACL;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiCommandJobType;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.context.CallContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@APICommand(name = "deleteEgressFirewallRule", description = "Deletes an egress firewall rule", responseObject = SuccessResponse.class, entityType = {FirewallRule.class},
        requestHasSensitiveInfo = false, responseHasSensitiveInfo = false)
public class DeleteEgressFirewallRuleCmd extends BaseAsyncCmd {
    public static final Logger s_logger = LoggerFactory.getLogger(DeleteEgressFirewallRuleCmd.class.getName());
    private static final String s_name = "deleteegressfirewallruleresponse";

    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////

    @ACL(accessType = AccessType.OperateEntry)
    @Parameter(name = ApiConstants.ID, type = CommandType.UUID, entityType = FirewallRuleResponse.class, required = true, description = "the ID of the firewall rule")
    private Long id;

    // unexposed parameter needed for events logging
    @Parameter(name = ApiConstants.ACCOUNT_ID, type = CommandType.UUID, entityType = AccountResponse.class, expose = false)
    private Long ownerId;

    /////////////////////////////////////////////////////
    /////////////////// Accessors ///////////////////////
    /////////////////////////////////////////////////////

    public Long getId() {
        return id;
    }

    @Override
    public String getEventType() {
        return EventTypes.EVENT_FIREWALL_EGRESS_CLOSE;
    }

    @Override
    public String getEventDescription() {
        return ("Deleting egress firewall rule id=" + id);
    }

    @Override
    public ApiCommandJobType getInstanceType() {
        return ApiCommandJobType.FirewallRule;
    }

    @Override
    public String getSyncObjType() {
        return BaseAsyncCmd.networkSyncObject;
    }

    @Override
    public Long getSyncObjId() {
        final FirewallRule fw = _firewallService.getFirewallRule(id);
        if (fw != null) {
            return fw.getNetworkId();
        }
        return null;
    }

    @Override
    public void execute() throws ResourceUnavailableException {
        CallContext.current().setEventDetails("Rule Id: " + id);
        final boolean result = _firewallService.revokeEgressFirewallRule(id, true);

        if (result) {
            final SuccessResponse response = new SuccessResponse(getCommandName());
            setResponseObject(response);
        } else {
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to delete egress firewall rule");
        }
    }

    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////
    @Override
    public String getCommandName() {
        return s_name;
    }

    @Override
    public long getEntityOwnerId() {
        if (ownerId == null) {
            final FirewallRule rule = _entityMgr.findById(FirewallRule.class, id);
            if (rule == null) {
                throw new InvalidParameterValueException("Unable to find egress firewall rule by id");
            } else {
                ownerId = _entityMgr.findById(FirewallRule.class, id).getAccountId();
            }
        }
        return ownerId;
    }
}
