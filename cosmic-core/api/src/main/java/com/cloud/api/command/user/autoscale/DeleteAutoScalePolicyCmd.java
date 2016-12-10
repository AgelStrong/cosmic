package com.cloud.api.command.user.autoscale;

import com.cloud.acl.SecurityChecker.AccessType;
import com.cloud.api.response.AutoScalePolicyResponse;
import com.cloud.api.response.SuccessResponse;
import com.cloud.event.EventTypes;
import com.cloud.network.as.AutoScalePolicy;
import com.cloud.user.Account;
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

@APICommand(name = "deleteAutoScalePolicy", description = "Deletes a autoscale policy.", responseObject = SuccessResponse.class, entityType = {AutoScalePolicy.class},
        requestHasSensitiveInfo = false, responseHasSensitiveInfo = false)
public class DeleteAutoScalePolicyCmd extends BaseAsyncCmd {
    public static final Logger s_logger = LoggerFactory.getLogger(DeleteAutoScalePolicyCmd.class.getName());
    private static final String s_name = "deleteautoscalepolicyresponse";
    // ///////////////////////////////////////////////////
    // ////////////// API parameters /////////////////////
    // ///////////////////////////////////////////////////

    @ACL(accessType = AccessType.OperateEntry)
    @Parameter(name = ApiConstants.ID,
            type = CommandType.UUID,
            entityType = AutoScalePolicyResponse.class,
            required = true,
            description = "the ID of the autoscale policy")
    private Long id;

    // ///////////////////////////////////////////////////
    // ///////////////// Accessors ///////////////////////
    // ///////////////////////////////////////////////////

    @Override
    public String getEventType() {
        return EventTypes.EVENT_AUTOSCALEPOLICY_DELETE;
    }

    // ///////////////////////////////////////////////////
    // ///////////// API Implementation///////////////////
    // ///////////////////////////////////////////////////

    @Override
    public String getEventDescription() {
        return "deleting AutoScale Policy: " + getId();
    }

    @Override
    public ApiCommandJobType getInstanceType() {
        return ApiCommandJobType.AutoScalePolicy;
    }

    public Long getId() {
        return id;
    }

    @Override
    public void execute() {
        CallContext.current().setEventDetails("AutoScale Policy Id: " + getId());
        final boolean result = _autoScaleService.deleteAutoScalePolicy(id);

        if (result) {
            final SuccessResponse response = new SuccessResponse(getCommandName());
            setResponseObject(response);
        } else {
            s_logger.warn("Failed to delete autoscale policy " + getId());
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to delete AutoScale Policy");
        }
    }

    @Override
    public String getCommandName() {
        return s_name;
    }

    @Override
    public long getEntityOwnerId() {
        final AutoScalePolicy autoScalePolicy = _entityMgr.findById(AutoScalePolicy.class, getId());
        if (autoScalePolicy != null) {
            return autoScalePolicy.getAccountId();
        }

        return Account.ACCOUNT_ID_SYSTEM; // no account info given, parent this command to SYSTEM so ERROR events are
        // tracked
    }
}
