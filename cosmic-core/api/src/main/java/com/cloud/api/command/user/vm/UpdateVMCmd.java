package com.cloud.api.command.user.vm;

import com.cloud.acl.RoleType;
import com.cloud.acl.SecurityChecker.AccessType;
import com.cloud.api.ACL;
import com.cloud.api.APICommand;
import com.cloud.api.APICommandGroup;
import com.cloud.api.ApiConstants;
import com.cloud.api.ApiErrorCode;
import com.cloud.api.BaseCustomIdCmd;
import com.cloud.api.Parameter;
import com.cloud.api.ResponseObject.ResponseView;
import com.cloud.api.ServerApiException;
import com.cloud.api.response.GuestOSResponse;
import com.cloud.api.response.UserVmResponse;
import com.cloud.context.CallContext;
import com.cloud.legacymodel.exceptions.InsufficientCapacityException;
import com.cloud.legacymodel.exceptions.ResourceUnavailableException;
import com.cloud.legacymodel.user.Account;
import com.cloud.legacymodel.vm.VirtualMachine;
import com.cloud.model.enumeration.MaintenancePolicy;
import com.cloud.model.enumeration.OptimiseFor;
import com.cloud.uservm.UserVm;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@APICommand(name = "updateVirtualMachine", group = APICommandGroup.VirtualMachineService, description = "Updates properties of a virtual machine. The VM has to be stopped and " +
        "restarted for the " +
        "new properties to take effect. UpdateVirtualMachine does not first check whether the VM is stopped. " +
        "Therefore, stop the VM manually before issuing this call.", responseObject = UserVmResponse.class, responseView = ResponseView.Restricted, entityType = {VirtualMachine
        .class},
        requestHasSensitiveInfo = false, responseHasSensitiveInfo = true)
public class UpdateVMCmd extends BaseCustomIdCmd {
    public static final Logger s_logger = LoggerFactory.getLogger(UpdateVMCmd.class.getName());
    private static final String s_name = "updatevirtualmachineresponse";

    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////
    @Parameter(name = ApiConstants.IS_DYNAMICALLY_SCALABLE,
            type = CommandType.BOOLEAN,
            description = "true if VM contains XS tools inorder to support dynamic scaling of VM cpu/memory")
    protected Boolean isDynamicallyScalable;
    @Parameter(name = ApiConstants.DETAILS, type = CommandType.MAP, description = "Details in key/value pairs.")
    protected Map<String, String> details;
    @Parameter(name = ApiConstants.DISPLAY_NAME, type = CommandType.STRING, description = "user generated name")
    private String displayName;
    @Parameter(name = ApiConstants.GROUP, type = CommandType.STRING, description = "group of the virtual machine")
    private String group;
    @Parameter(name = ApiConstants.HA_ENABLE, type = CommandType.BOOLEAN, description = "true if high-availability is enabled for the virtual machine, false otherwise")
    private Boolean haEnable;
    @ACL(accessType = AccessType.OperateEntry)
    @Parameter(name = ApiConstants.ID, type = CommandType.UUID, entityType = UserVmResponse.class,
            required = true, description = "The ID of the virtual machine")
    private Long id;
    @Parameter(name = ApiConstants.OS_TYPE_ID,
            type = CommandType.UUID,
            entityType = GuestOSResponse.class,
            description = "the ID of the OS type that best represents this VM.")
    private Long osTypeId;
    @Parameter(name = ApiConstants.USER_DATA,
            type = CommandType.STRING,
            description = "an optional binary data that can be sent to the virtual machine upon a successful deployment. This binary data must be base64 encoded before adding it" +
                    " to the request. Using HTTP GET (via querystring), you can send up to 2KB of data after base64 encoding. Using HTTP POST(via POST body), you can send up to " +
                    "32K of data after base64 encoding.",
            length = 32768)
    private String userData;
    @Parameter(name = ApiConstants.DISPLAY_VM, type = CommandType.BOOLEAN, description = "an optional field, whether to the display the vm to the end user or not.", authorized =
            {RoleType.Admin})
    private Boolean displayVm;
    @Parameter(name = ApiConstants.NAME, type = CommandType.STRING, description = "new host name of the vm. The VM has to be stopped/started for this update to take affect",
            since = "4.4")
    private String name;
    @Parameter(name = ApiConstants.INSTANCE_NAME, type = CommandType.STRING, description = "instance name of the user vm", since = "4.4", authorized = {RoleType.Admin})
    private String instanceName;
    @Parameter(name = ApiConstants.MANUFACTURER_STRING, type = CommandType.STRING, description = "Manufacturer String to put in hardware info, defaults to 'Mission Critical " +
            "Cloud'")
    private String manufacturerString;
    @Parameter(name = ApiConstants.OPTIMISE_FOR, type = CommandType.STRING, description = "Optimise for 'Windows' or 'Generic'")
    private String optimiseFor;
    @Parameter(name = ApiConstants.RESTART_REQUIRED, type = CommandType.BOOLEAN, description = "true if VM needs to a stop/start to receive updated VM specs on the hypervisor",
            authorized = {RoleType.Admin})
    protected Boolean requiresRestart;
    @Parameter(name = ApiConstants.MAINTENANCE_POLICY, type = CommandType.STRING, description = "either 'LiveMigrate' or 'ShutdownAndStart' when performing hypervisor maintenance")
    private String maintenancePolicy;
    @Parameter(name = ApiConstants.BOOT_MENU_TIMEOUT, type = CommandType.LONG, description = "Timeout (in ms) to for the boot menu")
    private Long bootMenuTimeout;
    @Parameter(name = ApiConstants.BOOT_ORDER, type = CommandType.STRING, description = "Comma seperated list from which device to boot from first, can one of 'hd','cdrom'," +
            "'network'")
    private String bootOrder;

    /////////////////////////////////////////////////////
    /////////////////// Accessors ///////////////////////
    /////////////////////////////////////////////////////

    public static String getResultObjectName() {
        return "virtualmachine";
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getGroup() {
        return group;
    }

    public Boolean getHaEnable() {
        return haEnable;
    }

    public String getUserData() {
        return userData;
    }

    public Boolean getDisplayVm() {
        return displayVm;
    }

    public Boolean isDynamicallyScalable() {
        return isDynamicallyScalable;
    }

    public String getHostName() {
        return name;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public Boolean getRequiresRestart() {
        return requiresRestart;
    }

    public MaintenancePolicy getMaintenancePolicy() {
        if (maintenancePolicy != null) {
            return MaintenancePolicy.valueOf(maintenancePolicy);
        } else {
            return null;
        }
    }

    public Map<String, String> getDetails() {
        if (details == null || details.isEmpty()) {
            return null;
        }

        final Collection<String> paramsCollection = details.values();
        return (Map<String, String>) paramsCollection.toArray()[0];
    }

    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////

    public Long getOsTypeId() {
        return osTypeId;
    }

    @Override
    public void execute() throws ResourceUnavailableException, InsufficientCapacityException, ServerApiException {
        CallContext.current().setEventDetails("Vm Id: " + getId());
        final UserVm result = _userVmService.updateVirtualMachine(this);
        if (result != null) {
            final UserVmResponse response = _responseGenerator.createUserVmResponse(ResponseView.Restricted, "virtualmachine", result).get(0);
            response.setResponseName(getCommandName());
            setResponseObject(response);
        } else {
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to update vm");
        }
    }

    @Override
    public String getCommandName() {
        return s_name;
    }

    @Override
    public long getEntityOwnerId() {
        final UserVm userVm = _entityMgr.findById(UserVm.class, getId());
        if (userVm != null) {
            return userVm.getAccountId();
        }

        return Account.ACCOUNT_ID_SYSTEM; // no account info given, parent this command to SYSTEM so ERROR events are tracked
    }

    public Long getId() {
        return id;
    }

    @Override
    public void checkUuid() {
        if (getCustomId() != null) {
            _uuidMgr.checkUuid(getCustomId(), UserVm.class);
        }
    }

    public OptimiseFor getOptimiseFor() {
        if (optimiseFor != null) {
            if (getHttpMethod() == HTTPMethod.POST) {
                optimiseFor = new String(Base64.decodeBase64(this.optimiseFor));
            }
            return OptimiseFor.valueOf(optimiseFor);
        } else {
            return OptimiseFor.Generic;
        }
    }

    public String getManufacturerString() {
        return manufacturerString;
    }

    public Long getBootMenuTimeout() {
        return bootMenuTimeout;
    }

    public String getBootOrder() {
        return bootOrder;
    }
}
