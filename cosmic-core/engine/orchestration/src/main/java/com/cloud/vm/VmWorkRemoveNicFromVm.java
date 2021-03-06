package com.cloud.vm;

public class VmWorkRemoveNicFromVm extends VmWork {
    Long nicId;

    public VmWorkRemoveNicFromVm(final long userId, final long accountId, final long vmId, final String handlerName, final Long nicId) {
        super(userId, accountId, vmId, handlerName);

        this.nicId = nicId;
    }

    public Long getNicId() {
        return nicId;
    }
}
