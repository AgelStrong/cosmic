package com.cloud.hypervisor.kvm.resource.wrapper;

import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.legacymodel.communication.answer.Answer;
import com.cloud.legacymodel.communication.answer.CheckHealthAnswer;
import com.cloud.legacymodel.communication.command.CheckHealthCommand;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;

@ResourceWrapper(handles = CheckHealthCommand.class)
public final class LibvirtCheckHealthCommandWrapper
        extends CommandWrapper<CheckHealthCommand, Answer, LibvirtComputingResource> {

    @Override
    public Answer execute(final CheckHealthCommand command, final LibvirtComputingResource libvirtComputingResource) {
        return new CheckHealthAnswer(command, true);
    }
}
