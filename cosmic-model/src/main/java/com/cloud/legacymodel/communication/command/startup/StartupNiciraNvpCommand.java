package com.cloud.legacymodel.communication.command.startup;

import com.cloud.model.enumeration.HostType;

public class StartupNiciraNvpCommand extends StartupCommand {

    public StartupNiciraNvpCommand() {
        super(HostType.L2Networking);
    }
}
