package com.cloud.network.rules;

import com.cloud.legacymodel.exceptions.ResourceUnavailableException;
import com.cloud.legacymodel.network.VirtualRouter;
import com.cloud.network.Network;
import com.cloud.network.topology.NetworkTopologyVisitor;
import com.cloud.vm.NicProfile;
import com.cloud.vm.NicVO;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.VirtualMachineProfile;
import com.cloud.vm.dao.NicDao;
import com.cloud.vm.dao.UserVmDao;

public class PasswordToRouterRules extends RuleApplier {

    private final NicProfile nic;
    private final VirtualMachineProfile profile;

    private NicVO nicVo;

    public PasswordToRouterRules(final Network network, final NicProfile nic, final VirtualMachineProfile profile) {
        super(network);

        this.nic = nic;
        this.profile = profile;
    }

    @Override
    public boolean accept(final NetworkTopologyVisitor visitor, final VirtualRouter router) throws ResourceUnavailableException {
        _router = router;

        final UserVmDao userVmDao = visitor.getVirtualNetworkApplianceFactory().getUserVmDao();
        userVmDao.loadDetails((UserVmVO) profile.getVirtualMachine());
        // for basic zone, send vm data/password information only to the router in the same pod
        final NicDao nicDao = visitor.getVirtualNetworkApplianceFactory().getNicDao();
        nicVo = nicDao.findById(nic.getId());

        return visitor.visit(this);
    }

    public VirtualMachineProfile getProfile() {
        return profile;
    }

    public NicVO getNicVo() {
        return nicVo;
    }
}
