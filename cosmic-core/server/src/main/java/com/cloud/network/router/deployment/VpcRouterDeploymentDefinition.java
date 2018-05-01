package com.cloud.network.router.deployment;

import com.cloud.dc.dao.VlanDao;
import com.cloud.deploy.DataCenterDeployment;
import com.cloud.deploy.DeployDestination;
import com.cloud.legacymodel.exceptions.CloudRuntimeException;
import com.cloud.legacymodel.exceptions.ConcurrentOperationException;
import com.cloud.legacymodel.exceptions.InsufficientAddressCapacityException;
import com.cloud.legacymodel.exceptions.InsufficientCapacityException;
import com.cloud.legacymodel.exceptions.ResourceUnavailableException;
import com.cloud.legacymodel.user.Account;
import com.cloud.network.Network;
import com.cloud.network.PhysicalNetwork;
import com.cloud.network.PhysicalNetworkServiceProvider;
import com.cloud.network.VirtualRouterProvider.Type;
import com.cloud.network.dao.PhysicalNetworkDao;
import com.cloud.network.vpc.Vpc;
import com.cloud.network.vpc.VpcManager;
import com.cloud.network.vpc.dao.VpcDao;
import com.cloud.network.vpc.dao.VpcOfferingDao;
import com.cloud.vm.DomainRouterVO;
import com.cloud.vm.VirtualMachineProfile.Param;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VpcRouterDeploymentDefinition extends RouterDeploymentDefinition {
    private static final Logger logger = LoggerFactory.getLogger(VpcRouterDeploymentDefinition.class);

    protected VpcDao vpcDao;
    protected VpcOfferingDao vpcOffDao;
    protected PhysicalNetworkDao pNtwkDao;
    protected VpcManager vpcMgr;
    protected VlanDao vlanDao;

    protected Vpc vpc;

    protected VpcRouterDeploymentDefinition(final Network guestNetwork, final Vpc vpc, final DeployDestination dest, final Account owner,
                                            final Map<Param, Object> params) {

        super(guestNetwork, dest, owner, params);

        this.vpc = vpc;
    }

    @Override
    public Vpc getVpc() {
        return vpc;
    }

    @Override
    public boolean isVpcRouter() {
        return true;
    }

    @Override
    protected void lock() {
        final Vpc vpcLock = vpcDao.acquireInLockTable(vpc.getId());
        if (vpcLock == null) {
            throw new ConcurrentOperationException("Unable to lock vpc " + vpc.getId());
        }
        tableLockId = vpcLock.getId();
    }

    @Override
    protected void unlock() {
        if (tableLockId != null) {
            vpcDao.releaseFromLockTable(tableLockId);
            if (logger.isDebugEnabled()) {
                logger.debug("Lock is released for vpc id " + tableLockId + " as a part of router startup in " + dest);
            }
        }
    }

    @Override
    protected void checkPreconditions() {
        // No preconditions for Vpc
    }

    @Override
    protected List<DeployDestination> findDestinations() {
        final List<DeployDestination> destinations = new ArrayList<>();
        destinations.add(dest);
        return destinations;
    }

    /**
     * @return if the deployment can proceed
     * @see RouterDeploymentDefinition#prepareDeployment()
     */
    @Override
    protected boolean prepareDeployment() {
        isPublicNetwork = needsPublicNic();

        return true;
    }

    @Override
    public boolean needsPublicNic() {
        // Check for SourceNat
        if (hasService(Network.Service.SourceNat)) {
            return true;
        }
        // Check for VPN
        if (hasService(Network.Service.Vpn)) {
            return true;
        }
        logger.info(String.format("No SourceNat service and no VPN service found on VPC %s, so we do not need to add a public nic", vpc.getName()));
        return false;
    }

    @Override
    public boolean hasSourceNatService() {
        return hasService(Network.Service.SourceNat);
    }

    public boolean hasGatewayService() {
        return hasService(Network.Service.Gateway);
    }

    private boolean hasService(final Network.Service service) {
        final Map<Network.Service, Set<Network.Provider>> vpcOffSvcProvidersMap = vpcMgr.getVpcOffSvcProvidersMap(vpc.getVpcOfferingId());
        try {
            vpcOffSvcProvidersMap.get(service).contains(Network.Provider.VPCVirtualRouter);
            logger.debug(String.format("Found %s service on VPC %s, adding a public nic", service.getName(), vpc.getName()));
            return true;
        } catch (final Exception e) {
            logger.debug(String.format("No %s service found on VPC %s", service.getName(), vpc.getName()));
        }
        return false;
    }

    @Override
    protected void findSourceNatIP() throws InsufficientAddressCapacityException, ConcurrentOperationException {
        sourceNatIp = null;
        if (needsPublicNic()) {
            sourceNatIp = vpcMgr.assignSourceNatIpAddressToVpc(owner, vpc);
        }
    }

    @Override
    protected void findOrDeployVirtualRouter() throws ConcurrentOperationException, InsufficientCapacityException, ResourceUnavailableException {
        final Vpc vpc = getVpc();
        if (vpc != null) {
            // This call will associate any existing router to the "routers" attribute.
            // It's needed in order to continue with the VMs deployment.
            planDeploymentRouters();
            if (routers.size() == MAX_NUMBER_OF_ROUTERS) {
                // If we have 2 routers already deployed, do nothing and return.
                return;
            }
        }
        super.findOrDeployVirtualRouter();
    }

    @Override
    protected void findVirtualProvider() {
        final List<? extends PhysicalNetwork> pNtwks = pNtwkDao.listByZone(vpc.getZoneId());

        for (final PhysicalNetwork pNtwk : pNtwks) {
            final PhysicalNetworkServiceProvider provider = physicalProviderDao.findByServiceProvider(pNtwk.getId(), Type.VPCVirtualRouter.toString());
            if (provider == null) {
                throw new CloudRuntimeException("Cannot find service provider " + Type.VPCVirtualRouter.toString() + " in physical network " + pNtwk.getId());
            }
            vrProvider = vrProviderDao.findByNspIdAndType(provider.getId(), Type.VPCVirtualRouter);
            if (vrProvider != null) {
                break;
            }
        }
    }

    @Override
    protected void findServiceOfferingId() {
        serviceOfferingId = vpcOffDao.findById(vpc.getVpcOfferingId()).getServiceOfferingId();
        if (serviceOfferingId == null) {
            findDefaultServiceOfferingId();
        }
    }

    @Override
    protected void findSecondaryServiceOfferingId() {
        secondaryServiceOfferingId = vpcOffDao.findById(vpc.getVpcOfferingId()).getSecondaryServiceOfferingId();
        if (secondaryServiceOfferingId == null) {
            secondaryServiceOfferingId = serviceOfferingId;
        }
    }

    @Override
    protected void deployAllVirtualRouters() throws ConcurrentOperationException, InsufficientCapacityException,
            ResourceUnavailableException {

        // Implement Redundant Vpc
        final int routersToDeploy = getNumberOfRoutersToDeploy();
        for (int i = 0; i < routersToDeploy; i++) {
            // Don't start the router as we are holding the network lock that needs to be released at the end of router allocation
            final DomainRouterVO router = nwHelper.deployRouter(this, false);

            if (router != null) {
                routers.add(router);
            } else {
                logger.error("We didn't get any routers returned, probably the deployment failed.");
            }
        }
    }

    @Override
    protected void planDeploymentRouters() {
        routers = routerDao.listByVpcId(vpc.getId());
    }

    @Override
    public void generateDeploymentPlan() {
        plan = new DataCenterDeployment(dest.getZone().getId());
    }

    @Override
    public boolean isRedundant() {
        return vpc.isRedundant();
    }
}
