package com.cloud.network.router;

import com.cloud.agent.AgentManager;
import com.cloud.agent.manager.Commands;
import com.cloud.alert.AlertManager;
import com.cloud.context.CallContext;
import com.cloud.deploy.DataCenterDeployment;
import com.cloud.deploy.DeployDestination;
import com.cloud.deploy.DeploymentPlan;
import com.cloud.deploy.DeploymentPlanner.ExcludeList;
import com.cloud.engine.orchestration.service.NetworkOrchestrationService;
import com.cloud.framework.config.ConfigKey;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.legacymodel.communication.answer.Answer;
import com.cloud.legacymodel.dc.DataCenter;
import com.cloud.legacymodel.dc.HostStatus;
import com.cloud.legacymodel.dc.Pod;
import com.cloud.legacymodel.exceptions.AgentUnavailableException;
import com.cloud.legacymodel.exceptions.CloudException;
import com.cloud.legacymodel.exceptions.CloudRuntimeException;
import com.cloud.legacymodel.exceptions.ConcurrentOperationException;
import com.cloud.legacymodel.exceptions.InsufficientAddressCapacityException;
import com.cloud.legacymodel.exceptions.InsufficientCapacityException;
import com.cloud.legacymodel.exceptions.InsufficientServerCapacityException;
import com.cloud.legacymodel.exceptions.OperationTimedoutException;
import com.cloud.legacymodel.exceptions.ResourceUnavailableException;
import com.cloud.legacymodel.exceptions.StorageUnavailableException;
import com.cloud.legacymodel.network.Network;
import com.cloud.legacymodel.network.Nic;
import com.cloud.legacymodel.network.VirtualRouter;
import com.cloud.legacymodel.network.VirtualRouter.RedundantState;
import com.cloud.legacymodel.network.VirtualRouter.Role;
import com.cloud.legacymodel.network.vpc.Vpc;
import com.cloud.legacymodel.to.NicTO;
import com.cloud.legacymodel.user.Account;
import com.cloud.legacymodel.user.User;
import com.cloud.legacymodel.vm.VirtualMachine.State;
import com.cloud.model.enumeration.BroadcastDomainType;
import com.cloud.model.enumeration.HypervisorType;
import com.cloud.model.enumeration.VolumeType;
import com.cloud.network.IpAddressManager;
import com.cloud.network.NetworkModel;
import com.cloud.network.Networks.IsolationType;
import com.cloud.network.addr.PublicIp;
import com.cloud.network.dao.IPAddressDao;
import com.cloud.network.dao.NetworkDao;
import com.cloud.network.dao.UserIpv6AddressDao;
import com.cloud.network.router.deployment.RouterDeploymentDefinition;
import com.cloud.network.vpn.Site2SiteVpnManager;
import com.cloud.offering.NetworkOffering;
import com.cloud.offerings.dao.NetworkOfferingDao;
import com.cloud.resource.ResourceManager;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.user.AccountManager;
import com.cloud.user.UserVO;
import com.cloud.user.dao.UserDao;
import com.cloud.utils.maint.Version;
import com.cloud.utils.net.NetUtils;
import com.cloud.vm.DomainRouterVO;
import com.cloud.vm.NicProfile;
import com.cloud.vm.VirtualMachineManager;
import com.cloud.vm.VirtualMachineName;
import com.cloud.vm.VirtualMachineProfile.Param;
import com.cloud.vm.dao.DomainRouterDao;
import com.cloud.vm.dao.NicDao;

import javax.annotation.PostConstruct;
import javax.ejb.Local;
import javax.inject.Inject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Local(value = {NetworkHelper.class})
public class NetworkHelperImpl implements NetworkHelper {

    private static final Logger logger = LoggerFactory.getLogger(NetworkHelperImpl.class);

    protected static Account s_systemAccount;
    protected static String s_vmInstanceName;
    protected final Map<HypervisorType, ConfigKey<String>> hypervisorsMap = new HashMap<>();
    @Inject
    protected NicDao _nicDao;
    @Inject
    protected NetworkDao _networkDao;
    @Inject
    protected NetworkOfferingDao _networkOfferingDao;
    @Inject
    protected DomainRouterDao _routerDao;
    @Inject
    protected NetworkModel _networkModel;
    @Inject
    protected IPAddressDao _ipAddressDao;
    @Inject
    protected NetworkOrchestrationService _networkMgr;
    @Inject
    protected ServiceOfferingDao _serviceOfferingDao;
    @Inject
    protected VirtualMachineManager _itMgr;
    @Inject
    protected IpAddressManager _ipAddrMgr;
    @Inject
    private AgentManager _agentMgr;
    @Inject
    private AlertManager _alertMgr;
    @Inject
    private AccountManager _accountMgr;
    @Inject
    private Site2SiteVpnManager _s2sVpnMgr;
    @Inject
    private HostDao _hostDao;
    @Inject
    private VolumeDao _volumeDao;
    @Inject
    private VMTemplateDao _templateDao;
    @Inject
    private ResourceManager _resourceMgr;
    @Inject
    private UserIpv6AddressDao _ipv6Dao;
    @Inject
    private UserDao _userDao;

    public static void setSystemAccount(final Account systemAccount) {
        s_systemAccount = systemAccount;
    }

    public static void setVMInstanceName(final String vmInstanceName) {
        s_vmInstanceName = vmInstanceName;
    }

    @PostConstruct
    protected void setupHypervisorsMap() {
        hypervisorsMap.put(HypervisorType.XenServer, VirtualNetworkApplianceManager.RouterTemplateXen);
        hypervisorsMap.put(HypervisorType.KVM, VirtualNetworkApplianceManager.RouterTemplateKvm);
    }

    @Override
    public boolean sendCommandsToRouter(final VirtualRouter router, final Commands cmds) throws AgentUnavailableException, ResourceUnavailableException {
        if (!checkRouterVersion(router)) {
            logger.debug("Router requires upgrade. Unable to send command to router:" + router.getId() + ", router template version : " + router.getTemplateVersion()
                    + ", minimal required version : " + NetworkOrchestrationService.MinVRVersion.valueIn(router.getDataCenterId()));
            throw new ResourceUnavailableException("Unable to send command. Router requires upgrade", VirtualRouter.class, router.getId());
        }
        Answer[] answers = null;
        try {
            answers = _agentMgr.send(router.getHostId(), cmds);
        } catch (final OperationTimedoutException e) {
            logger.warn("Timed Out", e);
            throw new AgentUnavailableException("Unable to send commands to virtual router ", router.getHostId(), e);
        }

        if (answers == null || answers.length != cmds.size()) {
            return false;
        }

        // FIXME: Have to return state for individual command in the future
        boolean result = true;
        for (final Answer answer : answers) {
            if (!answer.getResult()) {
                result = false;
                break;
            }
        }
        return result;
    }

    @Override
    public void handleSingleWorkingRedundantRouter(final List<? extends VirtualRouter> connectedRouters, final List<? extends VirtualRouter> disconnectedRouters,
                                                   final String reason) throws ResourceUnavailableException {
        if (connectedRouters.isEmpty() || disconnectedRouters.isEmpty()) {
            return;
        }

        for (final VirtualRouter virtualRouter : connectedRouters) {
            if (!virtualRouter.getIsRedundantRouter()) {
                throw new ResourceUnavailableException("Who is calling this with non-redundant router or non-domain router?", DataCenter.class, virtualRouter.getDataCenterId());
            }
        }

        for (final VirtualRouter virtualRouter : disconnectedRouters) {
            if (!virtualRouter.getIsRedundantRouter()) {
                throw new ResourceUnavailableException("Who is calling this with non-redundant router or non-domain router?", DataCenter.class, virtualRouter.getDataCenterId());
            }
        }

        connectedRouters.get(0);
        DomainRouterVO disconnectedRouter = (DomainRouterVO) disconnectedRouters.get(0);

        if (logger.isDebugEnabled()) {
            logger.debug("About to stop the router " + disconnectedRouter.getInstanceName() + " due to: " + reason);
        }
        final String title = "Virtual router " + disconnectedRouter.getInstanceName() + " would be stopped after connecting back, due to " + reason;
        final String context = "Virtual router (name: " + disconnectedRouter.getInstanceName() + ", id: " + disconnectedRouter.getId()
                + ") would be stopped after connecting back, due to: " + reason;
        _alertMgr.sendAlert(AlertManager.AlertType.ALERT_TYPE_DOMAIN_ROUTER, disconnectedRouter.getDataCenterId(), disconnectedRouter.getPodIdToDeployIn(), title, context);
        disconnectedRouter.setStopPending(true);
        disconnectedRouter = _routerDao.persist(disconnectedRouter);
    }

    @Override
    public NicTO getNicTO(final VirtualRouter router, final Long networkId, final String broadcastUri) {
        final NicProfile nicProfile = _networkModel.getNicProfile(router, networkId, broadcastUri);

        return _itMgr.toNicTO(nicProfile, router.getHypervisorType());
    }

    @Override
    public VirtualRouter destroyRouter(final long routerId, final Account caller, final Long callerUserId) throws ResourceUnavailableException, ConcurrentOperationException {

        if (logger.isDebugEnabled()) {
            logger.debug("Attempting to destroy router " + routerId);
        }

        final DomainRouterVO router = _routerDao.findById(routerId);
        if (router == null) {
            return null;
        }

        _accountMgr.checkAccess(caller, null, true, router);

        _itMgr.expunge(router.getUuid());
        _routerDao.remove(router.getId());
        return router;
    }

    @Override
    public boolean checkRouterVersion(final VirtualRouter router) {
        if (!VirtualNetworkApplianceManagerImpl.routerVersionCheckEnabled.value()) {
            // Router version check is disabled.
            return true;
        }
        if (router.getTemplateVersion() == null) {
            return false;
        }
        final long dcid = router.getDataCenterId();
        final String trimmedVersion = Version.trimRouterVersion(router.getTemplateVersion());
        return Version.compare(trimmedVersion, NetworkOrchestrationService.MinVRVersion.valueIn(dcid)) >= 0;
    }

    @Override
    public List<DomainRouterVO> startRouters(final RouterDeploymentDefinition routerDeploymentDefinition) throws StorageUnavailableException, InsufficientCapacityException,
            ConcurrentOperationException, ResourceUnavailableException {

        final List<DomainRouterVO> runningRouters = new ArrayList<>();

        for (DomainRouterVO router : routerDeploymentDefinition.getRouters()) {
            boolean skip = false;
            final State state = router.getState();
            if (router.getHostId() != null && state != State.Running) {
                final HostVO host = _hostDao.findById(router.getHostId());
                if (host == null || host.getState() != HostStatus.Up) {
                    skip = true;
                }
            }
            if (!skip) {
                if (state != State.Running) {
                    final Account caller = CallContext.current().getCallingAccount();
                    final User callerUser = _accountMgr.getActiveUser(CallContext.current().getCallingUserId());
                    router = startVirtualRouter(router, callerUser, caller, routerDeploymentDefinition.getParams());
                }
                if (router != null) {
                    runningRouters.add(router);
                }
            }
        }
        return runningRouters;
    }

    protected DomainRouterVO start(DomainRouterVO router, final Map<Param, Object> params, final DeploymentPlan planToDeploy)
            throws InsufficientCapacityException, ConcurrentOperationException, ResourceUnavailableException {
        logger.debug("Starting router " + router);
        try {
            _itMgr.advanceStart(router.getUuid(), params, planToDeploy, null);
        } catch (final OperationTimedoutException e) {
            throw new ResourceUnavailableException("Starting router " + router + " failed! " + e.toString(), DataCenter.class, router.getDataCenterId());
        }
        if (router.isStopPending()) {
            logger.info("Clear the stop pending flag of router " + router.getHostName() + " after start router successfully!");
            router.setStopPending(false);
            router = _routerDao.persist(router);
        }

        // Set date and version we start this VM
        router.setLastStartDateTime(getCurrentLocalDateTimeStamp());
        router.setLastStartVersion(NetworkHelperImpl.class.getPackage().getImplementationVersion());
        router = _routerDao.persist(router);

        // We don't want the failure of VPN Connection affect the status of
        // router, so we try to make connection
        // only after router start successfully
        final Long vpcId = router.getVpcId();
        if (vpcId != null) {
            _s2sVpnMgr.reconnectDisconnectedVpnByVpc(vpcId);
        }
        return _routerDao.findById(router.getId());
    }

    private String getCurrentLocalDateTimeStamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    protected DomainRouterVO waitRouter(final DomainRouterVO router) {
        DomainRouterVO vm = _routerDao.findById(router.getId());

        if (logger.isDebugEnabled()) {
            logger.debug("Router " + router.getInstanceName() + " is not fully up yet, we will wait");
        }
        while (vm.getState() == State.Starting) {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
            }

            // reload to get the latest state info
            vm = _routerDao.findById(router.getId());
        }

        if (vm.getState() == State.Running) {
            if (logger.isDebugEnabled()) {
                logger.debug("Router " + router.getInstanceName() + " is now fully up");
            }

            return router;
        }

        logger.warn("Router " + router.getInstanceName() + " failed to start. current state: " + vm.getState());
        return null;
    }

    @Override
    public DomainRouterVO startVirtualRouter(final DomainRouterVO router, final User user, final Account caller, final Map<Param, Object> params)
            throws InsufficientCapacityException, ConcurrentOperationException, ResourceUnavailableException {
        logger.info("Starting Virtual Router {}", router);

        if (router.getRole() != Role.VIRTUAL_ROUTER || !router.getIsRedundantRouter()) {
            logger.debug("Will start to deploy router {} without any avoidance rules", router);
            return start(router, params, null);
        }

        if (router.getState() == State.Running) {
            logger.debug("Redundant router {} is already running!", router);
            return router;
        }

        // If another thread has already requested a VR start, there is a
        // transition period for VR to transit from
        // Starting to Running, there exist a race conditioning window here
        // We will wait until VR is up or fail
        if (router.getState() == State.Starting) {
            return waitRouter(router);
        }

        final DataCenterDeployment plan = new DataCenterDeployment(0, null, null, null, null, null);
        final List<Long> networkIds = _routerDao.getRouterNetworks(router.getId());

        DomainRouterVO routerToBeAvoid = null;

        List<DomainRouterVO> routerList = null;
        if (router.getVpcId() != null) {
            routerList = _routerDao.listByVpcId(router.getVpcId());
        } else if (networkIds.size() != 0) {
            routerList = _routerDao.findByNetwork(networkIds.get(0));
        }

        if (routerList != null) {
            for (final DomainRouterVO rrouter : routerList) {
                if (rrouter.getHostId() != null && rrouter.getIsRedundantRouter() && rrouter.getState() == State.Running) {
                    if (routerToBeAvoid != null) {
                        throw new ResourceUnavailableException("Try to start router " + router.getInstanceName() + "(" + router.getId() + ")"
                                + ", but there are already two redundant routers with IP " + router.getPublicIpAddress() + ", they are " + rrouter.getInstanceName() + "("
                                + rrouter.getId() + ") and " + routerToBeAvoid.getInstanceName() + "(" + routerToBeAvoid.getId() + ")", DataCenter.class,
                                rrouter.getDataCenterId());
                    }
                    routerToBeAvoid = rrouter;
                }
            }
        }
        if (routerToBeAvoid == null) {
            logger.debug("Will start to deploy router {} without any avoidance rules (no router to be avoided)", router);
            return start(router, params, null);
        }

        // We would try best to deploy the router to another place
        final int retryIndex = 5;
        final ExcludeList[] avoids = getExcludeLists(routerToBeAvoid, retryIndex);

        logger.debug("Will start to deploy router {} with the most strict avoidance rules first (total number of attempts = {})", router, retryIndex);
        DomainRouterVO result = null;
        for (int i = 0; i < retryIndex; i++) {
            plan.setAvoids(avoids[i]);
            try {
                logger.debug("Starting router {} while trying to {}", router, avoids[i]);
                result = start(router, params, plan);
            } catch (final CloudException | CloudRuntimeException e) {
                logger.debug("Failed to start virtual router {} while trying to {} ({} attempts to go)", avoids[i], retryIndex - i);
                result = null;
            }
            if (result != null) {
                break;
            }
        }
        return result;
    }

    private ExcludeList[] getExcludeLists(final DomainRouterVO routerToBeAvoid, final int retryIndex) {
        final ExcludeList[] avoids = new ExcludeList[retryIndex];
        avoids[0] = new ExcludeList();
        avoids[0].addPod(routerToBeAvoid.getPodIdToDeployIn());
        avoids[1] = new ExcludeList();
        avoids[1].addCluster(_hostDao.findById(routerToBeAvoid.getHostId()).getClusterId());
        avoids[2] = new ExcludeList();
        final List<VolumeVO> volumes = _volumeDao.findByInstanceAndType(routerToBeAvoid.getId(), VolumeType.ROOT);
        if (volumes != null && volumes.size() != 0) {
            avoids[2].addPool(volumes.get(0).getPoolId());
        }
        avoids[2].addHost(routerToBeAvoid.getHostId());
        avoids[3] = new ExcludeList();
        avoids[3].addHost(routerToBeAvoid.getHostId());
        avoids[4] = new ExcludeList();
        return avoids;
    }

    @Override
    public DomainRouterVO deployRouter(final RouterDeploymentDefinition routerDeploymentDefinition, final boolean startRouter)
            throws InsufficientAddressCapacityException, InsufficientServerCapacityException, InsufficientCapacityException, StorageUnavailableException,
            ResourceUnavailableException {

        final List<DomainRouterVO> routers;
        final boolean isRedundant;

        if (routerDeploymentDefinition.isVpcRouter()) {
            final Vpc vpc = routerDeploymentDefinition.getVpc();
            routers = _routerDao.listByVpcId(vpc.getId());
            isRedundant = vpc.isRedundant();
        } else {
            final Network guestnetwork = routerDeploymentDefinition.getGuestNetwork();
            routers = _routerDao.listByNetworkAndRole(guestnetwork.getId(), Role.VIRTUAL_ROUTER);
            isRedundant = guestnetwork.isRedundant();
        }

        ServiceOfferingVO routerOffering = _serviceOfferingDao.findById(routerDeploymentDefinition.getServiceOfferingId());
        if (isRedundant && routers != null && routers.size() == 1 && routers.get(0).getServiceOfferingId() == routerDeploymentDefinition.getServiceOfferingId()) {
            routerOffering = _serviceOfferingDao.findById(routerDeploymentDefinition.getSecondaryServiceOfferingId());
        }

        Long routerUnicastId = 1L;
        if (isRedundant && routers != null && routers.size() == 1 && routers.get(0).getRouterUnicastId() == 1L) {
            routerUnicastId = 2L;
        }

        _serviceOfferingDao.loadDetails(routerOffering);
        final String serviceofferingHypervisor = routerOffering.getDetail("hypervisor");
        if (serviceofferingHypervisor != null && !serviceofferingHypervisor.isEmpty()) {
            logger.debug(String.format("Found hypervisor '%s' in details of serviceoffering with id %s. Going to check if that hypervisor is available.",
                    serviceofferingHypervisor, routerDeploymentDefinition.getServiceOfferingId()));
        }

        final Account owner = routerDeploymentDefinition.getOwner();

        // Router is the network element, we don't know the hypervisor type yet.
        // Try to allocate the domR twice using diff hypervisors, and when
        // failed both times, throw the exception up
        final List<HypervisorType> hypervisors = getHypervisors(routerDeploymentDefinition);

        int allocateRetry = 0;
        int startRetry = 0;
        DomainRouterVO router = null;
        for (final Iterator<HypervisorType> iter = hypervisors.iterator(); iter.hasNext(); ) {
            final HypervisorType hType = iter.next();
            try {
                final long id = _routerDao.getNextInSequence(Long.class, "id");
                if (serviceofferingHypervisor != null && !serviceofferingHypervisor.isEmpty() && !hType.toString().equalsIgnoreCase(serviceofferingHypervisor)) {
                    logger.debug(String.format("Skipping hypervisor type '%s' as the service offering details request hypervisor '%s'",
                            hType, serviceofferingHypervisor));
                    continue;
                }

                logger.debug(String.format("Allocating the VR with id=%s in datacenter %s with the hypervisor type %s", id, routerDeploymentDefinition.getDest().getZone(), hType));

                final String templateName = retrieveTemplateName(hType, routerDeploymentDefinition.getDest().getZone().getId());
                final VMTemplateVO template = _templateDao.findRoutingTemplate(hType, templateName);

                if (template == null) {
                    logger.debug(hType + " won't support system vm, skip it");
                    continue;
                }

                final boolean offerHA = routerOffering.getOfferHA();

                // routerDeploymentDefinition.getVpc().getId() ==> do not use
                // VPC because it is not a VPC offering.
                final Long vpcId = routerDeploymentDefinition.getVpc() != null ? routerDeploymentDefinition.getVpc().getId() : null;

                long userId = CallContext.current().getCallingUserId();
                if (CallContext.current().getCallingAccount().getId() != owner.getId()) {
                    final List<UserVO> userVOs = _userDao.listByAccount(owner.getAccountId());
                    if (!userVOs.isEmpty()) {
                        userId = userVOs.get(0).getId();
                    }
                }

                router = new DomainRouterVO(id, routerOffering.getId(), routerDeploymentDefinition.getVirtualProvider().getId(), VirtualMachineName.getRouterName(id,
                        s_vmInstanceName), template.getId(), template.getHypervisorType(), template.getGuestOSId(), owner.getDomainId(), owner.getId(),
                        userId, routerDeploymentDefinition.isRedundant(), RedundantState.UNKNOWN, offerHA, false, vpcId,
                        template.getOptimiseFor(), template.getManufacturerString(), template.getCpuFlags(), template.getMacLearning(), false, template.getMaintenancePolicy(), routerUnicastId);

                router.setDynamicallyScalable(template.isDynamicallyScalable());
                router.setRole(Role.VIRTUAL_ROUTER);
                router = _routerDao.persist(router);

                reallocateRouterNetworks(routerDeploymentDefinition, router, template, null);
                router = _routerDao.findById(router.getId());
            } catch (final InsufficientCapacityException ex) {
                if (allocateRetry < 2 && iter.hasNext()) {
                    logger.debug("Failed to allocate the VR with hypervisor type " + hType + ", retrying one more time");
                    continue;
                } else {
                    throw ex;
                }
            } finally {
                allocateRetry++;
            }

            if (startRouter) {
                try {
                    final Account caller = CallContext.current().getCallingAccount();
                    final User callerUser = _accountMgr.getActiveUser(CallContext.current().getCallingUserId());
                    router = startVirtualRouter(router, callerUser, caller, routerDeploymentDefinition.getParams());
                    break;
                } catch (final InsufficientCapacityException ex) {
                    if (startRetry < 2 && iter.hasNext()) {
                        logger.debug("Failed to start the VR  " + router + " with hypervisor type " + hType + ", " + "destroying it and recreating one more time");
                        // destroy the router
                        destroyRouter(router.getId(), _accountMgr.getAccount(Account.ACCOUNT_ID_SYSTEM), User.UID_SYSTEM);
                        continue;
                    } else {
                        throw ex;
                    }
                } finally {
                    startRetry++;
                }
            } else {
                // return stopped router
                return router;
            }
        }

        return router;
    }

    protected List<HypervisorType> getHypervisors(final RouterDeploymentDefinition routerDeploymentDefinition) throws InsufficientServerCapacityException {
        final DeployDestination dest = routerDeploymentDefinition.getDest();
        List<HypervisorType> hypervisors = new ArrayList<>();

        if (dest.getCluster() != null) {
            hypervisors.add(dest.getCluster().getHypervisorType());
        } else {
            final HypervisorType defaults = _resourceMgr.getDefaultHypervisor(dest.getZone().getId());
            if (defaults != HypervisorType.None) {
                hypervisors.add(defaults);
            } else {
                // if there is no default hypervisor, get it from the cluster
                hypervisors = _resourceMgr.getSupportedHypervisorTypes(dest.getZone().getId(), true, routerDeploymentDefinition.getPlan().getPodId());
            }
        }

        filterSupportedHypervisors(hypervisors);

        if (hypervisors.isEmpty()) {
            if (routerDeploymentDefinition.getPodId() != null) {
                throw new InsufficientServerCapacityException("Unable to create virtual router, there are no clusters in the pod." + getNoHypervisorsErrMsgDetails(), Pod.class,
                        routerDeploymentDefinition.getPodId());
            }
            throw new InsufficientServerCapacityException("Unable to create virtual router, there are no clusters in the zone." + getNoHypervisorsErrMsgDetails(),
                    DataCenter.class, dest.getZone().getId());
        }
        return hypervisors;
    }

    protected String retrieveTemplateName(final HypervisorType hType, final long datacenterId) {
        String templateName = null;

        final ConfigKey<String> hypervisorConfigKey = hypervisorsMap.get(hType);

        if (hypervisorConfigKey != null) {
            templateName = hypervisorConfigKey.valueIn(datacenterId);
        }

        return templateName;
    }

    protected void filterSupportedHypervisors(final List<HypervisorType> hypervisors) {
        // For non vpc we keep them all assuming all types in the list are
        // supported
    }

    protected String getNoHypervisorsErrMsgDetails() {
        return "";
    }

    protected LinkedHashMap<Network, List<? extends NicProfile>> configureControlNic(final RouterDeploymentDefinition routerDeploymentDefinition) {
        final LinkedHashMap<Network, List<? extends NicProfile>> controlConfig = new LinkedHashMap<>(3);

        logger.debug("Adding nic for Virtual Router in Control network ");
        final List<? extends NetworkOffering> offerings = _networkModel.getSystemAccountNetworkOfferings(NetworkOffering.SystemControlNetwork);
        final NetworkOffering controlOffering = offerings.get(0);
        final Network controlNic = _networkMgr.setupNetwork(s_systemAccount, controlOffering, routerDeploymentDefinition.getPlan(), null, null, false).get(0);

        controlConfig.put(controlNic, new ArrayList<>());

        return controlConfig;
    }

    protected LinkedHashMap<Network, List<? extends NicProfile>> configureSyncNic(final RouterDeploymentDefinition routerDeploymentDefinition) {
        final LinkedHashMap<Network, List<? extends NicProfile>> syncConfig = new LinkedHashMap<>(3);

        logger.debug("Adding nic for Virtual Router in Sync network ");

        final Network syncNic = _networkMgr.setupSyncNetwork(
                routerDeploymentDefinition.getOwner(),
                routerDeploymentDefinition.getPlan(),
                routerDeploymentDefinition.isVpcRouter(),
                routerDeploymentDefinition.getVpc(),
                routerDeploymentDefinition.getGuestNetwork()
        );

        syncConfig.put(syncNic, new ArrayList<>());

        return syncConfig;
    }

    protected LinkedHashMap<Network, List<? extends NicProfile>> configurePublicNic(final RouterDeploymentDefinition routerDeploymentDefinition, final boolean hasGuestNic)
            throws InsufficientAddressCapacityException {
        final LinkedHashMap<Network, List<? extends NicProfile>> publicConfig = new LinkedHashMap<>(3);

        if (routerDeploymentDefinition.isPublicNetwork()) {
            logger.debug("Adding nic for Virtual Router in Public network ");
            // if source nat service is supported by the network, get the source
            // nat ip address
            final NicProfile defaultNic = new NicProfile();
            defaultNic.setDefaultNic(true);
            final PublicIp sourceNatIp = routerDeploymentDefinition.getSourceNatIP();
            defaultNic.setIPv4Address(sourceNatIp.getAddress().addr());
            defaultNic.setIPv4Gateway(sourceNatIp.getGateway());
            defaultNic.setIPv4Netmask(sourceNatIp.getNetmask());
            defaultNic.setMacAddress(_networkModel.getNextAvailableMacAddressInNetwork(sourceNatIp.getNetworkId()));
            // get broadcast from public network
            final Network pubNet = _networkDao.findById(sourceNatIp.getNetworkId());
            if (pubNet.getBroadcastDomainType() == BroadcastDomainType.Vxlan) {
                defaultNic.setBroadcastType(BroadcastDomainType.Vxlan);
                defaultNic.setBroadcastUri(BroadcastDomainType.Vxlan.toUri(sourceNatIp.getVlanTag()));
                defaultNic.setIsolationUri(BroadcastDomainType.Vxlan.toUri(sourceNatIp.getVlanTag()));
            } else {
                defaultNic.setBroadcastType(BroadcastDomainType.Vlan);
                defaultNic.setBroadcastUri(BroadcastDomainType.Vlan.toUri(sourceNatIp.getVlanTag()));
                defaultNic.setIsolationUri(IsolationType.Vlan.toUri(sourceNatIp.getVlanTag()));
            }

            final NetworkOffering publicOffering = _networkModel.getSystemAccountNetworkOfferings(NetworkOffering.SystemPublicNetwork).get(0);
            final List<? extends Network> publicNetworks = _networkMgr.setupNetwork(s_systemAccount, publicOffering, routerDeploymentDefinition.getPlan(), null, null, false);
            publicConfig.put(publicNetworks.get(0), new ArrayList<>(Arrays.asList(defaultNic)));
        }

        return publicConfig;
    }

    @Override
    public void reallocateRouterNetworks(final RouterDeploymentDefinition routerDeploymentDefinition, final VirtualRouter router, final VMTemplateVO template, final
    HypervisorType hType)
            throws ConcurrentOperationException, InsufficientCapacityException {
        final ServiceOfferingVO routerOffering = _serviceOfferingDao.findById(routerDeploymentDefinition.getServiceOfferingId());

        final LinkedHashMap<Network, List<? extends NicProfile>> networks = configureDefaultNics(routerDeploymentDefinition);

        _itMgr.allocate(router.getInstanceName(), template, routerOffering, networks, routerDeploymentDefinition.getPlan(), hType);
    }

    @Override
    public LinkedHashMap<Network, List<? extends NicProfile>> configureDefaultNics(final RouterDeploymentDefinition routerDeploymentDefinition) throws
            ConcurrentOperationException, InsufficientAddressCapacityException {

        final LinkedHashMap<Network, List<? extends NicProfile>> networks = new LinkedHashMap<>(3);

        // 1) Control network (was 2)
        final LinkedHashMap<Network, List<? extends NicProfile>> controlNic = configureControlNic(routerDeploymentDefinition);
        networks.putAll(controlNic);

        // 2) Sync network (was 4)
        final LinkedHashMap<Network, List<? extends NicProfile>> syncNic = configureSyncNic(routerDeploymentDefinition);
        networks.putAll(syncNic);

        // 3) Public network (was 3)
        final LinkedHashMap<Network, List<? extends NicProfile>> publicNic = configurePublicNic(routerDeploymentDefinition, networks.size() > 1);
        networks.putAll(publicNic);

        // 4) Guest Network (was 1)
        final LinkedHashMap<Network, List<? extends NicProfile>> guestNic = configureGuestNic(routerDeploymentDefinition);
        networks.putAll(guestNic);

        return networks;
    }

    @Override
    public LinkedHashMap<Network, List<? extends NicProfile>> configureGuestNic(final RouterDeploymentDefinition routerDeploymentDefinition)
            throws ConcurrentOperationException, InsufficientAddressCapacityException {

        // Form networks
        final LinkedHashMap<Network, List<? extends NicProfile>> networks = new LinkedHashMap<>(3);
        // 1) Guest network
        final Network guestNetwork = routerDeploymentDefinition.getGuestNetwork();

        if (guestNetwork != null) {
            logger.debug("Adding nic for Virtual Router in Guest network " + guestNetwork);
            String defaultNetworkStartIp = null, defaultNetworkStartIpv6 = null;
            if (!routerDeploymentDefinition.isPublicNetwork()) {
                final Nic placeholder = _networkModel.getPlaceholderNicForRouter(guestNetwork, routerDeploymentDefinition.getPodId());
                if (guestNetwork.getCidr() != null) {
                    if (placeholder != null && placeholder.getIPv4Address() != null) {
                        logger.debug("Requesting ipv4 address " + placeholder.getIPv4Address() + " stored in placeholder nic for the network "
                                + guestNetwork);
                        defaultNetworkStartIp = placeholder.getIPv4Address();
                    } else {
                        final String startIp = _networkModel.getStartIpAddress(guestNetwork.getId());
                        if (startIp != null
                                && _ipAddressDao.findByIpAndSourceNetworkId(guestNetwork.getId(), startIp).getAllocatedTime() == null) {
                            defaultNetworkStartIp = startIp;
                        } else if (logger.isDebugEnabled()) {
                            logger.debug("First ipv4 " + startIp + " in network id=" + guestNetwork.getId()
                                    + " is already allocated, can't use it for domain router; will get random ip address from the range");
                        }
                    }
                }

                if (guestNetwork.getIp6Cidr() != null) {
                    if (placeholder != null && placeholder.getIPv6Address() != null) {
                        logger.debug("Requesting ipv6 address " + placeholder.getIPv6Address() + " stored in placeholder nic for the network "
                                + guestNetwork);
                        defaultNetworkStartIpv6 = placeholder.getIPv6Address();
                    } else {
                        final String startIpv6 = _networkModel.getStartIpv6Address(guestNetwork.getId());
                        if (startIpv6 != null && _ipv6Dao.findByNetworkIdAndIp(guestNetwork.getId(), startIpv6) == null) {
                            defaultNetworkStartIpv6 = startIpv6;
                        } else if (logger.isDebugEnabled()) {
                            logger.debug("First ipv6 " + startIpv6 + " in network id=" + guestNetwork.getId()
                                    + " is already allocated, can't use it for domain router; will get random ipv6 address from the range");
                        }
                    }
                }
            }

            final NicProfile gatewayNic = new NicProfile(defaultNetworkStartIp, defaultNetworkStartIpv6);
            if (routerDeploymentDefinition.isPublicNetwork()) {
                if (routerDeploymentDefinition.isRedundant()) {
                    gatewayNic.setIPv4Address(_ipAddrMgr.acquireGuestIpAddressForRouter(guestNetwork, null));
                } else {
                    gatewayNic.setIPv4Address(guestNetwork.getGateway());
                }
                gatewayNic.setBroadcastUri(guestNetwork.getBroadcastUri());
                gatewayNic.setBroadcastType(guestNetwork.getBroadcastDomainType());
                gatewayNic.setIsolationUri(guestNetwork.getBroadcastUri());
                gatewayNic.setMode(guestNetwork.getMode());
                final String gatewayCidr = guestNetwork.getCidr();
                gatewayNic.setIPv4Netmask(NetUtils.getCidrNetmask(gatewayCidr));
            } else {
                gatewayNic.setDefaultNic(true);
            }

            networks.put(guestNetwork, new ArrayList<>(Arrays.asList(gatewayNic)));
        }
        return networks;
    }
}
