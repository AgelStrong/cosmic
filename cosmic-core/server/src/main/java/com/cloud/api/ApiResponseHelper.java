package com.cloud.api;

import com.cloud.affinity.AffinityGroup;
import com.cloud.affinity.AffinityGroupResponse;
import com.cloud.api.ApiConstants.HostDetails;
import com.cloud.api.ApiConstants.VMDetails;
import com.cloud.api.ResponseObject.ResponseView;
import com.cloud.api.command.user.job.QueryAsyncJobResultCmd;
import com.cloud.api.query.ViewResponseHelper;
import com.cloud.api.query.vo.AccountJoinVO;
import com.cloud.api.query.vo.AsyncJobJoinVO;
import com.cloud.api.query.vo.ControlledViewEntity;
import com.cloud.api.query.vo.DataCenterJoinVO;
import com.cloud.api.query.vo.DiskOfferingJoinVO;
import com.cloud.api.query.vo.DomainRouterJoinVO;
import com.cloud.api.query.vo.HostJoinVO;
import com.cloud.api.query.vo.ImageStoreJoinVO;
import com.cloud.api.query.vo.InstanceGroupJoinVO;
import com.cloud.api.query.vo.ProjectJoinVO;
import com.cloud.api.query.vo.ResourceTagJoinVO;
import com.cloud.api.query.vo.ServiceOfferingJoinVO;
import com.cloud.api.query.vo.StoragePoolJoinVO;
import com.cloud.api.query.vo.TemplateJoinVO;
import com.cloud.api.query.vo.UserAccountJoinVO;
import com.cloud.api.query.vo.UserVmJoinVO;
import com.cloud.api.query.vo.VolumeJoinVO;
import com.cloud.api.response.AccountResponse;
import com.cloud.api.response.ApiResponseSerializer;
import com.cloud.api.response.AsyncJobResponse;
import com.cloud.api.response.CapabilityResponse;
import com.cloud.api.response.CapacityResponse;
import com.cloud.api.response.ClusterResponse;
import com.cloud.api.response.ConfigurationResponse;
import com.cloud.api.response.ControlledEntityResponse;
import com.cloud.api.response.ControlledViewEntityResponse;
import com.cloud.api.response.CreateCmdResponse;
import com.cloud.api.response.CreateSSHKeyPairResponse;
import com.cloud.api.response.DiskOfferingResponse;
import com.cloud.api.response.DomainResponse;
import com.cloud.api.response.DomainRouterResponse;
import com.cloud.api.response.ExtractResponse;
import com.cloud.api.response.FirewallResponse;
import com.cloud.api.response.FirewallRuleResponse;
import com.cloud.api.response.GuestOSResponse;
import com.cloud.api.response.GuestOsMappingResponse;
import com.cloud.api.response.GuestVlanRangeResponse;
import com.cloud.api.response.HostForMigrationResponse;
import com.cloud.api.response.HostResponse;
import com.cloud.api.response.HypervisorCapabilitiesResponse;
import com.cloud.api.response.IPAddressResponse;
import com.cloud.api.response.ImageStoreResponse;
import com.cloud.api.response.InstanceGroupResponse;
import com.cloud.api.response.IpForwardingRuleResponse;
import com.cloud.api.response.IsolationMethodResponse;
import com.cloud.api.response.LBHealthCheckPolicyResponse;
import com.cloud.api.response.LBHealthCheckResponse;
import com.cloud.api.response.LBStickinessPolicyResponse;
import com.cloud.api.response.LBStickinessResponse;
import com.cloud.api.response.ListResponse;
import com.cloud.api.response.LoadBalancerResponse;
import com.cloud.api.response.NetworkACLItemResponse;
import com.cloud.api.response.NetworkACLResponse;
import com.cloud.api.response.NetworkOfferingResponse;
import com.cloud.api.response.NetworkResponse;
import com.cloud.api.response.NicResponse;
import com.cloud.api.response.NicSecondaryIpResponse;
import com.cloud.api.response.PhysicalNetworkResponse;
import com.cloud.api.response.PodResponse;
import com.cloud.api.response.PrivateGatewayResponse;
import com.cloud.api.response.ProjectResponse;
import com.cloud.api.response.ProviderResponse;
import com.cloud.api.response.RegionResponse;
import com.cloud.api.response.RemoteAccessVpnResponse;
import com.cloud.api.response.ResourceCountResponse;
import com.cloud.api.response.ResourceLimitResponse;
import com.cloud.api.response.ResourceTagResponse;
import com.cloud.api.response.SSHKeyPairResponse;
import com.cloud.api.response.ServiceOfferingResponse;
import com.cloud.api.response.ServiceResponse;
import com.cloud.api.response.Site2SiteCustomerGatewayResponse;
import com.cloud.api.response.Site2SiteVpnConnectionResponse;
import com.cloud.api.response.Site2SiteVpnGatewayResponse;
import com.cloud.api.response.SnapshotResponse;
import com.cloud.api.response.StaticRouteResponse;
import com.cloud.api.response.StorageNetworkIpRangeResponse;
import com.cloud.api.response.StoragePoolResponse;
import com.cloud.api.response.SystemVmResponse;
import com.cloud.api.response.TemplatePermissionsResponse;
import com.cloud.api.response.TemplateResponse;
import com.cloud.api.response.TrafficTypeResponse;
import com.cloud.api.response.UpgradeRouterTemplateResponse;
import com.cloud.api.response.UserResponse;
import com.cloud.api.response.UserVmResponse;
import com.cloud.api.response.VMSnapshotResponse;
import com.cloud.api.response.VirtualRouterProviderResponse;
import com.cloud.api.response.VlanIpRangeResponse;
import com.cloud.api.response.VolumeResponse;
import com.cloud.api.response.VpcOfferingResponse;
import com.cloud.api.response.VpcResponse;
import com.cloud.api.response.VpnUsersResponse;
import com.cloud.api.response.ZoneResponse;
import com.cloud.capacity.Capacity;
import com.cloud.capacity.CapacityVO;
import com.cloud.capacity.dao.CapacityDaoImpl.SummedCapacity;
import com.cloud.config.Configuration;
import com.cloud.configuration.ConfigurationManager;
import com.cloud.context.CallContext;
import com.cloud.dao.EntityManager;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.HostPodVO;
import com.cloud.dc.VlanVO;
import com.cloud.engine.subsystem.api.storage.DataStore;
import com.cloud.engine.subsystem.api.storage.DataStoreCapabilities;
import com.cloud.engine.subsystem.api.storage.DataStoreManager;
import com.cloud.engine.subsystem.api.storage.SnapshotDataFactory;
import com.cloud.engine.subsystem.api.storage.SnapshotInfo;
import com.cloud.framework.jobs.AsyncJob;
import com.cloud.framework.jobs.AsyncJobManager;
import com.cloud.gpu.GPU;
import com.cloud.hypervisor.HypervisorCapabilities;
import com.cloud.legacymodel.acl.ControlledEntity;
import com.cloud.legacymodel.acl.ControlledEntity.ACLType;
import com.cloud.legacymodel.configuration.Resource.ResourceOwnerType;
import com.cloud.legacymodel.configuration.Resource.ResourceType;
import com.cloud.legacymodel.configuration.ResourceCount;
import com.cloud.legacymodel.configuration.ResourceLimit;
import com.cloud.legacymodel.dc.Cluster;
import com.cloud.legacymodel.dc.DataCenter;
import com.cloud.legacymodel.dc.Host;
import com.cloud.legacymodel.dc.Pod;
import com.cloud.legacymodel.dc.StorageNetworkIpRange;
import com.cloud.legacymodel.dc.Vlan;
import com.cloud.legacymodel.dc.Vlan.VlanType;
import com.cloud.legacymodel.domain.Domain;
import com.cloud.legacymodel.exceptions.CloudRuntimeException;
import com.cloud.legacymodel.exceptions.InvalidParameterValueException;
import com.cloud.legacymodel.exceptions.PermissionDeniedException;
import com.cloud.legacymodel.network.FirewallRule;
import com.cloud.legacymodel.network.LoadBalancer;
import com.cloud.legacymodel.network.Network;
import com.cloud.legacymodel.network.Network.Capability;
import com.cloud.legacymodel.network.Network.Provider;
import com.cloud.legacymodel.network.Network.Service;
import com.cloud.legacymodel.network.Nic;
import com.cloud.legacymodel.network.PortForwardingRule;
import com.cloud.legacymodel.network.StaticNatRule;
import com.cloud.legacymodel.network.VirtualRouter;
import com.cloud.legacymodel.network.VpnUser;
import com.cloud.legacymodel.network.vpc.NetworkACL;
import com.cloud.legacymodel.network.vpc.NetworkACLItem;
import com.cloud.legacymodel.network.vpc.PrivateGateway;
import com.cloud.legacymodel.network.vpc.StaticRoute;
import com.cloud.legacymodel.network.vpc.Vpc;
import com.cloud.legacymodel.network.vpc.VpcOffering;
import com.cloud.legacymodel.storage.DiskOffering;
import com.cloud.legacymodel.storage.StoragePool;
import com.cloud.legacymodel.storage.UploadStatus;
import com.cloud.legacymodel.storage.VMSnapshot;
import com.cloud.legacymodel.storage.VirtualMachineTemplate;
import com.cloud.legacymodel.storage.Volume;
import com.cloud.legacymodel.user.Account;
import com.cloud.legacymodel.user.SSHKeyPair;
import com.cloud.legacymodel.user.User;
import com.cloud.legacymodel.user.UserAccount;
import com.cloud.legacymodel.utils.Pair;
import com.cloud.legacymodel.vm.VgpuTypesInfo;
import com.cloud.legacymodel.vm.VirtualMachine;
import com.cloud.model.enumeration.BroadcastDomainType;
import com.cloud.model.enumeration.DataStoreRole;
import com.cloud.model.enumeration.TrafficType;
import com.cloud.model.enumeration.VirtualMachineType;
import com.cloud.network.GuestVlan;
import com.cloud.network.IpAddress;
import com.cloud.network.NetworkModel;
import com.cloud.network.NetworkProfile;
import com.cloud.network.Networks.IsolationType;
import com.cloud.network.PhysicalNetwork;
import com.cloud.network.PhysicalNetworkServiceProvider;
import com.cloud.network.PhysicalNetworkTrafficType;
import com.cloud.network.RemoteAccessVpn;
import com.cloud.network.Site2SiteCustomerGateway;
import com.cloud.network.Site2SiteVpnConnection;
import com.cloud.network.Site2SiteVpnGateway;
import com.cloud.network.VirtualRouterProvider;
import com.cloud.network.dao.IPAddressVO;
import com.cloud.network.dao.NetworkVO;
import com.cloud.network.dao.PhysicalNetworkVO;
import com.cloud.network.rules.HealthCheckPolicy;
import com.cloud.network.rules.StickinessPolicy;
import com.cloud.offering.NetworkOffering;
import com.cloud.offering.NetworkOffering.Detail;
import com.cloud.offering.ServiceOffering;
import com.cloud.projects.Project;
import com.cloud.region.Region;
import com.cloud.server.ResourceTag;
import com.cloud.server.ResourceTag.ResourceObjectType;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.GuestOS;
import com.cloud.storage.GuestOSCategoryVO;
import com.cloud.storage.GuestOSHypervisor;
import com.cloud.storage.ImageStore;
import com.cloud.storage.Snapshot;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.datastore.db.PrimaryDataStoreDao;
import com.cloud.storage.datastore.db.SnapshotDataStoreDao;
import com.cloud.storage.datastore.db.SnapshotDataStoreVO;
import com.cloud.storage.datastore.db.StoragePoolVO;
import com.cloud.user.AccountManager;
import com.cloud.uservm.UserVm;
import com.cloud.utils.StringUtils;
import com.cloud.utils.net.NetUtils;
import com.cloud.vm.ConsoleProxyVO;
import com.cloud.vm.InstanceGroup;
import com.cloud.vm.NicProfile;
import com.cloud.vm.NicSecondaryIp;
import com.cloud.vm.NicVO;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.NicSecondaryIpVO;

import javax.inject.Inject;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiResponseHelper implements ResponseGenerator {

    private static final Logger s_logger = LoggerFactory.getLogger(ApiResponseHelper.class);
    private static final DecimalFormat s_percentFormat = new DecimalFormat("##.##");
    @Inject
    protected AccountManager _accountMgr;
    @Inject
    protected AsyncJobManager _jobMgr;
    @Inject
    NetworkModel _ntwkModel;
    @Inject
    ConfigurationManager _configMgr;
    @Inject
    SnapshotDataFactory snapshotfactory;
    @Inject
    private EntityManager _entityMgr;
    @Inject
    private VolumeDao _volumeDao;
    @Inject
    private DataStoreManager _dataStoreMgr;
    @Inject
    ServiceOfferingDao _serviceOfferingDao;
    @Inject
    private SnapshotDataStoreDao _snapshotStoreDao;
    @Inject
    private PrimaryDataStoreDao _storagePoolDao;

    public static List<CapacityResponse> getDataCenterCapacityResponse(final Long zoneId) {
        final List<SummedCapacity> capacities = ApiDBUtils.getCapacityByClusterPodZone(zoneId, null, null);
        final Set<CapacityResponse> capacityResponses = new HashSet<>();

        for (final SummedCapacity capacity : capacities) {
            final CapacityResponse capacityResponse = new CapacityResponse();
            capacityResponse.setCapacityType(capacity.getCapacityType());
            capacityResponse.setCapacityUsed(capacity.getUsedCapacity() + capacity.getReservedCapacity());
            if (capacity.getCapacityType() == Capacity.CAPACITY_TYPE_STORAGE_ALLOCATED) {
                final List<SummedCapacity> c = ApiDBUtils.findNonSharedStorageForClusterPodZone(zoneId, null, null);
                capacityResponse.setCapacityTotal(capacity.getTotalCapacity() - c.get(0).getTotalCapacity());
                capacityResponse.setCapacityUsed(capacity.getUsedCapacity() - c.get(0).getUsedCapacity());
            } else {
                capacityResponse.setCapacityTotal(capacity.getTotalCapacity());
            }
            if (capacityResponse.getCapacityTotal() != 0) {
                capacityResponse.setPercentageAllocated(s_percentFormat.format((float) capacityResponse.getCapacityUsed() / (float) capacityResponse.getCapacityTotal() * 100f));
            } else {
                capacityResponse.setPercentageAllocated(s_percentFormat.format(0L));
            }
            capacityResponses.add(capacityResponse);
        }
        // Do it for stats as well.
        capacityResponses.addAll(getStatsCapacityresponse(null, null, null, zoneId));

        return new ArrayList<>(capacityResponses);
    }

    private static List<CapacityResponse> getStatsCapacityresponse(final Long poolId, final Long clusterId, final Long podId, final Long zoneId) {
        final List<CapacityVO> capacities = new ArrayList<>();
        capacities.add(ApiDBUtils.getStoragePoolUsedStats(poolId, clusterId, podId, zoneId));
        if (clusterId == null && podId == null) {
            capacities.add(ApiDBUtils.getSecondaryStorageUsedStats(poolId, zoneId));
        }

        final List<CapacityResponse> capacityResponses = new ArrayList<>();
        for (final CapacityVO capacity : capacities) {
            final CapacityResponse capacityResponse = new CapacityResponse();
            capacityResponse.setCapacityType(capacity.getCapacityType());
            capacityResponse.setCapacityUsed(capacity.getUsedCapacity());
            capacityResponse.setCapacityTotal(capacity.getTotalCapacity());
            if (capacityResponse.getCapacityTotal() != 0) {
                capacityResponse.setPercentageAllocated(s_percentFormat.format((float) capacityResponse.getCapacityUsed() / (float) capacityResponse.getCapacityTotal() * 100f));
            } else {
                capacityResponse.setPercentageAllocated(s_percentFormat.format(0L));
            }
            capacityResponses.add(capacityResponse);
        }

        return capacityResponses;
    }

    public static void populateOwner(final ControlledViewEntityResponse response, final ControlledViewEntity object) {

        if (object.getAccountType() == Account.ACCOUNT_TYPE_PROJECT) {
            response.setProjectId(object.getProjectUuid());
            response.setProjectName(object.getProjectName());
        } else {
            response.setAccountName(object.getAccountName());
        }

        response.setDomainId(object.getDomainUuid());
        response.setDomainName(object.getDomainName());
    }

    @Override
    public UserResponse createUserResponse(final UserAccount user) {
        final UserAccountJoinVO vUser = ApiDBUtils.newUserView(user);
        return ApiDBUtils.newUserResponse(vUser);
    }

    @Override
    public AccountResponse createAccountResponse(final ResponseView view, final Account account) {
        final AccountJoinVO vUser = ApiDBUtils.newAccountView(account);
        return ApiDBUtils.newAccountResponse(view, vUser);
    }

    @Override
    public DomainResponse createDomainResponse(final Domain domain) {
        final DomainResponse domainResponse = new DomainResponse();
        domainResponse.setDomainName(domain.getName());
        domainResponse.setId(domain.getUuid());
        domainResponse.setLevel(domain.getLevel());
        domainResponse.setNetworkDomain(domain.getNetworkDomain());
        domainResponse.setEmail(domain.getEmail());
        domainResponse.setSlackChannelName(domain.getSlackChannelName());
        final Domain parentDomain = ApiDBUtils.findDomainById(domain.getParent());
        if (parentDomain != null) {
            domainResponse.setParentDomainId(parentDomain.getUuid());
        }
        final StringBuilder domainPath = new StringBuilder("ROOT");
        domainPath.append(domain.getPath()).deleteCharAt(domainPath.length() - 1);
        domainResponse.setPath(domainPath.toString());
        if (domain.getParent() != null) {
            domainResponse.setParentDomainName(ApiDBUtils.findDomainById(domain.getParent()).getName());
        }
        if (domain.getChildCount() > 0) {
            domainResponse.setHasChild(true);
        }
        domainResponse.setObjectName("domain");
        return domainResponse;
    }

    @Override
    public DiskOfferingResponse createDiskOfferingResponse(final DiskOffering offering) {
        final DiskOfferingJoinVO vOffering = ApiDBUtils.newDiskOfferingView(offering);
        return ApiDBUtils.newDiskOfferingResponse(vOffering);
    }

    @Override
    public ResourceLimitResponse createResourceLimitResponse(final ResourceLimit limit) {
        final ResourceLimitResponse resourceLimitResponse = new ResourceLimitResponse();
        if (limit.getResourceOwnerType() == ResourceOwnerType.Domain) {
            populateDomain(resourceLimitResponse, limit.getOwnerId());
        } else if (limit.getResourceOwnerType() == ResourceOwnerType.Account) {
            final Account accountTemp = ApiDBUtils.findAccountById(limit.getOwnerId());
            populateAccount(resourceLimitResponse, limit.getOwnerId());
            populateDomain(resourceLimitResponse, accountTemp.getDomainId());
        }
        resourceLimitResponse.setResourceType(Integer.toString(limit.getType().getOrdinal()));

        if ((limit.getType() == ResourceType.primary_storage || limit.getType() == ResourceType.secondary_storage) && limit.getMax() >= 0) {
            resourceLimitResponse.setMax((long) Math.ceil((double) limit.getMax() / ResourceType.bytesToGiB));
        } else {
            resourceLimitResponse.setMax(limit.getMax());
        }
        resourceLimitResponse.setObjectName("resourcelimit");

        return resourceLimitResponse;
    }

    @Override
    public ResourceCountResponse createResourceCountResponse(final ResourceCount resourceCount) {
        final ResourceCountResponse resourceCountResponse = new ResourceCountResponse();

        if (resourceCount.getResourceOwnerType() == ResourceOwnerType.Account) {
            final Account accountTemp = ApiDBUtils.findAccountById(resourceCount.getOwnerId());
            if (accountTemp != null) {
                populateAccount(resourceCountResponse, accountTemp.getId());
                populateDomain(resourceCountResponse, accountTemp.getDomainId());
            }
        } else if (resourceCount.getResourceOwnerType() == ResourceOwnerType.Domain) {
            populateDomain(resourceCountResponse, resourceCount.getOwnerId());
        }

        resourceCountResponse.setResourceType(Integer.toString(resourceCount.getType().getOrdinal()));
        resourceCountResponse.setResourceCount(resourceCount.getCount());
        resourceCountResponse.setObjectName("resourcecount");
        return resourceCountResponse;
    }

    @Override
    public ServiceOfferingResponse createServiceOfferingResponse(final ServiceOffering offering) {
        final ServiceOfferingJoinVO vOffering = ApiDBUtils.newServiceOfferingView(offering);
        return ApiDBUtils.newServiceOfferingResponse(vOffering);
    }

    @Override
    public ConfigurationResponse createConfigurationResponse(final Configuration cfg) {
        final ConfigurationResponse cfgResponse = new ConfigurationResponse();
        cfgResponse.setCategory(cfg.getCategory());
        cfgResponse.setDescription(cfg.getDescription());
        cfgResponse.setName(cfg.getName());
        cfgResponse.setValue(cfg.getValue());
        cfgResponse.setObjectName("configuration");

        return cfgResponse;
    }

    @Override
    public SnapshotResponse createSnapshotResponse(final Snapshot snapshot) {
        final SnapshotResponse snapshotResponse = new SnapshotResponse();
        snapshotResponse.setId(snapshot.getUuid());

        populateOwner(snapshotResponse, snapshot);

        final VolumeVO volume = findVolumeById(snapshot.getVolumeId());
        final String snapshotTypeStr = snapshot.getRecurringType().name();
        snapshotResponse.setSnapshotType(snapshotTypeStr);
        if (volume != null) {
            snapshotResponse.setVolumeId(volume.getUuid());
            snapshotResponse.setVolumeName(volume.getName());
            snapshotResponse.setVolumeType(volume.getVolumeType().name());
            final DataCenter zone = ApiDBUtils.findZoneById(volume.getDataCenterId());
            if (zone != null) {
                snapshotResponse.setZoneId(zone.getUuid());
            }
        }
        snapshotResponse.setCreated(snapshot.getCreated());
        snapshotResponse.setName(snapshot.getName());
        snapshotResponse.setIntervalType(ApiDBUtils.getSnapshotIntervalTypes(snapshot.getId()));
        snapshotResponse.setState(snapshot.getState());

        final SnapshotInfo snapshotInfo;

        if (snapshot instanceof SnapshotInfo) {
            snapshotInfo = (SnapshotInfo) snapshot;
        } else {
            final DataStoreRole dataStoreRole = getDataStoreRole(snapshot, this._snapshotStoreDao, this._dataStoreMgr);

            snapshotInfo = this.snapshotfactory.getSnapshot(snapshot.getId(), dataStoreRole);
        }

        if (snapshotInfo == null) {
            s_logger.debug("Unable to find info for image store snapshot with uuid " + snapshot.getUuid());
            snapshotResponse.setRevertable(false);
        } else {
            snapshotResponse.setRevertable(snapshotInfo.isRevertable());
            snapshotResponse.setPhysicaSize(snapshotInfo.getPhysicalSize());
        }

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.Snapshot, snapshot.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);

            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        snapshotResponse.setTags(tagResponses);

        snapshotResponse.setObjectName("snapshot");
        return snapshotResponse;
    }

    @Override
    public List<UserVmResponse> createUserVmResponse(final ResponseView view, final String objectName, final UserVm... userVms) {
        final List<UserVmJoinVO> viewVms = ApiDBUtils.newUserVmView(userVms);
        return ViewResponseHelper.createUserVmResponse(view, objectName, viewVms.toArray(new UserVmJoinVO[viewVms.size()]));
    }

    @Override
    public List<UserVmResponse> createUserVmResponse(final ResponseView view, final String objectName, final EnumSet<VMDetails> details, final UserVm... userVms) {
        final List<UserVmJoinVO> viewVms = ApiDBUtils.newUserVmView(userVms);
        return ViewResponseHelper.createUserVmResponse(view, objectName, details, viewVms.toArray(new UserVmJoinVO[viewVms.size()]));
    }

    @Override
    public SystemVmResponse createSystemVmResponse(final VirtualMachine vm) {
        final SystemVmResponse vmResponse = new SystemVmResponse();
        if (vm.getType() == VirtualMachineType.SecondaryStorageVm || vm.getType() == VirtualMachineType.ConsoleProxy || vm.getType() == VirtualMachineType.DomainRouter) {
            // SystemVm vm = (SystemVm) systemVM;
            vmResponse.setId(vm.getUuid());
            // vmResponse.setObjectId(vm.getId());
            vmResponse.setSystemVmType(vm.getType().toString().toLowerCase());

            vmResponse.setName(vm.getHostName());
            if (vm.getPodIdToDeployIn() != null) {
                final HostPodVO pod = ApiDBUtils.findPodById(vm.getPodIdToDeployIn());
                if (pod != null) {
                    vmResponse.setPodId(pod.getUuid());
                }
            }
            final VMTemplateVO template = ApiDBUtils.findTemplateById(vm.getTemplateId());
            if (template != null) {
                vmResponse.setTemplateId(template.getUuid());
            }
            vmResponse.setCreated(vm.getCreated());

            if (vm.getHostId() != null) {
                final Host host = ApiDBUtils.findHostById(vm.getHostId());
                if (host != null) {
                    vmResponse.setHostId(host.getUuid());
                    vmResponse.setHostName(host.getName());
                    vmResponse.setHypervisor(host.getHypervisorType().toString());
                }
            }

            if (vm.getState() != null) {
                vmResponse.setState(vm.getState().toString());
            }

            // for console proxies, add the active sessions
            if (vm.getType() == VirtualMachineType.ConsoleProxy) {
                final ConsoleProxyVO proxy = ApiDBUtils.findConsoleProxy(vm.getId());
                // proxy can be already destroyed
                if (proxy != null) {
                    vmResponse.setActiveViewerSessions(proxy.getActiveSession());
                }
            }

            final DataCenter zone = ApiDBUtils.findZoneById(vm.getDataCenterId());
            if (zone != null) {
                vmResponse.setZoneId(zone.getUuid());
                vmResponse.setZoneName(zone.getName());
                vmResponse.setDns1(zone.getDns1());
                vmResponse.setDns2(zone.getDns2());
            }

            final List<NicProfile> nicProfiles = ApiDBUtils.getNics(vm);
            for (final NicProfile singleNicProfile : nicProfiles) {
                final Network network = ApiDBUtils.findNetworkById(singleNicProfile.getNetworkId());
                if (network != null) {
                    if (network.getTrafficType() == TrafficType.Management) {
                        vmResponse.setPrivateIp(singleNicProfile.getIPv4Address());
                        vmResponse.setPrivateMacAddress(singleNicProfile.getMacAddress());
                        vmResponse.setPrivateNetmask(singleNicProfile.getIPv4Netmask());
                    } else if (network.getTrafficType() == TrafficType.Control) {
                        vmResponse.setLinkLocalIp(singleNicProfile.getIPv4Address());
                        vmResponse.setLinkLocalMacAddress(singleNicProfile.getMacAddress());
                        vmResponse.setLinkLocalNetmask(singleNicProfile.getIPv4Netmask());
                    } else if (network.getTrafficType() == TrafficType.Public) {
                        vmResponse.setPublicIp(singleNicProfile.getIPv4Address());
                        vmResponse.setPublicMacAddress(singleNicProfile.getMacAddress());
                        vmResponse.setPublicNetmask(singleNicProfile.getIPv4Netmask());
                        vmResponse.setGateway(singleNicProfile.getIPv4Gateway());
                    } else if (network.getTrafficType() == TrafficType.Guest) {
                        /*
                         * In basic zone, public ip has TrafficType.Guest in case EIP service is not enabled.
                         * When EIP service is enabled in the basic zone, system VM by default get the public
                         * IP allocated for EIP. So return the guest/public IP accordingly.
                         * */
                        final NetworkOffering networkOffering = ApiDBUtils.findNetworkOfferingById(network.getNetworkOfferingId());
                        if (networkOffering.getElasticIp()) {
                            final IpAddress ip = ApiDBUtils.findIpByAssociatedVmId(vm.getId());
                            if (ip != null) {
                                final Vlan vlan = ApiDBUtils.findVlanById(ip.getVlanId());
                                vmResponse.setPublicIp(ip.getAddress().addr());
                                vmResponse.setPublicNetmask(vlan.getVlanNetmask());
                                vmResponse.setGateway(vlan.getVlanGateway());
                            }
                        } else {
                            vmResponse.setPublicIp(singleNicProfile.getIPv4Address());
                            vmResponse.setPublicMacAddress(singleNicProfile.getMacAddress());
                            vmResponse.setPublicNetmask(singleNicProfile.getIPv4Netmask());
                            vmResponse.setGateway(singleNicProfile.getIPv4Gateway());
                        }
                    }
                }
            }
        }
        vmResponse.setObjectName("systemvm");
        return vmResponse;
    }

    @Override
    public DomainRouterResponse createDomainRouterResponse(final VirtualRouter router) {
        final List<DomainRouterJoinVO> viewVrs = ApiDBUtils.newDomainRouterView(router);
        final List<DomainRouterResponse> listVrs = ViewResponseHelper.createDomainRouterResponse(viewVrs.toArray(new DomainRouterJoinVO[viewVrs.size()]));
        assert listVrs != null && listVrs.size() == 1 : "There should be one virtual router returned";
        return listVrs.get(0);
    }

    @Override
    public HostResponse createHostResponse(final Host host, final EnumSet<HostDetails> details) {
        final List<HostJoinVO> viewHosts = ApiDBUtils.newHostView(host);
        final List<HostResponse> listHosts = ViewResponseHelper.createHostResponse(details, viewHosts.toArray(new HostJoinVO[viewHosts.size()]));
        assert listHosts != null && listHosts.size() == 1 : "There should be one host returned";
        return listHosts.get(0);
    }

    @Override
    public HostResponse createHostResponse(final Host host) {
        return createHostResponse(host, EnumSet.of(HostDetails.all));
    }

    @Override
    public HostForMigrationResponse createHostForMigrationResponse(final Host host) {
        return createHostForMigrationResponse(host, EnumSet.of(HostDetails.all));
    }

    @Override
    public HostForMigrationResponse createHostForMigrationResponse(final Host host, final EnumSet<HostDetails> details) {
        final List<HostJoinVO> viewHosts = ApiDBUtils.newHostView(host);
        final List<HostForMigrationResponse> listHosts = ViewResponseHelper.createHostForMigrationResponse(details, viewHosts.toArray(new HostJoinVO[viewHosts.size()]));
        assert listHosts != null && listHosts.size() == 1 : "There should be one host returned";
        return listHosts.get(0);
    }

    @Override
    public VlanIpRangeResponse createVlanIpRangeResponse(final Vlan vlan) {
        final Long podId = ApiDBUtils.getPodIdForVlan(vlan.getId());

        final VlanIpRangeResponse vlanResponse = new VlanIpRangeResponse();
        vlanResponse.setId(vlan.getUuid());
        if (vlan.getVlanType() != null) {
            vlanResponse.setForVirtualNetwork(vlan.getVlanType().equals(VlanType.VirtualNetwork));
        }
        vlanResponse.setVlan(vlan.getVlanTag());
        final DataCenter zone = ApiDBUtils.findZoneById(vlan.getDataCenterId());
        if (zone != null) {
            vlanResponse.setZoneId(zone.getUuid());
        }

        if (podId != null) {
            final HostPodVO pod = ApiDBUtils.findPodById(podId);
            if (pod != null) {
                vlanResponse.setPodId(pod.getUuid());
                vlanResponse.setPodName(pod.getName());
            }
        }

        vlanResponse.setGateway(vlan.getVlanGateway());
        vlanResponse.setNetmask(vlan.getVlanNetmask());

        // get start ip and end ip of corresponding vlan
        final String ipRange = vlan.getIpRange();
        if (ipRange != null) {
            final String[] range = ipRange.split("-");
            vlanResponse.setStartIp(range[0]);
            vlanResponse.setEndIp(range[1]);
        }

        vlanResponse.setIp6Gateway(vlan.getIp6Gateway());
        vlanResponse.setIp6Cidr(vlan.getIp6Cidr());

        final String ip6Range = vlan.getIp6Range();
        if (ip6Range != null) {
            final String[] range = ip6Range.split("-");
            vlanResponse.setStartIpv6(range[0]);
            vlanResponse.setEndIpv6(range[1]);
        }

        if (vlan.getNetworkId() != null) {
            final Network nw = ApiDBUtils.findNetworkById(vlan.getNetworkId());
            if (nw != null) {
                vlanResponse.setNetworkId(nw.getUuid());
            }
        }
        final Account owner = ApiDBUtils.getVlanAccount(vlan.getId());
        if (owner != null) {
            populateAccount(vlanResponse, owner.getId());
            populateDomain(vlanResponse, owner.getDomainId());
        } else {
            final Domain domain = ApiDBUtils.getVlanDomain(vlan.getId());
            if (domain != null) {
                populateDomain(vlanResponse, domain.getId());
            } else {
                final Long networkId = vlan.getNetworkId();
                if (networkId != null) {
                    final Network network = this._ntwkModel.getNetwork(networkId);
                    if (network != null) {
                        final Long accountId = network.getAccountId();
                        populateAccount(vlanResponse, accountId);
                        populateDomain(vlanResponse, ApiDBUtils.findAccountById(accountId).getDomainId());
                    }
                }
            }
        }

        if (vlan.getPhysicalNetworkId() != null) {
            final PhysicalNetwork pnw = ApiDBUtils.findPhysicalNetworkById(vlan.getPhysicalNetworkId());
            if (pnw != null) {
                vlanResponse.setPhysicalNetworkId(pnw.getUuid());
            }
        }
        vlanResponse.setObjectName("vlan");
        return vlanResponse;
    }

    @Override
    public IPAddressResponse createIPAddressResponse(final ResponseView view, final IpAddress ipAddr) {
        final VlanVO vlan = ApiDBUtils.findVlanById(ipAddr.getVlanId());
        final boolean forVirtualNetworks = vlan.getVlanType().equals(VlanType.VirtualNetwork);
        final long zoneId = ipAddr.getDataCenterId();

        final IPAddressResponse ipResponse = new IPAddressResponse();
        ipResponse.setId(ipAddr.getUuid());
        ipResponse.setIpAddress(ipAddr.getAddress().toString());
        if (ipAddr.getAllocatedTime() != null) {
            ipResponse.setAllocated(ipAddr.getAllocatedTime());
        }
        final DataCenter zone = ApiDBUtils.findZoneById(ipAddr.getDataCenterId());
        if (zone != null) {
            ipResponse.setZoneId(zone.getUuid());
            ipResponse.setZoneName(zone.getName());
        }
        ipResponse.setSourceNat(ipAddr.isSourceNat());
        ipResponse.setIsSystem(ipAddr.getSystem());

        // get account information
        if (ipAddr.getAllocatedToAccountId() != null) {
            populateOwner(ipResponse, ipAddr);
        }

        ipResponse.setForVirtualNetwork(forVirtualNetworks);
        ipResponse.setStaticNat(ipAddr.isOneToOneNat());

        if (ipAddr.getAssociatedWithVmId() != null) {
            final UserVm vm = ApiDBUtils.findUserVmById(ipAddr.getAssociatedWithVmId());
            if (vm != null) {
                ipResponse.setVirtualMachineId(vm.getUuid());
                ipResponse.setVirtualMachineName(vm.getHostName());
                if (vm.getDisplayName() != null) {
                    ipResponse.setVirtualMachineDisplayName(vm.getDisplayName());
                } else {
                    ipResponse.setVirtualMachineDisplayName(vm.getHostName());
                }
            }
        }
        if (ipAddr.getVmIp() != null) {
            ipResponse.setVirtualMachineIp(ipAddr.getVmIp());
        }

        if (ipAddr.getAssociatedWithNetworkId() != null) {
            final Network ntwk = ApiDBUtils.findNetworkById(ipAddr.getAssociatedWithNetworkId());
            if (ntwk != null) {
                ipResponse.setAssociatedNetworkId(ntwk.getUuid());
                ipResponse.setAssociatedNetworkName(ntwk.getName());
            }
        }

        if (ipAddr.getVpcId() != null) {
            final Vpc vpc = ApiDBUtils.findVpcById(ipAddr.getVpcId());
            if (vpc != null) {
                ipResponse.setVpcId(vpc.getUuid());
            }
        }

        // Network id the ip is associated with (if associated networkId is
        // null, try to get this information from vlan)
        final Long vlanNetworkId = ApiDBUtils.getVlanNetworkId(ipAddr.getVlanId());

        // Network id the ip belongs to
        final Long networkId;
        if (vlanNetworkId != null) {
            networkId = vlanNetworkId;
        } else {
            networkId = ApiDBUtils.getPublicNetworkIdByZone(zoneId);
        }

        if (networkId != null) {
            final NetworkVO nw = ApiDBUtils.findNetworkById(networkId);
            if (nw != null) {
                ipResponse.setNetworkId(nw.getUuid());
                ipResponse.setAssociatedNetworkName(nw.getName());
            }
        }
        ipResponse.setState(ipAddr.getState().toString());

        final NetworkACL acl = ApiDBUtils.findByNetworkACLId(ipAddr.getIpACLId());
        if (acl != null) {
            ipResponse.setAclId(acl.getUuid());
            ipResponse.setAclName(acl.getName());
        }

        if (ipAddr.getPhysicalNetworkId() != null) {
            final PhysicalNetworkVO pnw = ApiDBUtils.findPhysicalNetworkById(ipAddr.getPhysicalNetworkId());
            if (pnw != null) {
                ipResponse.setPhysicalNetworkId(pnw.getUuid());
            }
        }

        // show this info to full view only
        if (view == ResponseView.Full) {
            final VlanVO vl = ApiDBUtils.findVlanById(ipAddr.getVlanId());
            if (vl != null) {
                ipResponse.setVlanId(vl.getUuid());
                ipResponse.setVlanName(vl.getVlanTag());
            }
        }

        if (ipAddr.getSystem()) {
            if (ipAddr.isOneToOneNat()) {
                ipResponse.setPurpose(IpAddress.Purpose.StaticNat.toString());
            } else {
                ipResponse.setPurpose(IpAddress.Purpose.Lb.toString());
            }
        }

        ipResponse.setForDisplay(ipAddr.isDisplay());

        //set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.PublicIpAddress, ipAddr.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        ipResponse.setTags(tagResponses);

        ipResponse.setObjectName("ipaddress");
        return ipResponse;
    }

    @Override
    public GuestVlanRangeResponse createDedicatedGuestVlanRangeResponse(final GuestVlan vlan) {
        final GuestVlanRangeResponse guestVlanRangeResponse = new GuestVlanRangeResponse();

        guestVlanRangeResponse.setId(vlan.getUuid());
        final Long accountId = ApiDBUtils.getAccountIdForGuestVlan(vlan.getId());
        final Account owner = ApiDBUtils.findAccountById(accountId);
        if (owner != null) {
            populateAccount(guestVlanRangeResponse, owner.getId());
            populateDomain(guestVlanRangeResponse, owner.getDomainId());
        }
        guestVlanRangeResponse.setGuestVlanRange(vlan.getGuestVlanRange());
        guestVlanRangeResponse.setPhysicalNetworkId(vlan.getPhysicalNetworkId());
        final PhysicalNetworkVO physicalNetwork = ApiDBUtils.findPhysicalNetworkById(vlan.getPhysicalNetworkId());
        guestVlanRangeResponse.setZoneId(physicalNetwork.getDataCenterId());

        return guestVlanRangeResponse;
    }

    @Override
    public LoadBalancerResponse createLoadBalancerResponse(final LoadBalancer loadBalancer) {
        final LoadBalancerResponse lbResponse = new LoadBalancerResponse();
        lbResponse.setId(loadBalancer.getUuid());
        lbResponse.setName(loadBalancer.getName());
        lbResponse.setDescription(loadBalancer.getDescription());
        final List<String> cidrs = ApiDBUtils.findFirewallSourceCidrs(loadBalancer.getId());
        lbResponse.setCidrList(StringUtils.join(cidrs, ","));

        final IPAddressVO publicIp = ApiDBUtils.findIpAddressById(loadBalancer.getSourceIpAddressId());
        lbResponse.setPublicIpId(publicIp.getUuid());
        lbResponse.setPublicIp(publicIp.getAddress().addr());
        lbResponse.setPublicPort(Integer.toString(loadBalancer.getSourcePortStart()));
        lbResponse.setPrivatePort(Integer.toString(loadBalancer.getDefaultPortStart()));
        lbResponse.setAlgorithm(loadBalancer.getAlgorithm());
        lbResponse.setLbProtocol(loadBalancer.getLbProtocol());
        lbResponse.setForDisplay(loadBalancer.isDisplay());
        lbResponse.setClientTimeout(loadBalancer.getClientTimeout());
        lbResponse.setServerTimeout(loadBalancer.getServerTimeout());

        final FirewallRule.State state = loadBalancer.getState();
        String stateToSet = state.toString();
        if (state.equals(FirewallRule.State.Revoke)) {
            stateToSet = "Deleting";
        }
        lbResponse.setState(stateToSet);
        populateOwner(lbResponse, loadBalancer);
        final DataCenter zone = ApiDBUtils.findZoneById(publicIp.getDataCenterId());
        if (zone != null) {
            lbResponse.setZoneId(zone.getUuid());
        }

        //set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.LoadBalancer, loadBalancer.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        lbResponse.setTags(tagResponses);

        final Network ntwk = ApiDBUtils.findNetworkById(loadBalancer.getNetworkId());
        lbResponse.setNetworkId(ntwk.getUuid());

        lbResponse.setObjectName("loadbalancer");
        return lbResponse;
    }

    @Override
    public LBStickinessResponse createLBStickinessPolicyResponse(final List<? extends StickinessPolicy> stickinessPolicies, final LoadBalancer lb) {
        final LBStickinessResponse spResponse = new LBStickinessResponse();

        if (lb == null) {
            return spResponse;
        }
        spResponse.setlbRuleId(lb.getUuid());
        final Account account = ApiDBUtils.findAccountById(lb.getAccountId());
        if (account != null) {
            spResponse.setAccountName(account.getAccountName());
            final Domain domain = ApiDBUtils.findDomainById(account.getDomainId());
            if (domain != null) {
                spResponse.setDomainId(domain.getUuid());
                spResponse.setDomainName(domain.getName());
            }
        }

        final List<LBStickinessPolicyResponse> responses = new ArrayList<>();
        for (final StickinessPolicy stickinessPolicy : stickinessPolicies) {
            final LBStickinessPolicyResponse ruleResponse = new LBStickinessPolicyResponse(stickinessPolicy);
            responses.add(ruleResponse);
        }
        spResponse.setRules(responses);

        spResponse.setObjectName("stickinesspolicies");
        return spResponse;
    }

    @Override
    public LBStickinessResponse createLBStickinessPolicyResponse(final StickinessPolicy stickinessPolicy, final LoadBalancer lb) {
        final LBStickinessResponse spResponse = new LBStickinessResponse();

        spResponse.setlbRuleId(lb.getUuid());
        final Account accountTemp = ApiDBUtils.findAccountById(lb.getAccountId());
        if (accountTemp != null) {
            spResponse.setAccountName(accountTemp.getAccountName());
            final Domain domain = ApiDBUtils.findDomainById(accountTemp.getDomainId());
            if (domain != null) {
                spResponse.setDomainId(domain.getUuid());
                spResponse.setDomainName(domain.getName());
            }
        }

        final List<LBStickinessPolicyResponse> responses = new ArrayList<>();
        final LBStickinessPolicyResponse ruleResponse = new LBStickinessPolicyResponse(stickinessPolicy);
        responses.add(ruleResponse);

        spResponse.setRules(responses);

        spResponse.setObjectName("stickinesspolicies");
        return spResponse;
    }

    @Override
    public LBHealthCheckResponse createLBHealthCheckPolicyResponse(final List<? extends HealthCheckPolicy> healthcheckPolicies, final LoadBalancer lb) {
        final LBHealthCheckResponse hcResponse = new LBHealthCheckResponse();

        if (lb == null) {
            return hcResponse;
        }
        hcResponse.setlbRuleId(lb.getUuid());
        final Account account = ApiDBUtils.findAccountById(lb.getAccountId());
        if (account != null) {
            hcResponse.setAccountName(account.getAccountName());
            final Domain domain = ApiDBUtils.findDomainById(account.getDomainId());
            if (domain != null) {
                hcResponse.setDomainId(domain.getUuid());
                hcResponse.setDomainName(domain.getName());
            }
        }

        final List<LBHealthCheckPolicyResponse> responses = new ArrayList<>();
        for (final HealthCheckPolicy healthcheckPolicy : healthcheckPolicies) {
            final LBHealthCheckPolicyResponse ruleResponse = new LBHealthCheckPolicyResponse(healthcheckPolicy);
            responses.add(ruleResponse);
        }
        hcResponse.setRules(responses);

        hcResponse.setObjectName("healthcheckpolicies");
        return hcResponse;
    }

    @Override
    public LBHealthCheckResponse createLBHealthCheckPolicyResponse(final HealthCheckPolicy healthcheckPolicy, final LoadBalancer lb) {
        final LBHealthCheckResponse hcResponse = new LBHealthCheckResponse();

        hcResponse.setlbRuleId(lb.getUuid());
        final Account accountTemp = ApiDBUtils.findAccountById(lb.getAccountId());
        if (accountTemp != null) {
            hcResponse.setAccountName(accountTemp.getAccountName());
            final Domain domain = ApiDBUtils.findDomainById(accountTemp.getDomainId());
            if (domain != null) {
                hcResponse.setDomainId(domain.getUuid());
                hcResponse.setDomainName(domain.getName());
            }
        }

        final List<LBHealthCheckPolicyResponse> responses = new ArrayList<>();
        final LBHealthCheckPolicyResponse ruleResponse = new LBHealthCheckPolicyResponse(healthcheckPolicy);
        responses.add(ruleResponse);
        hcResponse.setRules(responses);
        hcResponse.setObjectName("healthcheckpolicies");
        return hcResponse;
    }

    @Override
    public PodResponse createPodResponse(final Pod pod, final Boolean showCapacities) {
        String[] ipRange = new String[2];
        if (pod.getDescription() != null && pod.getDescription().length() > 0) {
            ipRange = pod.getDescription().split("-");
        } else {
            ipRange[0] = pod.getDescription();
        }

        final PodResponse podResponse = new PodResponse();
        podResponse.setId(pod.getUuid());
        podResponse.setName(pod.getName());
        final DataCenter zone = ApiDBUtils.findZoneById(pod.getDataCenterId());
        if (zone != null) {
            podResponse.setZoneId(zone.getUuid());
            podResponse.setZoneName(zone.getName());
        }
        podResponse.setNetmask(NetUtils.getCidrNetmask(pod.getCidrSize()));
        podResponse.setStartIp(ipRange[0]);
        podResponse.setEndIp(ipRange.length > 1 && ipRange[1] != null ? ipRange[1] : "");
        podResponse.setGateway(pod.getGateway());
        podResponse.setAllocationState(pod.getAllocationState().toString());
        if (showCapacities != null && showCapacities) {
            final List<SummedCapacity> capacities = ApiDBUtils.getCapacityByClusterPodZone(null, pod.getId(), null);
            final Set<CapacityResponse> capacityResponses = new HashSet<>();
            for (final SummedCapacity capacity : capacities) {
                final CapacityResponse capacityResponse = new CapacityResponse();
                capacityResponse.setCapacityType(capacity.getCapacityType());
                capacityResponse.setCapacityUsed(capacity.getUsedCapacity() + capacity.getReservedCapacity());
                if (capacity.getCapacityType() == Capacity.CAPACITY_TYPE_STORAGE_ALLOCATED) {
                    final List<SummedCapacity> c = ApiDBUtils.findNonSharedStorageForClusterPodZone(null, pod.getId(), null);
                    capacityResponse.setCapacityTotal(capacity.getTotalCapacity() - c.get(0).getTotalCapacity());
                    capacityResponse.setCapacityUsed(capacity.getUsedCapacity() - c.get(0).getUsedCapacity());
                } else {
                    capacityResponse.setCapacityTotal(capacity.getTotalCapacity());
                }
                if (capacityResponse.getCapacityTotal() != 0) {
                    capacityResponse.setPercentageAllocated(s_percentFormat.format((float) capacityResponse.getCapacityUsed() / (float) capacityResponse.getCapacityTotal() * 100f));
                } else {
                    capacityResponse.setPercentageAllocated(s_percentFormat.format(0L));
                }
                capacityResponses.add(capacityResponse);
            }
            // Do it for stats as well.
            capacityResponses.addAll(getStatsCapacityresponse(null, null, pod.getId(), pod.getDataCenterId()));
            podResponse.setCapacitites(new ArrayList<>(capacityResponses));
        }
        podResponse.setObjectName("pod");
        return podResponse;
    }

    @Override
    public ZoneResponse createZoneResponse(final ResponseView view, final DataCenter dataCenter, final Boolean showCapacities) {
        final DataCenterJoinVO vOffering = ApiDBUtils.newDataCenterView(dataCenter);
        return ApiDBUtils.newDataCenterResponse(view, vOffering, showCapacities);
    }

    @Override
    public VolumeResponse createVolumeResponse(final ResponseView view, final Volume volume) {
        final List<VolumeJoinVO> viewVrs = ApiDBUtils.newVolumeView(volume);
        final List<VolumeResponse> listVrs = ViewResponseHelper.createVolumeResponse(view, viewVrs.toArray(new VolumeJoinVO[viewVrs.size()]));
        assert listVrs != null && listVrs.size() == 1 : "There should be one volume returned";
        return listVrs.get(0);
    }

    @Override
    public InstanceGroupResponse createInstanceGroupResponse(final InstanceGroup group) {
        final InstanceGroupJoinVO vgroup = ApiDBUtils.newInstanceGroupView(group);
        return ApiDBUtils.newInstanceGroupResponse(vgroup);
    }

    @Override
    public StoragePoolResponse createStoragePoolResponse(final StoragePool pool) {
        final List<StoragePoolJoinVO> viewPools = ApiDBUtils.newStoragePoolView(pool);
        final List<StoragePoolResponse> listPools = ViewResponseHelper.createStoragePoolResponse(viewPools.toArray(new StoragePoolJoinVO[viewPools.size()]));
        assert listPools != null && listPools.size() == 1 : "There should be one storage pool returned";
        return listPools.get(0);
    }

    @Override
    public StoragePoolResponse createStoragePoolForMigrationResponse(final StoragePool pool) {
        final List<StoragePoolJoinVO> viewPools = ApiDBUtils.newStoragePoolView(pool);
        final List<StoragePoolResponse> listPools = ViewResponseHelper.createStoragePoolForMigrationResponse(viewPools.toArray(new StoragePoolJoinVO[viewPools.size()]));
        assert listPools != null && listPools.size() == 1 : "There should be one storage pool returned";
        return listPools.get(0);
    }

    @Override
    public ClusterResponse createClusterResponse(final Cluster cluster, final Boolean showCapacities) {
        final ClusterResponse clusterResponse = new ClusterResponse();
        clusterResponse.setId(cluster.getUuid());
        clusterResponse.setName(cluster.getName());
        final HostPodVO pod = ApiDBUtils.findPodById(cluster.getPodId());
        if (pod != null) {
            clusterResponse.setPodId(pod.getUuid());
            clusterResponse.setPodName(pod.getName());
        }
        final DataCenter dc = ApiDBUtils.findZoneById(cluster.getDataCenterId());
        if (dc != null) {
            clusterResponse.setZoneId(dc.getUuid());
            clusterResponse.setZoneName(dc.getName());
        }
        clusterResponse.setHypervisorType(cluster.getHypervisorType().toString());
        clusterResponse.setClusterType(cluster.getClusterType().toString());
        clusterResponse.setAllocationState(cluster.getAllocationState().toString());
        clusterResponse.setManagedState(cluster.getManagedState().toString());
        final String cpuOvercommitRatio = ApiDBUtils.findClusterDetails(cluster.getId(), "cpuOvercommitRatio");
        final String memoryOvercommitRatio = ApiDBUtils.findClusterDetails(cluster.getId(), "memoryOvercommitRatio");
        clusterResponse.setCpuOvercommitRatio(cpuOvercommitRatio);
        clusterResponse.setMemoryOvercommitRatio(memoryOvercommitRatio);

        if (showCapacities != null && showCapacities) {
            final List<SummedCapacity> capacities = ApiDBUtils.getCapacityByClusterPodZone(null, null, cluster.getId());
            final Set<CapacityResponse> capacityResponses = new HashSet<>();

            for (final SummedCapacity capacity : capacities) {
                final CapacityResponse capacityResponse = new CapacityResponse();
                capacityResponse.setCapacityType(capacity.getCapacityType());
                capacityResponse.setCapacityUsed(capacity.getUsedCapacity() + capacity.getReservedCapacity());

                if (capacity.getCapacityType() == Capacity.CAPACITY_TYPE_STORAGE_ALLOCATED) {
                    final List<SummedCapacity> c = ApiDBUtils.findNonSharedStorageForClusterPodZone(null, null, cluster.getId());
                    capacityResponse.setCapacityTotal(capacity.getTotalCapacity() - c.get(0).getTotalCapacity());
                    capacityResponse.setCapacityUsed(capacity.getUsedCapacity() - c.get(0).getUsedCapacity());
                } else {
                    capacityResponse.setCapacityTotal(capacity.getTotalCapacity());
                }
                if (capacityResponse.getCapacityTotal() != 0) {
                    capacityResponse.setPercentageAllocated(s_percentFormat.format((float) capacityResponse.getCapacityUsed() / (float) capacityResponse.getCapacityTotal() * 100f));
                } else {
                    capacityResponse.setPercentageAllocated(s_percentFormat.format(0L));
                }
                capacityResponses.add(capacityResponse);
            }
            // Do it for stats as well.
            capacityResponses.addAll(getStatsCapacityresponse(null, cluster.getId(), pod.getId(), pod.getDataCenterId()));
            clusterResponse.setCapacitites(new ArrayList<>(capacityResponses));
        }
        clusterResponse.setObjectName("cluster");
        return clusterResponse;
    }

    @Override
    public FirewallRuleResponse createPortForwardingRuleResponse(final PortForwardingRule fwRule) {
        final FirewallRuleResponse response = new FirewallRuleResponse();
        response.setId(fwRule.getUuid());
        response.setPrivateStartPort(Integer.toString(fwRule.getDestinationPortStart()));
        response.setPrivateEndPort(Integer.toString(fwRule.getDestinationPortEnd()));
        response.setProtocol(fwRule.getProtocol());
        response.setPublicStartPort(Integer.toString(fwRule.getSourcePortStart()));
        response.setPublicEndPort(Integer.toString(fwRule.getSourcePortEnd()));
        final List<String> cidrs = ApiDBUtils.findFirewallSourceCidrs(fwRule.getId());
        response.setCidrList(StringUtils.join(cidrs, ","));

        final Network guestNtwk = ApiDBUtils.findNetworkById(fwRule.getNetworkId());
        response.setNetworkId(guestNtwk.getUuid());

        final IpAddress ip = ApiDBUtils.findIpAddressById(fwRule.getSourceIpAddressId());

        if (ip != null) {
            response.setPublicIpAddressId(ip.getUuid());
            response.setPublicIpAddress(ip.getAddress().addr());
            if (fwRule.getDestinationIpAddress() != null) {
                response.setDestNatVmIp(fwRule.getDestinationIpAddress().toString());
                final UserVm vm = ApiDBUtils.findUserVmById(fwRule.getVirtualMachineId());
                if (vm != null) {
                    response.setVirtualMachineId(vm.getUuid());
                    response.setVirtualMachineName(vm.getHostName());

                    if (vm.getDisplayName() != null) {
                        response.setVirtualMachineDisplayName(vm.getDisplayName());
                    } else {
                        response.setVirtualMachineDisplayName(vm.getHostName());
                    }
                }
            }
        }
        final FirewallRule.State state = fwRule.getState();
        String stateToSet = state.toString();
        if (state.equals(FirewallRule.State.Revoke)) {
            stateToSet = "Deleting";
        }

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.PortForwardingRule, fwRule.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);

        response.setState(stateToSet);
        response.setForDisplay(fwRule.isDisplay());
        response.setObjectName("portforwardingrule");
        return response;
    }

    @Override
    public IpForwardingRuleResponse createIpForwardingRuleResponse(final StaticNatRule fwRule) {
        final IpForwardingRuleResponse response = new IpForwardingRuleResponse();
        response.setId(fwRule.getUuid());
        response.setProtocol(fwRule.getProtocol());

        final IpAddress ip = ApiDBUtils.findIpAddressById(fwRule.getSourceIpAddressId());

        if (ip != null) {
            response.setPublicIpAddressId(ip.getId());
            response.setPublicIpAddress(ip.getAddress().addr());
            if (fwRule.getDestIpAddress() != null) {
                final UserVm vm = ApiDBUtils.findUserVmById(ip.getAssociatedWithVmId());
                if (vm != null) {// vm might be destroyed
                    response.setVirtualMachineId(vm.getUuid());
                    response.setVirtualMachineName(vm.getHostName());
                    if (vm.getDisplayName() != null) {
                        response.setVirtualMachineDisplayName(vm.getDisplayName());
                    } else {
                        response.setVirtualMachineDisplayName(vm.getHostName());
                    }
                }
            }
        }
        final FirewallRule.State state = fwRule.getState();
        String stateToSet = state.toString();
        if (state.equals(FirewallRule.State.Revoke)) {
            stateToSet = "Deleting";
        }

        response.setStartPort(fwRule.getSourcePortStart());
        response.setEndPort(fwRule.getSourcePortEnd());
        response.setProtocol(fwRule.getProtocol());
        response.setState(stateToSet);
        response.setObjectName("ipforwardingrule");
        return response;
    }

    @Override
    public User findUserById(final Long userId) {
        return ApiDBUtils.findUserById(userId);
    }

    @Override
    public UserVm findUserVmById(final Long vmId) {
        return ApiDBUtils.findUserVmById(vmId);
    }

    @Override
    public VolumeVO findVolumeById(final Long volumeId) {
        return ApiDBUtils.findVolumeById(volumeId);
    }

    @Override
    public Account findAccountByNameDomain(final String accountName, final Long domainId) {
        return ApiDBUtils.findAccountByNameDomain(accountName, domainId);
    }

    @Override
    public VirtualMachineTemplate findTemplateById(final Long templateId) {
        return ApiDBUtils.findTemplateById(templateId);
    }

    @Override
    public VpnUsersResponse createVpnUserResponse(final VpnUser vpnUser) {
        final VpnUsersResponse vpnResponse = new VpnUsersResponse();
        vpnResponse.setId(vpnUser.getUuid());
        vpnResponse.setUserName(vpnUser.getUsername());
        vpnResponse.setState(vpnUser.getState().toString());

        populateOwner(vpnResponse, vpnUser);

        vpnResponse.setObjectName("vpnuser");
        return vpnResponse;
    }

    @Override
    public RemoteAccessVpnResponse createRemoteAccessVpnResponse(final RemoteAccessVpn vpn) {
        final RemoteAccessVpnResponse vpnResponse = new RemoteAccessVpnResponse();
        final IpAddress ip = ApiDBUtils.findIpAddressById(vpn.getServerAddressId());
        if (ip != null) {
            vpnResponse.setPublicIpId(ip.getUuid());
            vpnResponse.setPublicIp(ip.getAddress().addr());
        }
        vpnResponse.setIpRange(vpn.getIpRange());
        vpnResponse.setPresharedKey(vpn.getIpsecPresharedKey());
        populateOwner(vpnResponse, vpn);
        vpnResponse.setState(vpn.getState().toString());
        vpnResponse.setId(vpn.getUuid());
        vpnResponse.setForDisplay(vpn.isDisplay());
        vpnResponse.setObjectName("remoteaccessvpn");

        return vpnResponse;
    }

    @Override
    public List<TemplateResponse> createTemplateResponses(final ResponseView view, final long templateId, final Long zoneId, final boolean readyOnly) {
        final VirtualMachineTemplate template = findTemplateById(templateId);
        return createTemplateResponses(view, template, zoneId, readyOnly);
    }

    @Override
    public List<TemplateResponse> createTemplateResponses(final ResponseView view, final long templateId, final Long snapshotId, final Long volumeId, final boolean readyOnly) {
        Long zoneId = null;

        if (snapshotId != null) {
            final Snapshot snapshot = ApiDBUtils.findSnapshotById(snapshotId);
            final VolumeVO volume = findVolumeById(snapshot.getVolumeId());

            // it seems that the volume can actually be removed from the DB at some point if it's deleted
            // if volume comes back null, use another technique to try to discover the zone
            if (volume == null) {
                final SnapshotDataStoreVO snapshotStore = this._snapshotStoreDao.findBySnapshot(snapshot.getId(), DataStoreRole.Primary);

                if (snapshotStore != null) {
                    final long storagePoolId = snapshotStore.getDataStoreId();

                    final StoragePoolVO storagePool = this._storagePoolDao.findById(storagePoolId);

                    if (storagePool != null) {
                        zoneId = storagePool.getDataCenterId();
                    }
                }
            } else {
                zoneId = volume.getDataCenterId();
            }
        } else {
            final VolumeVO volume = findVolumeById(volumeId);

            zoneId = volume.getDataCenterId();
        }

        if (zoneId == null) {
            throw new CloudRuntimeException("Unable to determine the zone ID");
        }

        return createTemplateResponses(view, templateId, zoneId, readyOnly);
    }

    //TODO: we need to deprecate uploadVO, since extract is done in a synchronous fashion
    @Override
    public ExtractResponse createExtractResponse(final Long id, final Long zoneId, final Long accountId, final String mode, final String url) {

        final ExtractResponse response = new ExtractResponse();
        response.setObjectName("template");
        final VMTemplateVO template = ApiDBUtils.findTemplateById(id);
        response.setId(template.getUuid());
        response.setName(template.getName());
        if (zoneId != null) {
            final DataCenter zone = ApiDBUtils.findZoneById(zoneId);
            response.setZoneId(zone.getUuid());
            response.setZoneName(zone.getName());
        }
        response.setMode(mode);
        response.setUrl(url);
        response.setState(UploadStatus.DOWNLOAD_URL_CREATED.toString());
        final Account account = ApiDBUtils.findAccountById(accountId);
        response.setAccountId(account.getUuid());

        return response;
    }

    @Override
    public String toSerializedString(final CreateCmdResponse response, final String responseType) {
        return ApiResponseSerializer.toSerializedString(response, responseType);
    }

    @Override
    public TemplateResponse createTemplateUpdateResponse(final ResponseView view, final VirtualMachineTemplate result) {
        final List<TemplateJoinVO> tvo = ApiDBUtils.newTemplateView(result);
        final List<TemplateResponse> listVrs = ViewResponseHelper.createTemplateUpdateResponse(view, tvo.toArray(new TemplateJoinVO[tvo.size()]));
        assert listVrs != null && listVrs.size() == 1 : "There should be one template returned";
        return listVrs.get(0);
    }

    @Override
    public List<TemplateResponse> createTemplateResponses(final ResponseView view, final VirtualMachineTemplate result, final Long zoneId, final boolean readyOnly) {
        final List<TemplateJoinVO> tvo;
        if (zoneId == null || zoneId == -1 || result.isCrossZones()) {
            tvo = ApiDBUtils.newTemplateView(result);
        } else {
            tvo = ApiDBUtils.newTemplateView(result, zoneId, readyOnly);
        }
        return ViewResponseHelper.createTemplateResponse(view, tvo.toArray(new TemplateJoinVO[tvo.size()]));
    }

    @Override
    public List<CapacityResponse> createCapacityResponse(final List<? extends Capacity> result, final DecimalFormat format) {
        final List<CapacityResponse> capacityResponses = new ArrayList<>();

        for (final Capacity summedCapacity : result) {
            final CapacityResponse capacityResponse = new CapacityResponse();
            capacityResponse.setCapacityTotal(summedCapacity.getTotalCapacity());
            capacityResponse.setCapacityType(summedCapacity.getCapacityType());
            capacityResponse.setCapacityUsed(summedCapacity.getUsedCapacity());
            if (summedCapacity.getPodId() != null) {
                capacityResponse.setPodId(ApiDBUtils.findPodById(summedCapacity.getPodId()).getUuid());
                final HostPodVO pod = ApiDBUtils.findPodById(summedCapacity.getPodId());
                if (pod != null) {
                    capacityResponse.setPodId(pod.getUuid());
                    capacityResponse.setPodName(pod.getName());
                }
            }
            if (summedCapacity.getClusterId() != null) {
                final ClusterVO cluster = ApiDBUtils.findClusterById(summedCapacity.getClusterId());
                if (cluster != null) {
                    capacityResponse.setClusterId(cluster.getUuid());
                    capacityResponse.setClusterName(cluster.getName());
                    if (summedCapacity.getPodId() == null) {
                        final HostPodVO pod = ApiDBUtils.findPodById(cluster.getPodId());
                        capacityResponse.setPodId(pod.getUuid());
                        capacityResponse.setPodName(pod.getName());
                    }
                }
            }
            final DataCenter zone = ApiDBUtils.findZoneById(summedCapacity.getDataCenterId());
            if (zone != null) {
                capacityResponse.setZoneId(zone.getUuid());
                capacityResponse.setZoneName(zone.getName());
            }
            if (summedCapacity.getUsedPercentage() != null) {
                capacityResponse.setPercentageAllocated(format.format(summedCapacity.getUsedPercentage() * 100f));
            } else if (summedCapacity.getTotalCapacity() != 0) {
                capacityResponse.setPercentageAllocated(format.format((float) summedCapacity.getUsedCapacity() / (float) summedCapacity.getTotalCapacity() * 100f));
            } else {
                capacityResponse.setPercentageAllocated(format.format(0L));
            }

            capacityResponse.setObjectName("capacity");
            capacityResponses.add(capacityResponse);
        }

        final List<VgpuTypesInfo> gpuCapacities;
        if (result.size() > 1 && (gpuCapacities = ApiDBUtils.getGpuCapacites(result.get(0).getDataCenterId(), result.get(0).getPodId(), result.get(0).getClusterId())) != null) {
            final HashMap<String, Long> vgpuVMs = ApiDBUtils.getVgpuVmsCount(result.get(0).getDataCenterId(), result.get(0).getPodId(), result.get(0).getClusterId());

            float capacityUsed = 0;
            long capacityMax = 0;
            for (final VgpuTypesInfo capacity : gpuCapacities) {
                if (vgpuVMs.containsKey(capacity.getGroupName().concat(capacity.getModelName()))) {
                    capacityUsed += (float) vgpuVMs.get(capacity.getGroupName().concat(capacity.getModelName())) / capacity.getMaxVpuPerGpu();
                }
                if (capacity.getModelName().equals(GPU.GPUType.passthrough.toString())) {
                    capacityMax += capacity.getMaxCapacity();
                }
            }

            final DataCenter zone = ApiDBUtils.findZoneById(result.get(0).getDataCenterId());
            final CapacityResponse capacityResponse = new CapacityResponse();
            if (zone != null) {
                capacityResponse.setZoneId(zone.getUuid());
                capacityResponse.setZoneName(zone.getName());
            }
            if (result.get(0).getPodId() != null) {
                final HostPodVO pod = ApiDBUtils.findPodById(result.get(0).getPodId());
                capacityResponse.setPodId(pod.getUuid());
                capacityResponse.setPodName(pod.getName());
            }
            if (result.get(0).getClusterId() != null) {
                final ClusterVO cluster = ApiDBUtils.findClusterById(result.get(0).getClusterId());
                capacityResponse.setClusterId(cluster.getUuid());
                capacityResponse.setClusterName(cluster.getName());
            }
            capacityResponse.setCapacityType(Capacity.CAPACITY_TYPE_GPU);
            capacityResponse.setCapacityUsed((long) Math.ceil(capacityUsed));
            capacityResponse.setCapacityTotal(capacityMax);
            if (capacityMax > 0) {
                capacityResponse.setPercentageAllocated(format.format(capacityUsed / capacityMax * 100f));
            } else {
                capacityResponse.setPercentageAllocated(format.format(0));
            }
            capacityResponse.setObjectName("capacity");
            capacityResponses.add(capacityResponse);
        }
        return capacityResponses;
    }

    @Override
    public TemplatePermissionsResponse createTemplatePermissionsResponse(final ResponseView view, final List<String> accountNames, final Long id) {
        Long templateOwnerDomain = null;
        final VirtualMachineTemplate template = ApiDBUtils.findTemplateById(id);
        final Account templateOwner = ApiDBUtils.findAccountById(template.getAccountId());
        if (view == ResponseView.Full) {
            // FIXME: we have just template id and need to get template owner
            // from that
            if (templateOwner != null) {
                templateOwnerDomain = templateOwner.getDomainId();
            }
        }

        final TemplatePermissionsResponse response = new TemplatePermissionsResponse();
        response.setId(template.getUuid());
        response.setPublicTemplate(template.isPublicTemplate());
        if (view == ResponseView.Full && templateOwnerDomain != null) {
            final Domain domain = ApiDBUtils.findDomainById(templateOwnerDomain);
            if (domain != null) {
                response.setDomainId(domain.getUuid());
            }
        }

        // Set accounts
        final List<String> projectIds = new ArrayList<>();
        final List<String> regularAccounts = new ArrayList<>();
        for (final String accountName : accountNames) {
            final Account account = ApiDBUtils.findAccountByNameDomain(accountName, templateOwner.getDomainId());
            if (account.getType() != Account.ACCOUNT_TYPE_PROJECT) {
                regularAccounts.add(accountName);
            } else {
                // convert account to projectIds
                final Project project = ApiDBUtils.findProjectByProjectAccountId(account.getId());

                if (project.getUuid() != null && !project.getUuid().isEmpty()) {
                    projectIds.add(project.getUuid());
                } else {
                    projectIds.add(String.valueOf(project.getId()));
                }
            }
        }

        if (!projectIds.isEmpty()) {
            response.setProjectIds(projectIds);
        }

        if (!regularAccounts.isEmpty()) {
            response.setAccountNames(regularAccounts);
        }

        response.setObjectName("templatepermission");
        return response;
    }

    @Override
    public AsyncJobResponse queryJobResult(final QueryAsyncJobResultCmd cmd) {
        final Account caller = CallContext.current().getCallingAccount();

        final AsyncJob job = this._entityMgr.findById(AsyncJob.class, cmd.getId());
        if (job == null) {
            throw new InvalidParameterValueException("Unable to find a job by id " + cmd.getId());
        }

        final User userJobOwner = this._accountMgr.getUserIncludingRemoved(job.getUserId());
        final Account jobOwner = this._accountMgr.getAccount(userJobOwner.getAccountId());

        //check permissions
        if (this._accountMgr.isNormalUser(caller.getId())) {
            //regular user can see only jobs he owns
            if (caller.getId() != jobOwner.getId()) {
                throw new PermissionDeniedException("Account " + caller + " is not authorized to see job id=" + job.getId());
            }
        } else if (this._accountMgr.isDomainAdmin(caller.getId())) {
            this._accountMgr.checkAccess(caller, null, true, jobOwner);
        }

        return createAsyncJobResponse(this._jobMgr.queryJob(cmd.getId(), true));
    }

    public AsyncJobResponse createAsyncJobResponse(final AsyncJob job) {
        final AsyncJobJoinVO vJob = ApiDBUtils.newAsyncJobView(job);
        return ApiDBUtils.newAsyncJobResponse(vJob);
    }

    @Override
    public NetworkOfferingResponse createNetworkOfferingResponse(final NetworkOffering offering) {
        final NetworkOfferingResponse response = new NetworkOfferingResponse();
        response.setId(offering.getUuid());
        response.setName(offering.getName());
        response.setDisplayText(offering.getDisplayText());
        response.setTags(offering.getTags());
        response.setTrafficType(offering.getTrafficType().toString());
        response.setIsDefault(offering.isDefault());
        response.setSpecifyVlan(offering.getSpecifyVlan());
        response.setConserveMode(offering.isConserveMode());
        response.setSpecifyIpRanges(offering.getSpecifyIpRanges());
        response.setAvailability(offering.getAvailability().toString());
        response.setIsPersistent(offering.getIsPersistent());
        response.setNetworkRate(ApiDBUtils.getNetworkRate(offering.getId()));
        response.setEgressDefaultPolicy(offering.getEgressDefaultPolicy());
        response.setConcurrentConnections(offering.getConcurrentConnections());
        response.setSupportsStrechedL2Subnet(offering.getSupportsStrechedL2());
        final Long so;
        if (offering.getServiceOfferingId() != null) {
            so = offering.getServiceOfferingId();
        } else {
            so = ApiDBUtils.findDefaultRouterServiceOffering();
        }
        if (so != null) {
            final ServiceOffering serviceOffering = ApiDBUtils.findServiceOfferingById(so);
            if (serviceOffering != null) {
                response.setServiceOfferingId(serviceOffering.getUuid());
                response.setServiceOfferingName(serviceOffering.getName());
            }
        }

        final ServiceOffering secondaryServiceOffering = this._serviceOfferingDao.findById(offering.getSecondaryServiceOfferingId());
        if (secondaryServiceOffering != null) {
            response.setSecondaryServiceOfferingId(secondaryServiceOffering.getUuid());
            response.setSecondaryServiceOfferingName(secondaryServiceOffering.getName());
        }

        if (offering.getGuestType() != null) {
            response.setGuestIpType(offering.getGuestType().toString());
        }

        response.setState(offering.getState().name());

        final Map<Service, Set<Provider>> serviceProviderMap = ApiDBUtils.listNetworkOfferingServices(offering.getId());
        final List<ServiceResponse> serviceResponses = new ArrayList<>();
        for (final Map.Entry<Service, Set<Provider>> entry : serviceProviderMap.entrySet()) {
            final Service service = entry.getKey();
            final Set<Provider> srvc_providers = entry.getValue();
            final ServiceResponse svcRsp = new ServiceResponse();
            // skip gateway service
            if (service == Service.Gateway) {
                continue;
            }
            svcRsp.setName(service.getName());
            final List<ProviderResponse> providers = getProviderResponses(srvc_providers);
            svcRsp.setProviders(providers);

            if (Service.Lb == service) {
                final List<CapabilityResponse> lbCapResponse = new ArrayList<>();

                final CapabilityResponse lbIsoaltion = new CapabilityResponse();
                lbIsoaltion.setName(Capability.SupportedLBIsolation.getName());
                lbIsoaltion.setValue(offering.getDedicatedLB() ? "dedicated" : "shared");
                lbCapResponse.add(lbIsoaltion);

                final CapabilityResponse eLb = new CapabilityResponse();
                eLb.setName(Capability.ElasticLb.getName());
                eLb.setValue(offering.getElasticLb() ? "true" : "false");
                lbCapResponse.add(eLb);

                final CapabilityResponse inline = new CapabilityResponse();
                inline.setName(Capability.InlineMode.getName());
                inline.setValue(offering.isInline() ? "true" : "false");
                lbCapResponse.add(inline);

                svcRsp.setCapabilities(lbCapResponse);
            } else if (Service.SourceNat == service) {
                final List<CapabilityResponse> capabilities = new ArrayList<>();
                final CapabilityResponse sharedSourceNat = new CapabilityResponse();
                sharedSourceNat.setName(Capability.SupportedSourceNatTypes.getName());
                sharedSourceNat.setValue(offering.getSharedSourceNat() ? "perzone" : "peraccount");
                capabilities.add(sharedSourceNat);

                final CapabilityResponse redundantRouter = new CapabilityResponse();
                redundantRouter.setName(Capability.RedundantRouter.getName());
                redundantRouter.setValue(offering.getRedundantRouter() ? "true" : "false");
                capabilities.add(redundantRouter);

                svcRsp.setCapabilities(capabilities);
            } else if (service == Service.StaticNat) {
                final List<CapabilityResponse> staticNatCapResponse = new ArrayList<>();

                final CapabilityResponse eIp = new CapabilityResponse();
                eIp.setName(Capability.ElasticIp.getName());
                eIp.setValue(offering.getElasticIp() ? "true" : "false");
                staticNatCapResponse.add(eIp);

                final CapabilityResponse associatePublicIp = new CapabilityResponse();
                associatePublicIp.setName(Capability.AssociatePublicIP.getName());
                associatePublicIp.setValue(offering.getAssociatePublicIP() ? "true" : "false");
                staticNatCapResponse.add(associatePublicIp);

                svcRsp.setCapabilities(staticNatCapResponse);
            }

            serviceResponses.add(svcRsp);
        }
        response.setForVpc(this._configMgr.isOfferingForVpc(offering));

        response.setServices(serviceResponses);

        //set network offering details
        final Map<Detail, String> details = this._ntwkModel.getNtwkOffDetails(offering.getId());
        if (details != null && !details.isEmpty()) {
            response.setDetails(details);
        }

        response.setObjectName("networkoffering");
        return response;
    }

    private List<ProviderResponse> getProviderResponses(final Set<Provider> srvc_providers) {
        final List<ProviderResponse> providers = new ArrayList<>();
        for (final Provider provider : srvc_providers) {
            if (provider != null) {
                final ProviderResponse providerRsp = new ProviderResponse();
                providerRsp.setName(provider.getName());
                providers.add(providerRsp);
            }
        }
        return providers;
    }

    @Override
    public NetworkResponse createNetworkResponse(final ResponseView view, final Network network) {
        // need to get network profile in order to retrieve dns information from
        // there
        final NetworkProfile profile = ApiDBUtils.getNetworkProfile(network.getId());
        final NetworkResponse response = new NetworkResponse();
        response.setId(network.getUuid());
        response.setName(network.getName());
        response.setDisplaytext(network.getDisplayText());
        if (network.getBroadcastDomainType() != null) {
            response.setBroadcastDomainType(network.getBroadcastDomainType().toString());
        }

        if (network.getTrafficType() != null) {
            response.setTrafficType(network.getTrafficType().name());
        }

        if (network.getGuestType() != null) {
            response.setType(network.getGuestType().toString());
        }

        response.setGateway(network.getGateway());

        // FIXME - either set netmask or cidr
        response.setCidr(network.getCidr());
        response.setNetworkCidr(network.getNetworkCidr());
        // If network has reservation its entire network cidr is defined by
        // getNetworkCidr()
        // if no reservation is present then getCidr() will define the entire
        // network cidr
        if (network.getNetworkCidr() != null) {
            response.setNetmask(NetUtils.cidr2Netmask(network.getNetworkCidr()));
        }
        if (network.getCidr() != null && network.getNetworkCidr() == null) {
            response.setNetmask(NetUtils.cidr2Netmask(network.getCidr()));
        }

        response.setIpExclusionList(((NetworkVO) network).getIpExclusionList());

        response.setIp6Gateway(network.getIp6Gateway());
        response.setIp6Cidr(network.getIp6Cidr());

        // create response for reserved IP ranges that can be used for
        // non-cloudstack purposes
        String reservation = null;
        if (network.getCidr() != null && NetUtils.isNetworkAWithinNetworkB(network.getCidr(), network.getNetworkCidr())) {
            final String[] guestVmCidrPair = network.getCidr().split("\\/");
            final String[] guestCidrPair = network.getNetworkCidr().split("\\/");

            final Long guestVmCidrSize = Long.valueOf(guestVmCidrPair[1]);
            final Long guestCidrSize = Long.valueOf(guestCidrPair[1]);

            final String[] guestVmIpRange = NetUtils.getIpRangeFromCidr(guestVmCidrPair[0], guestVmCidrSize);
            final String[] guestIpRange = NetUtils.getIpRangeFromCidr(guestCidrPair[0], guestCidrSize);
            final long startGuestIp = NetUtils.ip2Long(guestIpRange[0]);
            final long endGuestIp = NetUtils.ip2Long(guestIpRange[1]);
            final long startVmIp = NetUtils.ip2Long(guestVmIpRange[0]);
            final long endVmIp = NetUtils.ip2Long(guestVmIpRange[1]);

            if (startVmIp == startGuestIp && endVmIp < endGuestIp - 1) {
                reservation = NetUtils.long2Ip(endVmIp + 1) + "-" + NetUtils.long2Ip(endGuestIp);
            }
            if (endVmIp == endGuestIp && startVmIp > startGuestIp + 1) {
                reservation = NetUtils.long2Ip(startGuestIp) + "-" + NetUtils.long2Ip(startVmIp - 1);
            }
            if (startVmIp > startGuestIp + 1 && endVmIp < endGuestIp - 1) {
                reservation = NetUtils.long2Ip(startGuestIp) + "-" + NetUtils.long2Ip(startVmIp - 1) + " ,  " + NetUtils.long2Ip(endVmIp + 1) + "-" + NetUtils.long2Ip(endGuestIp);
            }
        }
        response.setReservedIpRange(reservation);

        if (network.getBroadcastUri() != null) {
            final String broadcastUri = network.getBroadcastUri().toString();
            response.setBroadcastUri(broadcastUri);
            String vlan = "N/A";
            switch (BroadcastDomainType.getSchemeValue(network.getBroadcastUri())) {
                case Vlan:
                case Vxlan:
                    vlan = BroadcastDomainType.getValue(network.getBroadcastUri());
                    break;
            }
            response.setVlan(vlan);
        }

        final DataCenter zone = ApiDBUtils.findZoneById(network.getDataCenterId());
        if (zone != null) {
            response.setZoneId(zone.getUuid());
            response.setZoneName(zone.getName());
        }
        if (network.getPhysicalNetworkId() != null) {
            final PhysicalNetworkVO pnet = ApiDBUtils.findPhysicalNetworkById(network.getPhysicalNetworkId());
            response.setPhysicalNetworkId(pnet.getUuid());
        }

        // populate network offering information
        final NetworkOffering networkOffering = ApiDBUtils.findNetworkOfferingById(network.getNetworkOfferingId());
        if (networkOffering != null) {
            response.setNetworkOfferingId(networkOffering.getUuid());
            response.setNetworkOfferingName(networkOffering.getName());
            response.setNetworkOfferingDisplayText(networkOffering.getDisplayText());
            response.setNetworkOfferingConserveMode(networkOffering.isConserveMode());
            response.setIsSystem(networkOffering.isSystemOnly());
            response.setNetworkOfferingAvailability(networkOffering.getAvailability().toString());
            response.setIsPersistent(networkOffering.getIsPersistent());
        }

        if (network.getAclType() != null) {
            response.setAclType(network.getAclType().toString());
        }
        response.setDisplayNetwork(network.getDisplayNetwork());
        response.setState(network.getState().toString());
        response.setRestartRequired(network.isRestartRequired());
        final NetworkVO nw = ApiDBUtils.findNetworkById(network.getRelated());
        if (nw != null) {
            response.setRelated(nw.getUuid());
        }
        response.setNetworkDomain(network.getNetworkDomain());

        response.setDns1(profile.getDns1());
        response.setDns2(profile.getDns2());
        response.setDhcpTftpServer(profile.getDhcpTftpServer());
        response.setDhcpBootfileName(profile.getDhcpBootfileName());
        // populate capability
        final Map<Service, Map<Capability, String>> serviceCapabilitiesMap = ApiDBUtils.getNetworkCapabilities(network.getId(), network.getDataCenterId());
        final List<ServiceResponse> serviceResponses = new ArrayList<>();
        if (serviceCapabilitiesMap != null) {
            for (final Map.Entry<Service, Map<Capability, String>> entry : serviceCapabilitiesMap.entrySet()) {
                final Service service = entry.getKey();
                final ServiceResponse serviceResponse = new ServiceResponse();
                // skip gateway service
                if (service == Service.Gateway) {
                    continue;
                }
                serviceResponse.setName(service.getName());

                // set list of capabilities for the service
                final List<CapabilityResponse> capabilityResponses = new ArrayList<>();
                final Map<Capability, String> serviceCapabilities = entry.getValue();
                if (serviceCapabilities != null) {
                    for (final Map.Entry<Capability, String> ser_cap_entries : serviceCapabilities.entrySet()) {
                        final Capability capability = ser_cap_entries.getKey();
                        final CapabilityResponse capabilityResponse = new CapabilityResponse();
                        final String capabilityValue = ser_cap_entries.getValue();
                        capabilityResponse.setName(capability.getName());
                        capabilityResponse.setValue(capabilityValue);
                        capabilityResponse.setObjectName("capability");
                        capabilityResponses.add(capabilityResponse);
                    }
                    serviceResponse.setCapabilities(capabilityResponses);
                }

                serviceResponse.setObjectName("service");
                serviceResponses.add(serviceResponse);
            }
        }
        response.setServices(serviceResponses);

        if (network.getAclType() == null || network.getAclType() == ACLType.Account) {
            populateOwner(response, network);
        } else {
            // get domain from network_domain table
            final Pair<Long, Boolean> domainNetworkDetails = ApiDBUtils.getDomainNetworkDetails(network.getId());
            if (domainNetworkDetails.first() != null) {
                final Domain domain = ApiDBUtils.findDomainById(domainNetworkDetails.first());
                if (domain != null) {
                    response.setDomainId(domain.getUuid());
                }
            }
            response.setSubdomainAccess(domainNetworkDetails.second());
        }

        final Long dedicatedDomainId = ApiDBUtils.getDedicatedNetworkDomain(network.getId());
        if (dedicatedDomainId != null) {
            final Domain domain = ApiDBUtils.findDomainById(dedicatedDomainId);
            if (domain != null) {
                response.setDomainId(domain.getUuid());
                response.setDomainName(domain.getName());
            }
        }

        response.setSpecifyIpRanges(network.getSpecifyIpRanges());
        if (network.getVpcId() != null) {
            final Vpc vpc = ApiDBUtils.findVpcById(network.getVpcId());
            if (vpc != null) {
                response.setVpcId(vpc.getUuid());
                response.setVpcName(vpc.getName());
            }
        }
        response.setCanUseForDeploy(ApiDBUtils.canUseForDeploy(network));

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.Network, network.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);

        if (network.getNetworkACLId() != null) {
            final NetworkACL acl = ApiDBUtils.findByNetworkACLId(network.getNetworkACLId());
            if (acl != null) {
                response.setAclId(acl.getUuid());
                response.setAclName(acl.getName());
            }
        }

        response.setStrechedL2Subnet(network.isStrechedL2Network());
        if (network.isStrechedL2Network()) {
            final Set<String> networkSpannedZones = new HashSet<>();
            final List<VMInstanceVO> vmInstances = new ArrayList<>();
            vmInstances.addAll(ApiDBUtils.listUserVMsByNetworkId(network.getId()));
            vmInstances.addAll(ApiDBUtils.listDomainRoutersByNetworkId(network.getId()));
            for (final VirtualMachine vm : vmInstances) {
                final DataCenter vmZone = ApiDBUtils.findZoneById(vm.getDataCenterId());
                networkSpannedZones.add(vmZone.getUuid());
            }
            response.setNetworkSpannedZones(networkSpannedZones);
        }
        response.setObjectName("network");
        return response;
    }

    @Override
    public UserResponse createUserResponse(final User user) {
        final UserAccountJoinVO vUser = ApiDBUtils.newUserView(user);
        return ApiDBUtils.newUserResponse(vUser);
    }

    // this method is used for response generation via createAccount (which
    // creates an account + user)
    @Override
    public AccountResponse createUserAccountResponse(final ResponseView view, final UserAccount user) {
        return ApiDBUtils.newAccountResponse(view, ApiDBUtils.findAccountViewById(user.getAccountId()));
    }

    @Override
    public List<TemplateResponse> createIsoResponses(final ResponseView view, final VirtualMachineTemplate result, final Long zoneId, final boolean readyOnly) {
        final List<TemplateJoinVO> tvo;
        if (zoneId == null || zoneId == -1) {
            tvo = ApiDBUtils.newTemplateView(result);
        } else {
            tvo = ApiDBUtils.newTemplateView(result, zoneId, readyOnly);
        }

        return ViewResponseHelper.createIsoResponse(view, tvo.toArray(new TemplateJoinVO[tvo.size()]));
    }

    @Override
    public ProjectResponse createProjectResponse(final Project project) {
        final List<ProjectJoinVO> viewPrjs = ApiDBUtils.newProjectView(project);
        final List<ProjectResponse> listPrjs = ViewResponseHelper.createProjectResponse(viewPrjs.toArray(new ProjectJoinVO[viewPrjs.size()]));
        assert listPrjs != null && listPrjs.size() == 1 : "There should be one project  returned";
        return listPrjs.get(0);
    }

    @Override
    public FirewallResponse createFirewallResponse(final FirewallRule fwRule) {
        final FirewallResponse response = new FirewallResponse();

        response.setId(fwRule.getUuid());
        response.setProtocol(fwRule.getProtocol());
        if (fwRule.getSourcePortStart() != null) {
            response.setStartPort(fwRule.getSourcePortStart());
        }

        if (fwRule.getSourcePortEnd() != null) {
            response.setEndPort(fwRule.getSourcePortEnd());
        }

        final List<String> cidrs = ApiDBUtils.findFirewallSourceCidrs(fwRule.getId());
        response.setCidrList(StringUtils.join(cidrs, ","));

        if (fwRule.getTrafficType() == FirewallRule.TrafficType.Ingress) {
            final IpAddress ip = ApiDBUtils.findIpAddressById(fwRule.getSourceIpAddressId());
            response.setPublicIpAddressId(ip.getUuid());
            response.setPublicIpAddress(ip.getAddress().addr());
        }

        final Network network = ApiDBUtils.findNetworkById(fwRule.getNetworkId());
        response.setNetworkId(network.getUuid());

        final FirewallRule.State state = fwRule.getState();
        String stateToSet = state.toString();
        if (state.equals(FirewallRule.State.Revoke)) {
            stateToSet = "Deleting";
        }

        response.setIcmpCode(fwRule.getIcmpCode());
        response.setIcmpType(fwRule.getIcmpType());
        response.setForDisplay(fwRule.isDisplay());

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.FirewallRule, fwRule.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);

        response.setState(stateToSet);
        response.setObjectName("firewallrule");
        return response;
    }

    @Override
    public HypervisorCapabilitiesResponse createHypervisorCapabilitiesResponse(final HypervisorCapabilities hpvCapabilities) {
        final HypervisorCapabilitiesResponse hpvCapabilitiesResponse = new HypervisorCapabilitiesResponse();
        hpvCapabilitiesResponse.setId(hpvCapabilities.getUuid());
        hpvCapabilitiesResponse.setHypervisor(hpvCapabilities.getHypervisorType());
        hpvCapabilitiesResponse.setHypervisorVersion(hpvCapabilities.getHypervisorVersion());
        hpvCapabilitiesResponse.setMaxGuestsLimit(hpvCapabilities.getMaxGuestsLimit());
        hpvCapabilitiesResponse.setMaxDataVolumesLimit(hpvCapabilities.getMaxDataVolumesLimit());
        hpvCapabilitiesResponse.setMaxHostsPerCluster(hpvCapabilities.getMaxHostsPerCluster());
        hpvCapabilitiesResponse.setIsStorageMotionSupported(hpvCapabilities.isStorageMotionSupported());
        return hpvCapabilitiesResponse;
    }

    @Override
    public PhysicalNetworkResponse createPhysicalNetworkResponse(final PhysicalNetwork result) {
        final PhysicalNetworkResponse response = new PhysicalNetworkResponse();

        final DataCenter zone = ApiDBUtils.findZoneById(result.getDataCenterId());
        if (zone != null) {
            response.setZoneId(zone.getUuid());
        }
        response.setNetworkSpeed(result.getSpeed());
        response.setVlan(result.getVnetString());
        if (result.getDomainId() != null) {
            final Domain domain = ApiDBUtils.findDomainById(result.getDomainId());
            if (domain != null) {
                response.setDomainId(domain.getUuid());
            }
        }
        response.setId(result.getUuid());
        if (result.getBroadcastDomainRange() != null) {
            response.setBroadcastDomainRange(result.getBroadcastDomainRange().toString());
        }
        response.setIsolationMethods(result.getIsolationMethods());
        response.setTags(result.getTags());
        if (result.getState() != null) {
            response.setState(result.getState().toString());
        }

        response.setName(result.getName());

        response.setObjectName("physicalnetwork");
        return response;
    }

    @Override
    public ServiceResponse createNetworkServiceResponse(final Service service) {
        final ServiceResponse response = new ServiceResponse();
        response.setName(service.getName());

        // set list of capabilities required for the service
        final List<CapabilityResponse> capabilityResponses = new ArrayList<>();
        final Capability[] capabilities = service.getCapabilities();
        for (final Capability cap : capabilities) {
            final CapabilityResponse capabilityResponse = new CapabilityResponse();
            capabilityResponse.setName(cap.getName());
            capabilityResponse.setObjectName("capability");
            if (cap.getName().equals(Capability.SupportedLBIsolation.getName()) || cap.getName().equals(Capability.SupportedSourceNatTypes.getName())
                    || cap.getName().equals(Capability.RedundantRouter.getName())) {
                capabilityResponse.setCanChoose(true);
            } else {
                capabilityResponse.setCanChoose(false);
            }
            capabilityResponses.add(capabilityResponse);
        }
        response.setCapabilities(capabilityResponses);

        // set list of providers providing this service
        final List<? extends Network.Provider> serviceProviders = ApiDBUtils.getProvidersForService(service);
        final List<ProviderResponse> serviceProvidersResponses = new ArrayList<>();
        for (final Network.Provider serviceProvider : serviceProviders) {
            // return only Virtual Router/JuniperSRX/CiscoVnmc as a provider for the firewall
            if (service == Service.Firewall && serviceProvider != Provider.VirtualRouter) {
                continue;
            }

            final ProviderResponse serviceProviderResponse = createServiceProviderResponse(serviceProvider);
            serviceProvidersResponses.add(serviceProviderResponse);
        }
        response.setProviders(serviceProvidersResponses);

        response.setObjectName("networkservice");
        return response;
    }

    private ProviderResponse createServiceProviderResponse(final Provider serviceProvider) {
        final ProviderResponse response = new ProviderResponse();
        response.setName(serviceProvider.getName());
        final boolean canEnableIndividualServices = ApiDBUtils.canElementEnableIndividualServices(serviceProvider);
        response.setCanEnableIndividualServices(canEnableIndividualServices);
        return response;
    }

    @Override
    public ProviderResponse createNetworkServiceProviderResponse(final PhysicalNetworkServiceProvider result) {
        final ProviderResponse response = new ProviderResponse();
        response.setId(result.getUuid());
        response.setName(result.getProviderName());
        final PhysicalNetwork pnw = ApiDBUtils.findPhysicalNetworkById(result.getPhysicalNetworkId());
        if (pnw != null) {
            response.setPhysicalNetworkId(pnw.getUuid());
        }
        final PhysicalNetwork dnw = ApiDBUtils.findPhysicalNetworkById(result.getDestinationPhysicalNetworkId());
        if (dnw != null) {
            response.setDestinationPhysicalNetworkId(dnw.getUuid());
        }
        response.setState(result.getState().toString());

        // set enabled services
        final List<String> services = new ArrayList<>();
        for (final Service service : result.getEnabledServices()) {
            services.add(service.getName());
        }
        response.setServices(services);

        final Provider serviceProvider = Provider.getProvider(result.getProviderName());
        final boolean canEnableIndividualServices = ApiDBUtils.canElementEnableIndividualServices(serviceProvider);
        response.setCanEnableIndividualServices(canEnableIndividualServices);

        response.setObjectName("networkserviceprovider");
        return response;
    }

    @Override
    public TrafficTypeResponse createTrafficTypeResponse(final PhysicalNetworkTrafficType result) {
        final TrafficTypeResponse response = new TrafficTypeResponse();
        response.setId(result.getUuid());
        final PhysicalNetwork pnet = ApiDBUtils.findPhysicalNetworkById(result.getPhysicalNetworkId());
        if (pnet != null) {
            response.setPhysicalNetworkId(pnet.getUuid());
        }
        if (result.getTrafficType() != null) {
            response.setTrafficType(result.getTrafficType().toString());
        }

        response.setXenLabel(result.getXenNetworkLabel());
        response.setKvmLabel(result.getKvmNetworkLabel());

        response.setObjectName("traffictype");
        return response;
    }

    @Override
    public VirtualRouterProviderResponse createVirtualRouterProviderResponse(final VirtualRouterProvider result) {
        //generate only response of the VR/VPCVR provider type
        if (!(result.getType() == VirtualRouterProvider.Type.VirtualRouter || result.getType() == VirtualRouterProvider.Type.VPCVirtualRouter)) {
            return null;
        }
        final VirtualRouterProviderResponse response = new VirtualRouterProviderResponse();
        response.setId(result.getUuid());
        final PhysicalNetworkServiceProvider nsp = ApiDBUtils.findPhysicalNetworkServiceProviderById(result.getNspId());
        if (nsp != null) {
            response.setNspId(nsp.getUuid());
        }
        response.setEnabled(result.isEnabled());

        response.setObjectName("virtualrouterelement");
        return response;
    }

    @Override
    public StorageNetworkIpRangeResponse createStorageNetworkIpRangeResponse(final StorageNetworkIpRange result) {
        final StorageNetworkIpRangeResponse response = new StorageNetworkIpRangeResponse();
        response.setUuid(result.getUuid());
        response.setVlan(result.getVlan());
        response.setEndIp(result.getEndIp());
        response.setStartIp(result.getStartIp());
        response.setPodUuid(result.getPodUuid());
        response.setZoneUuid(result.getZoneUuid());
        response.setNetworkUuid(result.getNetworkUuid());
        response.setNetmask(result.getNetmask());
        response.setGateway(result.getGateway());
        response.setObjectName("storagenetworkiprange");
        return response;
    }

    @Override
    public RegionResponse createRegionResponse(final Region region) {
        final RegionResponse response = new RegionResponse();
        response.setId(region.getId());
        response.setName(region.getName());
        response.setEndPoint(region.getEndPoint());
        response.setObjectName("region");
        return response;
    }

    @Override
    public ImageStoreResponse createImageStoreResponse(final ImageStore os) {
        final List<ImageStoreJoinVO> viewStores = ApiDBUtils.newImageStoreView(os);
        final List<ImageStoreResponse> listStores = ViewResponseHelper.createImageStoreResponse(viewStores.toArray(new ImageStoreJoinVO[viewStores.size()]));
        assert listStores != null && listStores.size() == 1 : "There should be one image data store returned";
        return listStores.get(0);
    }

    @Override
    public ResourceTagResponse createResourceTagResponse(final ResourceTag resourceTag, final boolean keyValueOnly) {
        final ResourceTagJoinVO rto = ApiDBUtils.newResourceTagView(resourceTag);
        if (rto == null) {
            return null;
        }
        return ApiDBUtils.newResourceTagResponse(rto, keyValueOnly);
    }

    @Override
    public Site2SiteVpnGatewayResponse createSite2SiteVpnGatewayResponse(final Site2SiteVpnGateway result) {
        final Site2SiteVpnGatewayResponse response = new Site2SiteVpnGatewayResponse();
        response.setId(result.getUuid());
        response.setIp(ApiDBUtils.findIpAddressById(result.getAddrId()).getAddress().toString());
        final Vpc vpc = ApiDBUtils.findVpcById(result.getVpcId());
        if (vpc != null) {
            response.setVpcId(vpc.getUuid());
        }
        response.setRemoved(result.getRemoved());
        response.setForDisplay(result.isDisplay());
        response.setObjectName("vpngateway");

        populateAccount(response, result.getAccountId());
        populateDomain(response, result.getDomainId());
        return response;
    }

    @Override
    public VpcOfferingResponse createVpcOfferingResponse(final VpcOffering offering) {
        final VpcOfferingResponse response = new VpcOfferingResponse();
        response.setId(offering.getUuid());
        response.setName(offering.getName());
        response.setDisplayText(offering.getDisplayText());
        response.setIsDefault(offering.isDefault());
        response.setState(offering.getState().name());
        final ServiceOffering serviceOffering = this._serviceOfferingDao.findById(offering.getServiceOfferingId());
        if (serviceOffering != null) {
            response.setServiceOfferingId(serviceOffering.getUuid());
            response.setServiceOfferingName(serviceOffering.getName());
        }
        final ServiceOffering secondaryServiceOffering = this._serviceOfferingDao.findById(offering.getSecondaryServiceOfferingId());
        if (secondaryServiceOffering != null) {
            response.setSecondaryServiceOfferingId(secondaryServiceOffering.getUuid());
            response.setSecondaryServiceOfferingName(secondaryServiceOffering.getName());
        }
        final Map<Service, Set<Provider>> serviceProviderMap = ApiDBUtils.listVpcOffServices(offering.getId());
        final List<ServiceResponse> serviceResponses = getServiceResponses(serviceProviderMap);

        response.setServices(serviceResponses);
        response.setObjectName("vpcoffering");
        return response;
    }

    @Override
    public VpcResponse createVpcResponse(final ResponseView view, final Vpc vpc) {
        final VpcResponse response = new VpcResponse();
        response.setId(vpc.getUuid());
        response.setName(vpc.getName());
        response.setDisplayText(vpc.getDisplayText());
        response.setState(vpc.getState().name());
        final VpcOffering voff = ApiDBUtils.findVpcOfferingById(vpc.getVpcOfferingId());
        if (voff != null) {
            response.setVpcOfferingId(voff.getUuid());
            response.setVpcOfferingName(voff.getName());
            response.setVpcOfferingDisplayText(voff.getDisplayText());
        }
        response.setCidr(vpc.getCidr());
        response.setRestartRequired(vpc.isRestartRequired());
        response.setNetworkDomain(vpc.getNetworkDomain());
        response.setForDisplay(vpc.isDisplay());
        response.setRedundantRouter(vpc.isRedundant());
        response.setSourceNatList(vpc.getSourceNatList());
        response.setSyslogServerList(vpc.getSyslogServerList());
        response.setAdvertInterval(vpc.getAdvertInterval());
        response.setAdvertMethod(vpc.getAdvertMethod());

        final Map<Service, Set<Provider>> serviceProviderMap = ApiDBUtils.listVpcOffServices(vpc.getVpcOfferingId());
        final List<ServiceResponse> serviceResponses = getServiceResponses(serviceProviderMap);

        final List<NetworkResponse> networkResponses = new ArrayList<>();
        final List<? extends Network> networks = ApiDBUtils.listVpcNetworks(vpc.getId());
        for (final Network network : networks) {
            final NetworkResponse ntwkRsp = createNetworkResponse(view, network);
            networkResponses.add(ntwkRsp);
        }

        final DataCenter zone = ApiDBUtils.findZoneById(vpc.getZoneId());
        if (zone != null) {
            response.setZoneId(zone.getUuid());
            response.setZoneName(zone.getName());
        }

        response.setNetworks(networkResponses);
        response.setServices(serviceResponses);
        populateOwner(response, vpc);

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.Vpc, vpc.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);
        response.setObjectName("vpc");
        return response;
    }

    private List<ServiceResponse> getServiceResponses(final Map<Service, Set<Provider>> serviceProviderMap) {
        final List<ServiceResponse> serviceResponses = new ArrayList<>();
        for (final Map.Entry<Service, Set<Provider>> entry : serviceProviderMap.entrySet()) {
            final Service service = entry.getKey();
            final Set<Provider> serviceProviders = entry.getValue();
            final ServiceResponse svcRsp = new ServiceResponse();
            // skip gateway service
            if (service == Service.Gateway) {
                continue;
            }
            svcRsp.setName(service.getName());
            final List<ProviderResponse> providers = getProviderResponses(serviceProviders);
            svcRsp.setProviders(providers);

            serviceResponses.add(svcRsp);
        }
        return serviceResponses;
    }

    @Override
    public NetworkACLItemResponse createNetworkACLItemResponse(final NetworkACLItem aclItem) {
        final NetworkACLItemResponse response = new NetworkACLItemResponse();

        response.setId(aclItem.getUuid());
        response.setProtocol(aclItem.getProtocol());
        if (aclItem.getSourcePortStart() != null) {
            response.setStartPort(Integer.toString(aclItem.getSourcePortStart()));
        }

        if (aclItem.getSourcePortEnd() != null) {
            response.setEndPort(Integer.toString(aclItem.getSourcePortEnd()));
        }

        response.setCidrList(StringUtils.join(aclItem.getSourceCidrList(), ","));

        response.setTrafficType(aclItem.getTrafficType().toString());

        final NetworkACLItem.State state = aclItem.getState();
        String stateToSet = state.toString();
        if (state.equals(NetworkACLItem.State.Revoke)) {
            stateToSet = "Deleting";
        }

        response.setIcmpCode(aclItem.getIcmpCode());
        response.setIcmpType(aclItem.getIcmpType());

        response.setState(stateToSet);
        response.setNumber(aclItem.getNumber());
        response.setAction(aclItem.getAction().toString());
        response.setForDisplay(aclItem.isDisplay());

        final NetworkACL acl = ApiDBUtils.findByNetworkACLId(aclItem.getAclId());
        if (acl != null) {
            response.setAclId(acl.getUuid());
        }

        //set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.NetworkACL, aclItem.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);

        response.setObjectName("networkacl");
        return response;
    }

    @Override
    public NetworkACLResponse createNetworkACLResponse(final NetworkACL networkACL) {
        final NetworkACLResponse response = new NetworkACLResponse();
        response.setId(networkACL.getUuid());
        response.setName(networkACL.getName());
        response.setDescription(networkACL.getDescription());
        response.setForDisplay(networkACL.isDisplay());
        final Vpc vpc = ApiDBUtils.findVpcById(networkACL.getVpcId());
        if (vpc != null) {
            response.setVpcId(vpc.getUuid());
        }
        response.setObjectName("networkacllist");
        return response;
    }

    @Override
    public PrivateGatewayResponse createPrivateGatewayResponse(final PrivateGateway result) {
        final PrivateGatewayResponse response = new PrivateGatewayResponse();
        response.setId(result.getUuid());
        if (result.getVpcId() != null) {
            final Vpc vpc = ApiDBUtils.findVpcById(result.getVpcId());
            response.setVpcId(vpc.getUuid());
        }

        final DataCenter zone = ApiDBUtils.findZoneById(result.getZoneId());
        if (zone != null) {
            response.setZoneId(zone.getUuid());
            response.setZoneName(zone.getName());
        }
        response.setAddress(result.getIp4Address());

        final Network network = ApiDBUtils.findNetworkById(result.getNetworkId());
        response.setNetworkId(network.getUuid());
        response.setNetworkName(network.getName());
        response.setCidr(network.getCidr());

        populateAccount(response, result.getAccountId());
        populateDomain(response, result.getDomainId());
        response.setState(result.getState().toString());
        response.setSourceNat(result.getSourceNat());

        final NetworkACL acl = ApiDBUtils.findByNetworkACLId(result.getNetworkACLId());
        if (acl != null) {
            response.setAclId(acl.getUuid());
        }

        response.setObjectName("privategateway");

        return response;
    }

    @Override
    public StaticRouteResponse createStaticRouteResponse(final StaticRoute result) {
        final StaticRouteResponse response = new StaticRouteResponse();
        response.setId(result.getUuid());
        if (result.getVpcId() != null) {
            final Vpc vpc = ApiDBUtils.findVpcById(result.getVpcId());
            if (vpc != null) {
                response.setVpcId(vpc.getUuid());
            }
        }
        response.setCidr(result.getCidr());
        response.setGwIpAddress(result.getGwIpAddress());

        StaticRoute.State state = result.getState();
        if (state.equals(StaticRoute.State.Revoke)) {
            state = StaticRoute.State.Deleting;
        }
        response.setState(state.toString());
        populateAccount(response, result.getAccountId());
        populateDomain(response, result.getDomainId());

        // set tag information
        final List<? extends ResourceTag> tags = ApiDBUtils.listByResourceTypeAndId(ResourceObjectType.StaticRoute, result.getId());
        final List<ResourceTagResponse> tagResponses = new ArrayList<>();
        for (final ResourceTag tag : tags) {
            final ResourceTagResponse tagResponse = createResourceTagResponse(tag, true);
            if (tagResponse != null) {
                tagResponses.add(tagResponse);
            }
        }
        response.setTags(tagResponses);
        response.setObjectName("staticroute");

        return response;
    }

    @Override
    public Site2SiteCustomerGatewayResponse createSite2SiteCustomerGatewayResponse(final Site2SiteCustomerGateway result) {
        final Site2SiteCustomerGatewayResponse response = new Site2SiteCustomerGatewayResponse();
        response.setId(result.getUuid());
        response.setName(result.getName());
        response.setGatewayIp(result.getGatewayIp());
        response.setGuestCidrList(result.getGuestCidrList());
        response.setIpsecPsk(result.getIpsecPsk());
        response.setIkePolicy(result.getIkePolicy());
        response.setEspPolicy(result.getEspPolicy());
        response.setIkeLifetime(result.getIkeLifetime());
        response.setEspLifetime(result.getEspLifetime());
        response.setDpd(result.getDpd());
        response.setEncap(result.getEncap());
        response.setRemoved(result.getRemoved());
        response.setObjectName("vpncustomergateway");

        populateAccount(response, result.getAccountId());
        populateDomain(response, result.getDomainId());

        return response;
    }

    @Override
    public Site2SiteVpnConnectionResponse createSite2SiteVpnConnectionResponse(final Site2SiteVpnConnection result) {
        final Site2SiteVpnConnectionResponse response = new Site2SiteVpnConnectionResponse();
        response.setId(result.getUuid());
        response.setPassive(result.isPassive());

        final Long vpnGatewayId = result.getVpnGatewayId();
        if (vpnGatewayId != null) {
            final Site2SiteVpnGateway vpnGateway = ApiDBUtils.findVpnGatewayById(vpnGatewayId);
            if (vpnGateway != null) {
                response.setVpnGatewayId(vpnGateway.getUuid());
                final long ipId = vpnGateway.getAddrId();
                final IPAddressVO ipObj = ApiDBUtils.findIpAddressById(ipId);
                response.setIp(ipObj.getAddress().addr());
            }
        }

        final Long customerGatewayId = result.getCustomerGatewayId();
        if (customerGatewayId != null) {
            final Site2SiteCustomerGateway customerGateway = ApiDBUtils.findCustomerGatewayById(customerGatewayId);
            if (customerGateway != null) {
                response.setCustomerGatewayId(customerGateway.getUuid());
                response.setGatewayIp(customerGateway.getGatewayIp());
                response.setGuestCidrList(customerGateway.getGuestCidrList());
                response.setIpsecPsk(customerGateway.getIpsecPsk());
                response.setIkePolicy(customerGateway.getIkePolicy());
                response.setEspPolicy(customerGateway.getEspPolicy());
                response.setIkeLifetime(customerGateway.getIkeLifetime());
                response.setEspLifetime(customerGateway.getEspLifetime());
                response.setDpd(customerGateway.getDpd());
                response.setEncap(customerGateway.getEncap());
            }
        }

        populateAccount(response, result.getAccountId());
        populateDomain(response, result.getDomainId());

        response.setState(result.getState().toString());
        response.setCreated(result.getCreated());
        response.setRemoved(result.getRemoved());
        response.setForDisplay(result.isDisplay());
        response.setObjectName("vpnconnection");
        return response;
    }

    @Override
    public GuestOSResponse createGuestOSResponse(final GuestOS guestOS) {
        final GuestOSResponse response = new GuestOSResponse();
        response.setDescription(guestOS.getDisplayName());
        response.setId(guestOS.getUuid());
        response.setIsUserDefined(Boolean.valueOf(guestOS.getIsUserDefined()).toString());
        final GuestOSCategoryVO category = ApiDBUtils.findGuestOsCategoryById(guestOS.getCategoryId());
        if (category != null) {
            response.setOsCategoryId(category.getUuid());
        }

        response.setObjectName("ostype");
        return response;
    }

    @Override
    public GuestOsMappingResponse createGuestOSMappingResponse(final GuestOSHypervisor guestOSHypervisor) {
        final GuestOsMappingResponse response = new GuestOsMappingResponse();
        response.setId(guestOSHypervisor.getUuid());
        response.setHypervisor(guestOSHypervisor.getHypervisorType());
        response.setHypervisorVersion(guestOSHypervisor.getHypervisorVersion());
        response.setOsNameForHypervisor(guestOSHypervisor.getGuestOsName());
        response.setIsUserDefined(Boolean.valueOf(guestOSHypervisor.getIsUserDefined()).toString());
        final GuestOS guestOs = ApiDBUtils.findGuestOSById(guestOSHypervisor.getGuestOsId());
        if (guestOs != null) {
            response.setOsStdName(guestOs.getDisplayName());
            response.setOsTypeId(guestOs.getUuid());
        }

        response.setObjectName("guestosmapping");
        return response;
    }

    @Override
    public VMSnapshotResponse createVMSnapshotResponse(final VMSnapshot vmSnapshot) {
        final VMSnapshotResponse vmSnapshotResponse = new VMSnapshotResponse();
        vmSnapshotResponse.setId(vmSnapshot.getUuid());
        vmSnapshotResponse.setName(vmSnapshot.getName());
        vmSnapshotResponse.setState(vmSnapshot.getState());
        vmSnapshotResponse.setCreated(vmSnapshot.getCreated());
        vmSnapshotResponse.setDescription(vmSnapshot.getDescription());
        vmSnapshotResponse.setDisplayName(vmSnapshot.getDisplayName());
        final UserVm vm = ApiDBUtils.findUserVmById(vmSnapshot.getVmId());
        if (vm != null) {
            vmSnapshotResponse.setVirtualMachineid(vm.getUuid());
        }
        if (vmSnapshot.getParent() != null) {
            final VMSnapshot vmSnapshotParent = ApiDBUtils.getVMSnapshotById(vmSnapshot.getParent());
            if (vmSnapshotParent != null) {
                vmSnapshotResponse.setParent(vmSnapshotParent.getUuid());
                vmSnapshotResponse.setParentName(vmSnapshotParent.getDisplayName());
            }
        }
        populateOwner(vmSnapshotResponse, vmSnapshot);
        final Project project = ApiDBUtils.findProjectByProjectAccountId(vmSnapshot.getAccountId());
        if (project != null) {
            vmSnapshotResponse.setProjectId(project.getUuid());
            vmSnapshotResponse.setProjectName(project.getName());
        }
        vmSnapshotResponse.setCurrent(vmSnapshot.getCurrent());
        vmSnapshotResponse.setType(vmSnapshot.getType().toString());
        vmSnapshotResponse.setObjectName("vmsnapshot");
        return vmSnapshotResponse;
    }

    @Override
    public NicSecondaryIpResponse createSecondaryIPToNicResponse(final NicSecondaryIp result) {
        final NicSecondaryIpResponse response = new NicSecondaryIpResponse();
        final NicVO nic = this._entityMgr.findById(NicVO.class, result.getNicId());
        final NetworkVO network = this._entityMgr.findById(NetworkVO.class, result.getNetworkId());
        response.setId(result.getUuid());
        response.setIpAddr(result.getIp4Address());
        response.setNicId(nic.getUuid());
        response.setNwId(network.getUuid());
        response.setObjectName("nicsecondaryip");
        return response;
    }

    @Override
    public NicResponse createNicResponse(final Nic result) {
        final NicResponse response = new NicResponse();
        final NetworkVO network = this._entityMgr.findById(NetworkVO.class, result.getNetworkId());
        final VMInstanceVO vm = this._entityMgr.findById(VMInstanceVO.class, result.getInstanceId());

        response.setId(result.getUuid());
        response.setNetworkid(network.getUuid());

        if (vm != null) {
            response.setVmId(vm.getUuid());
        }

        response.setIpaddress(result.getIPv4Address());

        if (result.getSecondaryIp()) {
            final List<NicSecondaryIpVO> secondaryIps = ApiDBUtils.findNicSecondaryIps(result.getId());
            if (secondaryIps != null) {
                final List<NicSecondaryIpResponse> ipList = new ArrayList<>();
                for (final NicSecondaryIpVO ip : secondaryIps) {
                    final NicSecondaryIpResponse ipRes = new NicSecondaryIpResponse();
                    ipRes.setId(ip.getUuid());
                    ipRes.setIpAddr(ip.getIp4Address());
                    ipList.add(ipRes);
                }
                response.setSecondaryIps(ipList);
            }
        }

        response.setGateway(result.getIPv4Gateway());
        response.setNetmask(result.getIPv4Netmask());
        response.setMacAddress(result.getMacAddress());

        if (result.getIPv6Address() != null) {
            response.setIp6Address(result.getIPv6Address());
        }

        response.setIsDefault(result.isDefaultNic());
        return response;
    }

    @Override
    public AffinityGroupResponse createAffinityGroupResponse(final AffinityGroup group) {

        final AffinityGroupResponse response = new AffinityGroupResponse();

        final Account account = ApiDBUtils.findAccountById(group.getAccountId());
        response.setId(group.getUuid());
        response.setAccountName(account.getAccountName());
        response.setName(group.getName());
        response.setType(group.getType());
        response.setDescription(group.getDescription());
        final Domain domain = ApiDBUtils.findDomainById(account.getDomainId());
        if (domain != null) {
            response.setDomainId(domain.getUuid());
            response.setDomainName(domain.getName());
        }

        response.setObjectName("affinitygroup");
        return response;
    }

    @Override
    public Long getAffinityGroupId(final String groupName, final long accountId) {
        final AffinityGroup ag = ApiDBUtils.getAffinityGroup(groupName, accountId);
        if (ag == null) {
            return null;
        } else {
            return ag.getId();
        }
    }

    @Override
    public IsolationMethodResponse createIsolationMethodResponse(final IsolationType method) {
        final IsolationMethodResponse response = new IsolationMethodResponse();
        response.setIsolationMethodName(method.toString());
        response.setObjectName("isolationmethod");
        return response;
    }

    @Override
    public ListResponse<UpgradeRouterTemplateResponse> createUpgradeRouterTemplateResponse(final List<Long> jobIds) {
        final ListResponse<UpgradeRouterTemplateResponse> response = new ListResponse<>();
        final List<UpgradeRouterTemplateResponse> responses = new ArrayList<>();
        for (final Long jobId : jobIds) {
            final UpgradeRouterTemplateResponse routerResponse = new UpgradeRouterTemplateResponse();
            final AsyncJob job = this._entityMgr.findById(AsyncJob.class, jobId);
            routerResponse.setAsyncJobId(job.getUuid());
            routerResponse.setObjectName("asyncjobs");
            responses.add(routerResponse);
        }
        response.setResponses(responses);
        return response;
    }

    @Override
    public SSHKeyPairResponse createSSHKeyPairResponse(final SSHKeyPair sshkeyPair, final boolean privatekey) {
        SSHKeyPairResponse response = new SSHKeyPairResponse(sshkeyPair.getName(), sshkeyPair.getFingerprint());
        if (privatekey) {
            response = new CreateSSHKeyPairResponse(sshkeyPair.getName(), sshkeyPair.getFingerprint(), sshkeyPair.getPrivateKey());
        }
        final Account account = ApiDBUtils.findAccountById(sshkeyPair.getAccountId());
        response.setAccountName(account.getAccountName());
        final Domain domain = ApiDBUtils.findDomainById(sshkeyPair.getDomainId());
        response.setDomainId(domain.getUuid());
        response.setDomainName(domain.getName());
        return response;
    }

    // TODO: we may need to refactor once ControlledEntityResponse and
    // ControlledEntity id to uuid conversion are all done.
    // currently code is scattered in
    private void populateOwner(final ControlledEntityResponse response, final ControlledEntity object) {
        final Account account = ApiDBUtils.findAccountById(object.getAccountId());

        if (account.getType() == Account.ACCOUNT_TYPE_PROJECT) {
            // find the project
            final Project project = ApiDBUtils.findProjectByProjectAccountId(account.getId());
            if (project != null) {
                response.setProjectId(project.getUuid());
                response.setProjectName(project.getName());
            }
        } else {
            response.setAccountName(account.getAccountName());
        }

        final Domain domain = ApiDBUtils.findDomainById(object.getDomainId());
        response.setDomainId(domain.getUuid());
        response.setDomainName(domain.getName());
    }

    public static DataStoreRole getDataStoreRole(final Snapshot snapshot, final SnapshotDataStoreDao snapshotStoreDao, final DataStoreManager dataStoreMgr) {
        final SnapshotDataStoreVO snapshotStore = snapshotStoreDao.findBySnapshot(snapshot.getId(), DataStoreRole.Primary);

        if (snapshotStore == null) {
            return DataStoreRole.Image;
        }

        final long storagePoolId = snapshotStore.getDataStoreId();
        final DataStore dataStore = dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        final Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

        if (mapCapabilities != null) {
            final String value = mapCapabilities.get(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString());
            final Boolean supportsStorageSystemSnapshots = new Boolean(value);

            if (supportsStorageSystemSnapshots) {
                return DataStoreRole.Primary;
            }
        }

        return DataStoreRole.Image;
    }

    private void populateDomain(final ControlledEntityResponse response, final long domainId) {
        final Domain domain = ApiDBUtils.findDomainById(domainId);

        response.setDomainId(domain.getUuid());
        response.setDomainName(domain.getName());
    }

    private void populateAccount(final ControlledEntityResponse response, final long accountId) {
        final Account account = ApiDBUtils.findAccountById(accountId);
        if (account.getType() == Account.ACCOUNT_TYPE_PROJECT) {
            // find the project
            final Project project = ApiDBUtils.findProjectByProjectAccountId(account.getId());
            if (project != null) {
                response.setProjectId(project.getUuid());
                response.setProjectName(project.getName());
                response.setAccountName(account.getAccountName());
            }
        } else {
            response.setAccountName(account.getAccountName());
        }
    }
}
