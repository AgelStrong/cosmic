package com.cloud.storage;

import static com.cloud.storage.ScopeType.CLUSTER;
import static com.cloud.storage.ScopeType.HOST;
import static com.cloud.storage.ScopeType.ZONE;

import com.cloud.agent.AgentManager;
import com.cloud.api.ApiDBUtils;
import com.cloud.api.command.user.volume.AttachVolumeCmd;
import com.cloud.api.command.user.volume.CreateVolumeCmd;
import com.cloud.api.command.user.volume.DetachVolumeCmd;
import com.cloud.api.command.user.volume.ExtractVolumeCmd;
import com.cloud.api.command.user.volume.GetUploadParamsForVolumeCmd;
import com.cloud.api.command.user.volume.MigrateVolumeCmd;
import com.cloud.api.command.user.volume.ResizeVolumeCmd;
import com.cloud.api.command.user.volume.UploadVolumeCmd;
import com.cloud.api.query.dao.StoragePoolJoinDao;
import com.cloud.api.query.vo.StoragePoolJoinVO;
import com.cloud.api.response.GetUploadParamsResponse;
import com.cloud.capacity.CapacityManager;
import com.cloud.configuration.Config;
import com.cloud.configuration.ConfigurationManager;
import com.cloud.context.CallContext;
import com.cloud.dao.EntityManager;
import com.cloud.dao.UUIDManager;
import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.DataCenterVO;
import com.cloud.dc.dao.DataCenterDao;
import com.cloud.engine.orchestration.service.VolumeOrchestrationService;
import com.cloud.engine.subsystem.api.storage.ChapInfo;
import com.cloud.engine.subsystem.api.storage.DataObject;
import com.cloud.engine.subsystem.api.storage.DataStore;
import com.cloud.engine.subsystem.api.storage.DataStoreManager;
import com.cloud.engine.subsystem.api.storage.EndPoint;
import com.cloud.engine.subsystem.api.storage.StoragePoolAllocator;
import com.cloud.engine.subsystem.api.storage.VolumeDataFactory;
import com.cloud.engine.subsystem.api.storage.VolumeInfo;
import com.cloud.engine.subsystem.api.storage.VolumeService;
import com.cloud.engine.subsystem.api.storage.VolumeService.VolumeApiResult;
import com.cloud.event.ActionEvent;
import com.cloud.event.EventTypes;
import com.cloud.framework.async.AsyncCallFuture;
import com.cloud.framework.config.ConfigKey;
import com.cloud.framework.config.dao.ConfigurationDao;
import com.cloud.framework.jobs.AsyncJob;
import com.cloud.framework.jobs.AsyncJobExecutionContext;
import com.cloud.framework.jobs.AsyncJobManager;
import com.cloud.framework.jobs.Outcome;
import com.cloud.framework.jobs.dao.VmWorkJobDao;
import com.cloud.framework.jobs.impl.AsyncJobVO;
import com.cloud.framework.jobs.impl.OutcomeImpl;
import com.cloud.framework.jobs.impl.VmWorkJobVO;
import com.cloud.gpu.GPU;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.hypervisor.HypervisorCapabilitiesVO;
import com.cloud.hypervisor.dao.HypervisorCapabilitiesDao;
import com.cloud.jobs.JobInfo;
import com.cloud.legacymodel.communication.answer.Answer;
import com.cloud.legacymodel.communication.answer.AttachAnswer;
import com.cloud.legacymodel.communication.command.AttachCommand;
import com.cloud.legacymodel.communication.command.DettachCommand;
import com.cloud.legacymodel.communication.command.TemplateOrVolumePostUploadCommand;
import com.cloud.legacymodel.configuration.Resource.ResourceType;
import com.cloud.legacymodel.dc.DataCenter;
import com.cloud.legacymodel.dc.Host;
import com.cloud.legacymodel.domain.Domain;
import com.cloud.legacymodel.exceptions.CloudException;
import com.cloud.legacymodel.exceptions.CloudRuntimeException;
import com.cloud.legacymodel.exceptions.ConcurrentOperationException;
import com.cloud.legacymodel.exceptions.InvalidParameterValueException;
import com.cloud.legacymodel.exceptions.NoTransitionException;
import com.cloud.legacymodel.exceptions.PermissionDeniedException;
import com.cloud.legacymodel.exceptions.ResourceAllocationException;
import com.cloud.legacymodel.exceptions.StorageUnavailableException;
import com.cloud.legacymodel.statemachine.StateMachine2;
import com.cloud.legacymodel.storage.DiskOffering;
import com.cloud.legacymodel.storage.StoragePool;
import com.cloud.legacymodel.storage.TemplateType;
import com.cloud.legacymodel.storage.Upload;
import com.cloud.legacymodel.storage.VMTemplateStatus;
import com.cloud.legacymodel.storage.Volume;
import com.cloud.legacymodel.to.DataTO;
import com.cloud.legacymodel.to.DiskTO;
import com.cloud.legacymodel.user.Account;
import com.cloud.legacymodel.user.User;
import com.cloud.legacymodel.utils.Pair;
import com.cloud.legacymodel.vm.VirtualMachine;
import com.cloud.legacymodel.vm.VirtualMachine.State;
import com.cloud.model.enumeration.AllocationState;
import com.cloud.model.enumeration.DataStoreRole;
import com.cloud.model.enumeration.DiskControllerType;
import com.cloud.model.enumeration.HostType;
import com.cloud.model.enumeration.HypervisorType;
import com.cloud.model.enumeration.ImageFormat;
import com.cloud.model.enumeration.MaintenancePolicy;
import com.cloud.model.enumeration.StoragePoolStatus;
import com.cloud.model.enumeration.StorageProvisioningType;
import com.cloud.model.enumeration.VirtualMachineType;
import com.cloud.model.enumeration.VolumeType;
import com.cloud.offering.ServiceOffering;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.service.dao.ServiceOfferingDetailsDao;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.datastore.db.PrimaryDataStoreDao;
import com.cloud.storage.datastore.db.StoragePoolDetailsDao;
import com.cloud.storage.datastore.db.StoragePoolVO;
import com.cloud.storage.datastore.db.VolumeDataStoreDao;
import com.cloud.storage.datastore.db.VolumeDataStoreVO;
import com.cloud.storage.image.datastore.ImageStoreEntity;
import com.cloud.storage.snapshot.SnapshotApiService;
import com.cloud.storage.snapshot.SnapshotManager;
import com.cloud.template.TemplateManager;
import com.cloud.user.AccountManager;
import com.cloud.user.ResourceLimitService;
import com.cloud.user.VmDiskStatisticsVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.user.dao.VmDiskStatisticsDao;
import com.cloud.uservm.UserVm;
import com.cloud.utils.DateUtil;
import com.cloud.utils.EncryptionUtil;
import com.cloud.utils.EnumUtils;
import com.cloud.utils.NumbersUtil;
import com.cloud.utils.Predicate;
import com.cloud.utils.ReflectionUse;
import com.cloud.utils.StringUtils;
import com.cloud.utils.UriUtils;
import com.cloud.utils.component.ManagerBase;
import com.cloud.utils.db.DB;
import com.cloud.utils.db.Transaction;
import com.cloud.utils.db.TransactionCallback;
import com.cloud.utils.db.TransactionCallbackWithException;
import com.cloud.utils.db.TransactionStatus;
import com.cloud.utils.fsm.StateMachine2Transitions;
import com.cloud.utils.identity.ManagementServerNode;
import com.cloud.utils.imagestore.ImageStoreUtil;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.VmWork;
import com.cloud.vm.VmWorkAttachVolume;
import com.cloud.vm.VmWorkConstants;
import com.cloud.vm.VmWorkDetachVolume;
import com.cloud.vm.VmWorkExtractVolume;
import com.cloud.vm.VmWorkJobHandler;
import com.cloud.vm.VmWorkJobHandlerProxy;
import com.cloud.vm.VmWorkMigrateVolume;
import com.cloud.vm.VmWorkResizeVolume;
import com.cloud.vm.VmWorkSerializer;
import com.cloud.vm.VmWorkTakeVolumeSnapshot;
import com.cloud.vm.dao.UserVmDao;
import com.cloud.vm.dao.VMInstanceDao;
import com.cloud.vm.snapshot.VMSnapshotVO;
import com.cloud.vm.snapshot.dao.VMSnapshotDao;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolumeApiServiceImpl extends ManagerBase implements VolumeApiService, VmWorkJobHandler {
    public static final String VM_WORK_JOB_HANDLER = VolumeApiServiceImpl.class.getSimpleName();
    static final ConfigKey<Long> VmJobCheckInterval = new ConfigKey<>("Advanced", Long.class, "vm.job.check.interval", "3000",
            "Interval in milliseconds to check if the job is complete", false);
    static final ConfigKey<String> DefaultDiskControllerName = new ConfigKey<>("Advanced", String.class, "vm.default.disk.controller", "SCSI",
            "Default disk controller type for routers, systemVMs and ", false);

    private final static Logger s_logger = LoggerFactory.getLogger(VolumeApiServiceImpl.class);
    @Inject
    DataCenterDao _dcDao = null;
    private final StateMachine2<Volume.State, Volume.Event, Volume> _volStateMachine;
    @Inject
    VolumeOrchestrationService _volumeMgr;
    @Inject
    EntityManager _entityMgr;
    @Inject
    AgentManager _agentMgr;
    @Inject
    TemplateManager _tmpltMgr;
    @Inject
    SnapshotManager _snapshotMgr;
    @Inject
    AccountManager _accountMgr;
    @Inject
    ConfigurationManager _configMgr;
    @Inject
    VolumeDao _volsDao;
    @Inject
    HostDao _hostDao;
    @Inject
    SnapshotDao _snapshotDao;
    @Inject
    ServiceOfferingDetailsDao _serviceOfferingDetailsDao;
    @Inject
    ServiceOfferingDao _serviceOfferingDao;
    @Inject
    StoragePoolDetailsDao storagePoolDetailsDao;
    @Inject
    UserVmDao _userVmDao;
    @Inject
    VolumeDataStoreDao _volumeStoreDao;
    @Inject
    VMInstanceDao _vmInstanceDao;
    @Inject
    PrimaryDataStoreDao _storagePoolDao;
    @Inject
    StoragePoolJoinDao _storagePoolJoinDao;
    @Inject
    DiskOfferingDao _diskOfferingDao;
    @Inject
    AccountDao _accountDao;
    @Inject
    VMTemplateDao _templateDao;
    @Inject
    ResourceLimitService _resourceLimitMgr;
    @Inject
    VmDiskStatisticsDao _vmDiskStatsDao;
    @Inject
    VMSnapshotDao _vmSnapshotDao;
    @Inject
    ConfigurationDao _configDao;
    @Inject
    DataStoreManager dataStoreMgr;
    @Inject
    VolumeService volService;
    @Inject
    VolumeDataFactory volFactory;
    @Inject
    SnapshotApiService snapshotMgr;
    @Inject
    UUIDManager _uuidMgr;
    @Inject
    HypervisorCapabilitiesDao _hypervisorCapabilitiesDao;
    @Inject
    AsyncJobManager _jobMgr;
    @Inject
    VmWorkJobDao _workJobDao;
    @Inject
    ClusterDetailsDao _clusterDetailsDao;
    @Inject
    VolumeDao _volumeDao;
    @Inject
    CapacityManager _capacityMgr;

    VmWorkJobHandlerProxy _jobHandlerProxy = new VmWorkJobHandlerProxy(this);
    private List<StoragePoolAllocator> _storagePoolAllocators;
    private long _maxVolumeSizeInGb;

    protected VolumeApiServiceImpl() {
        this._volStateMachine = Volume.State.getStateMachine();
    }

    /*
     * Just allocate a volume in the database, don't send the createvolume cmd
     * to hypervisor. The volume will be finally created only when it's attached
     * to a VM.
     */
    @Override
    @DB
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_CREATE, eventDescription = "creating volume", create = true)
    public VolumeVO allocVolume(final CreateVolumeCmd cmd) throws ResourceAllocationException {
        // FIXME: some of the scheduled event stuff might be missing here...
        final Account caller = CallContext.current().getCallingAccount();

        final long ownerId = cmd.getEntityOwnerId();
        final Account owner = this._accountMgr.getActiveAccountById(ownerId);
        Boolean displayVolume = cmd.getDisplayVolume();

        // permission check
        this._accountMgr.checkAccess(caller, null, true, this._accountMgr.getActiveAccountById(ownerId));

        if (displayVolume == null) {
            displayVolume = true;
        } else {
            if (!this._accountMgr.isRootAdmin(caller.getId())) {
                throw new PermissionDeniedException("Cannot update parameter displayvolume, only admin permitted ");
            }
        }

        // Check that the resource limit for volumes won't be exceeded
        this._resourceLimitMgr.checkResourceLimit(owner, ResourceType.volume, displayVolume);

        Long zoneId = cmd.getZoneId();
        final Long diskOfferingId;
        final DiskOfferingVO diskOffering;
        final StorageProvisioningType provisioningType;
        Long size;
        Long minIops = null;
        Long maxIops = null;
        // Volume VO used for extracting the source template id
        VolumeVO parentVolume = null;

        // validate input parameters before creating the volume
        if (cmd.getSnapshotId() == null && cmd.getDiskOfferingId() == null || cmd.getSnapshotId() != null && cmd.getDiskOfferingId() != null) {
            throw new InvalidParameterValueException("Either disk Offering Id or snapshot Id must be passed whilst creating volume");
        }

        if (cmd.getSnapshotId() == null) {// create a new volume

            diskOfferingId = cmd.getDiskOfferingId();
            size = cmd.getSize();
            final Long sizeInGB = size;
            if (size != null) {
                if (size > 0) {
                    size = size * 1024 * 1024 * 1024; // user specify size in GB
                } else {
                    throw new InvalidParameterValueException("Disk size must be larger than 0");
                }
            }

            // Check that the the disk offering is specified
            diskOffering = this._diskOfferingDao.findById(diskOfferingId);
            if (diskOffering == null || diskOffering.getRemoved() != null || !DiskOfferingVO.Type.Disk.equals(diskOffering.getType())) {
                throw new InvalidParameterValueException("Please specify a valid disk offering.");
            }

            if (diskOffering.isCustomized()) {
                if (size == null) {
                    throw new InvalidParameterValueException("This disk offering requires a custom size specified");
                }
                final Long customDiskOfferingMaxSize = this._volumeMgr.CustomDiskOfferingMaxSize.value();
                final Long customDiskOfferingMinSize = this._volumeMgr.CustomDiskOfferingMinSize.value();

                if (sizeInGB < customDiskOfferingMinSize || sizeInGB > customDiskOfferingMaxSize) {
                    throw new InvalidParameterValueException("Volume size: " + sizeInGB + "GB is out of allowed range. Max: " + customDiskOfferingMaxSize + " Min:"
                            + customDiskOfferingMinSize);
                }
            }

            if (!diskOffering.isCustomized() && size != null) {
                throw new InvalidParameterValueException("This disk offering does not allow custom size");
            }

            if (diskOffering.getDomainId() == null) {
                // do nothing as offering is public
            } else {
                this._configMgr.checkDiskOfferingAccess(caller, diskOffering);
            }

            if (diskOffering.getDiskSize() > 0) {
                size = diskOffering.getDiskSize();
            }

            final Boolean isCustomizedIops = diskOffering.isCustomizedIops();

            if (isCustomizedIops != null) {
                if (isCustomizedIops) {
                    minIops = cmd.getMinIops();
                    maxIops = cmd.getMaxIops();

                    if (minIops == null && maxIops == null) {
                        minIops = 0L;
                        maxIops = 0L;
                    } else {
                        if (minIops == null || minIops <= 0) {
                            throw new InvalidParameterValueException("The min IOPS must be greater than 0.");
                        }

                        if (maxIops == null) {
                            maxIops = 0L;
                        }

                        if (minIops > maxIops) {
                            throw new InvalidParameterValueException("The min IOPS must be less than or equal to the max IOPS.");
                        }
                    }
                } else {
                    minIops = diskOffering.getMinIops();
                    maxIops = diskOffering.getMaxIops();
                }
            }

            provisioningType = diskOffering.getProvisioningType();

            if (!validateVolumeSizeRange(size)) {// convert size from mb to gb
                // for validation
                throw new InvalidParameterValueException("Invalid size for custom volume creation: " + size + " ,max volume size is:" + this._maxVolumeSizeInGb);
            }
        } else { // create volume from snapshot
            final Long snapshotId = cmd.getSnapshotId();
            final SnapshotVO snapshotCheck = this._snapshotDao.findById(snapshotId);
            if (snapshotCheck == null) {
                throw new InvalidParameterValueException("unable to find a snapshot with id " + snapshotId);
            }

            if (snapshotCheck.getState() != Snapshot.State.BackedUp) {
                throw new InvalidParameterValueException("Snapshot id=" + snapshotId + " is not in " + Snapshot.State.BackedUp + " state yet and can't be used for volume " +
                        "creation");
            }
            parentVolume = this._volsDao.findByIdIncludingRemoved(snapshotCheck.getVolumeId());

            diskOfferingId = snapshotCheck.getDiskOfferingId();
            diskOffering = this._diskOfferingDao.findById(diskOfferingId);
            if (zoneId == null) {
                // if zoneId is not provided, we default to create volume in the same zone as the snapshot zone.
                zoneId = snapshotCheck.getDataCenterId();
            }
            size = snapshotCheck.getSize(); // ; disk offering is used for tags
            // purposes

            minIops = snapshotCheck.getMinIops();
            maxIops = snapshotCheck.getMaxIops();

            provisioningType = diskOffering.getProvisioningType();
            // check snapshot permissions
            this._accountMgr.checkAccess(caller, null, true, snapshotCheck);

            // one step operation - create volume in VM's cluster and attach it
            // to the VM
            final Long vmId = cmd.getVirtualMachineId();
            if (vmId != null) {
                // Check that the virtual machine ID is valid and it's a user vm
                final UserVmVO vm = this._userVmDao.findById(vmId);
                if (vm == null || vm.getType() != VirtualMachineType.User) {
                    throw new InvalidParameterValueException("Please specify a valid User VM.");
                }

                // Check that the VM is in the correct state
                if (vm.getState() != State.Running && vm.getState() != State.Stopped) {
                    throw new InvalidParameterValueException("Please specify a VM that is either running or stopped.");
                }

                // permission check
                this._accountMgr.checkAccess(caller, null, false, vm);
            }
        }

        // Check that the resource limit for primary storage won't be exceeded
        this._resourceLimitMgr.checkResourceLimit(owner, ResourceType.primary_storage, displayVolume, new Long(size));

        // Verify that zone exists
        final DataCenterVO zone = this._dcDao.findById(zoneId);
        if (zone == null) {
            throw new InvalidParameterValueException("Unable to find zone by id " + zoneId);
        }

        // Check if zone is disabled
        if (AllocationState.Disabled == zone.getAllocationState() && !this._accountMgr.isRootAdmin(caller.getId())) {
            throw new PermissionDeniedException("Cannot perform this operation, Zone is currently disabled: " + zoneId);
        }

        final String userSpecifiedName = getVolumeNameFromCommand(cmd);

        DiskControllerType diskControllerType = getDiskControllerType();

        if (cmd.getDiskController() != null) {
            diskControllerType = DiskControllerType.valueOf(cmd.getDiskController().toUpperCase());
        }

        ImageFormat fileFormat = ImageFormat.QCOW2;
        if (cmd.getFileFormat() != null) {
            fileFormat = ImageFormat.valueOf(cmd.getFileFormat().toUpperCase());
        }

        return commitVolume(cmd, caller, owner, displayVolume, zoneId, diskOfferingId, provisioningType, size, minIops, maxIops, parentVolume,
                userSpecifiedName, this._uuidMgr.generateUuid(Volume.class, cmd.getCustomId()), diskControllerType, fileFormat);
    }

    private DiskControllerType getDiskControllerType() {
        DiskControllerType diskControllerType = DiskControllerType.SCSI;

        try {
            diskControllerType = DiskControllerType.valueOf(DefaultDiskControllerName.value());
        } catch (final Exception e) {
            s_logger.debug("Unable to parse vm.default.controller value '" + DefaultDiskControllerName + "' due to ", e);
        }
        return diskControllerType;
    }

    public boolean validateVolumeSizeRange(final long size) {
        if (size < 0 || size > 0 && size < 1024 * 1024 * 1024) {
            throw new InvalidParameterValueException("Please specify a size of at least 1 GB.");
        } else if (size > this._maxVolumeSizeInGb * 1024 * 1024 * 1024) {
            throw new InvalidParameterValueException("Requested volume size is " + size + ", but the maximum size allowed is " + this._maxVolumeSizeInGb + " GB.");
        }

        return true;
    }

    /**
     * Retrieves the volume name from CreateVolumeCmd object.
     * <p>
     * If the retrieved volume name is null, empty or blank, then A random name
     * will be generated using getRandomVolumeName method.
     *
     * @param cmd
     * @return Either the retrieved name or a random name.
     */
    public String getVolumeNameFromCommand(final CreateVolumeCmd cmd) {
        String userSpecifiedName = cmd.getVolumeName();

        if (org.apache.commons.lang.StringUtils.isBlank(userSpecifiedName)) {
            userSpecifiedName = getRandomVolumeName();
        }

        return userSpecifiedName;
    }

    private VolumeVO commitVolume(final CreateVolumeCmd cmd, final Account caller, final Account owner, final Boolean displayVolume, final Long zoneId, final Long diskOfferingId,
                                  final StorageProvisioningType provisioningType, final Long size, final Long minIops, final Long maxIops, final VolumeVO parentVolume,
                                  final String userSpecifiedName, final String uuid, final DiskControllerType diskController, final ImageFormat diskFormat) {
        return Transaction.execute(new TransactionCallback<VolumeVO>() {
            @Override
            public VolumeVO doInTransaction(final TransactionStatus status) {
                VolumeVO volume = new VolumeVO(userSpecifiedName, -1, -1, -1, -1, new Long(-1), null, null, provisioningType, 0, VolumeType.DATADISK, diskController);
                volume.setPoolId(null);
                volume.setUuid(uuid);
                volume.setDataCenterId(zoneId);
                volume.setPodId(null);
                volume.setAccountId(owner.getId());
                volume.setDomainId(owner.getDomainId());
                volume.setDiskOfferingId(diskOfferingId);
                volume.setSize(size);
                volume.setMinIops(minIops);
                volume.setMaxIops(maxIops);
                volume.setInstanceId(null);
                volume.setUpdated(new Date());
                volume.setDisplayVolume(displayVolume);
                if (diskFormat != null) {
                    volume.setFormat(diskFormat);
                }

                if (diskController != null) {
                    volume.setDiskController(diskController);
                }

                if (parentVolume != null) {
                    volume.setTemplateId(parentVolume.getTemplateId());
                    volume.setFormat(parentVolume.getFormat());
                } else {
                    volume.setTemplateId(null);
                }

                volume = VolumeApiServiceImpl.this._volsDao.persist(volume);

                CallContext.current().setEventDetails("Volume Id: " + volume.getId());

                // Increment resource count during allocation; if actual creation fails,
                // decrement it
                VolumeApiServiceImpl.this._resourceLimitMgr.incrementResourceCount(volume.getAccountId(), ResourceType.volume, displayVolume);
                VolumeApiServiceImpl.this._resourceLimitMgr.incrementResourceCount(volume.getAccountId(), ResourceType.primary_storage, displayVolume, new Long(volume.getSize()));
                return volume;
            }
        });
    }

    public String getRandomVolumeName() {
        return UUID.randomUUID().toString();
    }

    @Override
    @DB
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_CREATE, eventDescription = "creating volume", async = true)
    public VolumeVO createVolume(final CreateVolumeCmd cmd) {
        VolumeVO volume = this._volsDao.findById(cmd.getEntityId());
        boolean created = true;

        try {
            if (cmd.getSnapshotId() != null) {
                volume = createVolumeFromSnapshot(volume, cmd.getSnapshotId(), cmd.getVirtualMachineId());
                if (volume.getState() != Volume.State.Ready) {
                    created = false;
                }

                DiskControllerType diskController = getDiskControllerType();
                if (cmd.getDiskController() != null) {
                    diskController = DiskControllerType.valueOf(cmd.getDiskController().toUpperCase());
                }

                // if VM Id is provided, attach the volume to the VM
                if (cmd.getVirtualMachineId() != null) {
                    try {
                        attachVolumeToVM(cmd.getVirtualMachineId(), volume.getId(), volume.getDeviceId());
                    } catch (final Exception ex) {
                        final StringBuilder message = new StringBuilder("Volume: ");
                        message.append(volume.getUuid());
                        message.append(" created successfully, but failed to attach the newly created volume to VM: ");
                        message.append(cmd.getVirtualMachineId());
                        message.append(" due to error: ");
                        message.append(ex.getMessage());
                        if (s_logger.isDebugEnabled()) {
                            s_logger.debug(message.toString());
                        }
                        throw new CloudRuntimeException(message.toString());
                    }
                }
            }
            return volume;
        } catch (final Exception e) {
            created = false;
            final VolumeInfo vol = this.volFactory.getVolume(cmd.getEntityId());
            vol.stateTransit(Volume.Event.DestroyRequested);
            throw new CloudRuntimeException("Failed to create volume: " + volume.getId(), e);
        } finally {
            if (!created) {
                s_logger.trace("Decrementing volume resource count for account id=" + volume.getAccountId() + " as volume failed to create on the backend");
                this._resourceLimitMgr.decrementResourceCount(volume.getAccountId(), ResourceType.volume, cmd.getDisplayVolume());
                this._resourceLimitMgr.recalculateResourceCount(volume.getAccountId(), volume.getDomainId(), ResourceType.primary_storage.getOrdinal());
            }
        }
    }

    protected VolumeVO createVolumeFromSnapshot(final VolumeVO volume, final long snapshotId, final Long vmId) throws StorageUnavailableException {
        final VolumeInfo createdVolume;
        final SnapshotVO snapshot = this._snapshotDao.findById(snapshotId);
        snapshot.getVolumeId();

        UserVmVO vm = null;
        if (vmId != null) {
            vm = this._userVmDao.findById(vmId);
        }

        // sync old snapshots to region store if necessary

        createdVolume = this._volumeMgr.createVolumeFromSnapshot(volume, snapshot, vm);

        return this._volsDao.findById(createdVolume.getId());
    }

    public Volume attachVolumeToVM(final Long vmId, final Long volumeId, final Long deviceId) {
        final Account caller = CallContext.current().getCallingAccount();

        // Check that the volume ID is valid
        final VolumeInfo volumeToAttach = this.volFactory.getVolume(volumeId);
        // Check that the volume is a data volume
        if (volumeToAttach == null || !(volumeToAttach.getVolumeType() == VolumeType.DATADISK || volumeToAttach.getVolumeType() == VolumeType.ROOT)) {
            throw new InvalidParameterValueException("Please specify a volume with the valid type: " + VolumeType.ROOT.toString() + " or " + VolumeType.DATADISK.toString());
        }

        // Check that the volume is not currently attached to any VM
        if (volumeToAttach.getInstanceId() != null) {
            throw new InvalidParameterValueException("Please specify a volume that is not attached to any VM.");
        }

        // Check that the volume is not destroyed
        if (volumeToAttach.getState() == Volume.State.Destroy) {
            throw new InvalidParameterValueException("Please specify a volume that is not destroyed.");
        }

        // Check that the virtual machine ID is valid and it's a user vm
        final UserVmVO vm = this._userVmDao.findById(vmId);
        if (vm == null || vm.getType() != VirtualMachineType.User) {
            throw new InvalidParameterValueException("Please specify a valid User VM.");
        }

        // Check that the VM is in the correct state
        if (vm.getState() != State.Running && vm.getState() != State.Stopped) {
            throw new InvalidParameterValueException("Please specify a VM that is either running or stopped.");
        }

        // Check that the VM and the volume are in the same zone
        if (vm.getDataCenterId() != volumeToAttach.getDataCenterId()) {
            throw new InvalidParameterValueException("Please specify a VM that is in the same zone as the volume.");
        }

        // Check that the device ID is valid
        if (deviceId != null) {
            // validate ROOT volume type
            if (deviceId.longValue() == 0) {
                validateRootVolumeDetachAttach(this._volsDao.findById(volumeToAttach.getId()), vm);
                // vm shouldn't have any volume with deviceId 0
                if (!this._volsDao.findByInstanceAndDeviceId(vm.getId(), 0).isEmpty()) {
                    throw new InvalidParameterValueException("Vm already has root volume attached to it");
                }
                // volume can't be in Uploaded state
                if (volumeToAttach.getState() == Volume.State.Uploaded) {
                    throw new InvalidParameterValueException("No support for Root volume attach in state " + Volume.State.Uploaded);
                }
            }
        }

        // Check that the number of data volumes attached to VM is less than
        // that supported by hypervisor
        if (deviceId == null || deviceId.longValue() != 0) {
            final List<VolumeVO> existingDataVolumes = this._volsDao.findByInstanceAndType(vmId, VolumeType.DATADISK);
            final int maxDataVolumesSupported = getMaxDataVolumesSupported(vm);
            if (existingDataVolumes.size() >= maxDataVolumesSupported) {
                throw new InvalidParameterValueException("The specified VM already has the maximum number of data disks (" + maxDataVolumesSupported + "). Please specify another" +
                        " VM.");
            }
        }

        // if target VM has associated VM snapshots
        final List<VMSnapshotVO> vmSnapshots = this._vmSnapshotDao.findByVm(vmId);
        if (vmSnapshots.size() > 0) {
            throw new InvalidParameterValueException("Unable to attach volume, please specify a VM that does not have VM snapshots");
        }

        // permission check
        this._accountMgr.checkAccess(caller, null, true, volumeToAttach, vm);

        if (!(Volume.State.Allocated.equals(volumeToAttach.getState()) || Volume.State.Ready.equals(volumeToAttach.getState()) || Volume.State.Uploaded.equals(volumeToAttach
                .getState()))) {
            throw new InvalidParameterValueException("Volume state must be in Allocated, Ready or in Uploaded state");
        }

        final Account owner = this._accountDao.findById(volumeToAttach.getAccountId());

        if (!(volumeToAttach.getState() == Volume.State.Allocated || volumeToAttach.getState() == Volume.State.Ready)) {
            try {
                this._resourceLimitMgr.checkResourceLimit(owner, ResourceType.primary_storage, volumeToAttach.getSize());
            } catch (final ResourceAllocationException e) {
                s_logger.error("primary storage resource limit check failed", e);
                throw new InvalidParameterValueException(e.getMessage());
            }
        }

        final HypervisorType rootDiskHyperType = vm.getHypervisorType();
        final HypervisorType volumeToAttachHyperType = this._volsDao.getHypervisorType(volumeToAttach.getId());

        final StoragePoolVO volumeToAttachStoragePool = this._storagePoolDao.findById(volumeToAttach.getPoolId());

        // managed storage can be used for different types of hypervisors
        // only perform this check if the volume's storage pool is not null and not managed
        if (volumeToAttachStoragePool != null && !volumeToAttachStoragePool.isManaged()) {
            if (volumeToAttachHyperType != HypervisorType.None && rootDiskHyperType != volumeToAttachHyperType) {
                throw new InvalidParameterValueException("Can't attach a volume created by: " + volumeToAttachHyperType + " to a " + rootDiskHyperType + " vm");
            }
        }

        final AsyncJobExecutionContext asyncExecutionContext = AsyncJobExecutionContext.getCurrentExecutionContext();

        if (asyncExecutionContext != null) {
            final AsyncJob job = asyncExecutionContext.getJob();

            if (s_logger.isInfoEnabled()) {
                s_logger.info("Trying to attaching volume " + volumeId + " to vm instance:" + vm.getId() + ", update async job-" + job.getId() + " progress status");
            }

            this._jobMgr.updateAsyncJobAttachment(job.getId(), "Volume", volumeId);
        }

        final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();
        if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
            // avoid re-entrance

            final VmWorkJobVO placeHolder;
            placeHolder = createPlaceHolderWork(vmId);
            try {
                return orchestrateAttachVolumeToVM(vmId, volumeId, deviceId, volumeToAttach.getDiskController());
            } finally {
                this._workJobDao.expunge(placeHolder.getId());
            }
        } else {
            final Outcome<Volume> outcome = attachVolumeToVmThroughJobQueue(vmId, volumeId, deviceId, volumeToAttach.getDiskController());

            Volume vol = null;
            try {
                outcome.get();
            } catch (final InterruptedException e) {
                throw new RuntimeException("Operation is interrupted", e);
            } catch (final java.util.concurrent.ExecutionException e) {
                throw new RuntimeException("Execution excetion", e);
            }

            final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());
            if (jobResult != null) {
                if (jobResult instanceof ConcurrentOperationException) {
                    throw (ConcurrentOperationException) jobResult;
                } else if (jobResult instanceof InvalidParameterValueException) {
                    throw (InvalidParameterValueException) jobResult;
                } else if (jobResult instanceof RuntimeException) {
                    throw (RuntimeException) jobResult;
                } else if (jobResult instanceof Throwable) {
                    throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                } else if (jobResult instanceof Long) {
                    vol = this._volsDao.findById((Long) jobResult);
                }
            }
            return vol;
        }
    }

    private void validateRootVolumeDetachAttach(final VolumeVO volume, final UserVmVO vm) {
        if (!(vm.getHypervisorType() == HypervisorType.XenServer || vm.getHypervisorType() == HypervisorType.KVM)) {
            throw new InvalidParameterValueException("Root volume detach is not supported for hypervisor type " + vm.getHypervisorType());
        }
        if (!(vm.getState() == State.Stopped) || vm.getState() == State.Destroyed) {
            throw new InvalidParameterValueException("Root volume detach can happen only when vm is in states: " + State.Stopped.toString() + " or " + State.Destroyed.toString());
        }

        if (volume.getPoolId() != null) {
            final StoragePoolVO pool = this._storagePoolDao.findById(volume.getPoolId());
            if (pool.isManaged()) {
                throw new InvalidParameterValueException("Root volume detach is not supported for Managed DataStores");
            }
        }
    }

    private int getMaxDataVolumesSupported(final UserVmVO vm) {
        Long hostId = vm.getHostId();
        if (hostId == null) {
            hostId = vm.getLastHostId();
        }
        final HostVO host = this._hostDao.findById(hostId);
        Integer maxDataVolumesSupported = null;
        if (host != null) {
            this._hostDao.loadDetails(host);
            maxDataVolumesSupported = this._hypervisorCapabilitiesDao.getMaxDataVolumesLimit(host.getHypervisorType(), host.getDetail("product_version"));
        }
        if (maxDataVolumesSupported == null) {
            maxDataVolumesSupported = 6; // 6 data disks by default if nothing
            // is specified in
            // 'hypervisor_capabilities' table
        }

        return maxDataVolumesSupported.intValue();
    }

    private VmWorkJobVO createPlaceHolderWork(final long instanceId) {
        final VmWorkJobVO workJob = new VmWorkJobVO("");

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_PLACEHOLDER);
        workJob.setCmd("");
        workJob.setCmdInfo("");

        workJob.setAccountId(0);
        workJob.setUserId(0);
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(instanceId);
        workJob.setInitMsid(ManagementServerNode.getManagementServerId());

        this._workJobDao.persist(workJob);

        return workJob;
    }

    /**
     * Volume placement logic
     */

    private StoragePool findStoragePoolToCreateVolumeOn(final VolumeInfo volumeInfo, final UserVmVO virtualMachine) {
        final DiskOffering diskOffering = this._diskOfferingDao.findById(volumeInfo.getDiskOfferingId());

        final List<StoragePoolVO> storagePoolsInScopeWithVirtualMachine = findAllStoragePoolsInScopeWithVirtualMachine(virtualMachine);

        final List<StoragePoolVO> storagePoolVOListMatchedTags =
                checkIfStoragePoolWithMatchingTags(storagePoolsInScopeWithVirtualMachine, Arrays.asList(diskOffering.getTagsArray()));

        final List<StoragePoolVO> storagePoolVOListSufficientResources = checkIfStoragePoolHasSufficientResources(storagePoolVOListMatchedTags, volumeInfo);

        return getStoragePoolLeastResourcesUsed(storagePoolVOListSufficientResources);
    }

    private List<StoragePoolVO> findAllStoragePoolsInScopeWithVirtualMachine(final UserVmVO virtualMachine) {
        List<StoragePoolVO> storagePoolVOList;

        final StoragePoolVO existingVolume = findMostSpecificStoragePoolForVirtualMachine(virtualMachine);

        if (virtualMachine.getState().equals(State.Running)) {
            // We know the host! Easy stuff.
            final Host host = this._hostDao.findById(virtualMachine.getHostId());

            // Gather all the storage pools reachable by this host
            storagePoolVOList = listAllStoragePoolsReachableByHost(host);
        } else {
            // TODO We don't account for VM CPU / MEM capacity when selecting the storage location
            final ServiceOffering serviceOffering = this._serviceOfferingDao.findById(null, virtualMachine.getServiceOfferingId());

            // 1. Figure out on which hosts the vm can run!
            final List<HostVO> hosts = this._hostDao.listByHostTag(HostType.Routing, null, null, virtualMachine.getDataCenterId(), serviceOffering.getHostTag());

            // 2. Gather all the storage pools reachable by this host
            storagePoolVOList = hosts.stream().flatMap(hostVO -> listAllStoragePoolsReachableByHost(hostVO).stream()).distinct().collect(Collectors.toList());

            if (existingVolume != null) {
                // 3. Gather all the available pools from this volume -> inner join with previously found pools
                final List<StoragePoolVO> availableStoragePools = listStoragePoolsInScopeWithStoragePool(existingVolume);

                storagePoolVOList = storagePoolVOList.stream().flatMap(storagePoolVO -> availableStoragePools.stream().filter(storagePoolVO::equals)).collect(Collectors.toList());
            }
        }

        return storagePoolVOList;
    }

    private StoragePoolVO findMostSpecificStoragePoolForVirtualMachine(final UserVm userVm) {
        final List<VolumeVO> volumesOfVirtualMachine = this._volsDao.findByInstance(userVm.getId());

        final List<StoragePoolVO> storagePoolVOList =
                volumesOfVirtualMachine.stream().map(volumeVO -> this._storagePoolDao.findById(volumeVO.getPoolId())).filter(Objects::nonNull).collect(Collectors.toList());

        final Optional<StoragePoolVO> volumeWithStoragePoolHost = storagePoolVOList.stream().filter(storagePoolVO -> storagePoolVO.getScope().equals(HOST)).findFirst();
        if (volumeWithStoragePoolHost.isPresent()) {
            return volumeWithStoragePoolHost.get();
        }

        final Optional<StoragePoolVO> volumeWithStoragePoolCluster = storagePoolVOList.stream().filter(storagePoolVO -> storagePoolVO.getScope().equals(CLUSTER)).findFirst();
        if (volumeWithStoragePoolCluster.isPresent()) {
            return volumeWithStoragePoolCluster.get();
        }

        final Optional<StoragePoolVO> volumeWithStoragePoolZone = storagePoolVOList.stream().filter(storagePoolVO -> storagePoolVO.getScope().equals(ZONE)).findFirst();
        return volumeWithStoragePoolZone.orElse(null);
    }

    private List<StoragePoolVO> listAllStoragePoolsReachableByHost(final Host host) {
        final List<StoragePoolVO> storagePools = new ArrayList<>();

        // 1. Look for all storage pools on this host with ScopeType.HOST
        storagePools.addAll(this._storagePoolDao.listHostScopedPoolsByStorageHost(host.getName()));

        // 2. Look for all storage pools in this cluster with ScopeType.CLUSTER
        storagePools.addAll(this._storagePoolDao.listByScopeAndCluster(ScopeType.CLUSTER, host.getClusterId()));

        // 3. Look for all storage pools in this zone with ScopeType.ZONE
        storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.ZONE, host.getDataCenterId()));

        // Only return "Up" storage pools
        return storagePools.stream()
                           .filter(storagePool -> storagePool.getStatus().equals(StoragePoolStatus.Up) && !storagePool.isInMaintenance() && storagePool.getRemoved() == null)
                           .collect
                                   (Collectors.toList());
    }

    private List<StoragePoolVO> listStoragePoolsInScopeWithStoragePool(final StoragePoolVO storagePoolVO) {
        final List<StoragePoolVO> storagePools = new ArrayList<>();

        switch (storagePoolVO.getScope()) {
            case HOST:
                // 1. Look for all storage pools on this host with ScopeType.HOST
                storagePools.addAll(this._storagePoolDao.listHostScopedPoolsByStorageHost(storagePoolVO.getHostAddress()));

                // 2. Look for all storage pools in this cluster with ScopeType.CLUSTER
                storagePools.addAll(this._storagePoolDao.listByScopeAndCluster(ScopeType.CLUSTER, storagePoolVO.getClusterId()));

                // 3. Look for all storage pools in this zone with ScopeType.ZONE
                storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.ZONE, storagePoolVO.getDataCenterId()));
                break;
            case CLUSTER:
                // 1. Look for all storage pools in this cluster with ScopeType.HOST
                storagePools.addAll(this._storagePoolDao.listByScopeAndCluster(ScopeType.HOST, storagePoolVO.getClusterId()));

                // 2. Look for all storage pools in this cluster with ScopeType.CLUSTER
                storagePools.addAll(this._storagePoolDao.listByScopeAndCluster(ScopeType.CLUSTER, storagePoolVO.getClusterId()));

                // 3. Look for all storage pools in this zone with ScopeType.ZONE
                storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.ZONE, storagePoolVO.getDataCenterId()));

                break;
            case ZONE:
                // 1. Look for all storage pools in this zone with ScopeType.HOST
                storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.HOST, storagePoolVO.getDataCenterId()));

                // 2. Look for all storage pools in this zone with ScopeType.CLUSTER
                storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.CLUSTER, storagePoolVO.getDataCenterId()));

                // 3. Look for all storage pools in this zone with ScopeType.ZONE
                storagePools.addAll(this._storagePoolDao.listByScopeAndZone(ScopeType.ZONE, storagePoolVO.getDataCenterId()));

                break;
        }

        // Only return "Up" storage pools
        return storagePools.stream()
                           .filter(storagePool -> storagePool.getStatus().equals(StoragePoolStatus.Up) && !storagePool.isInMaintenance() && storagePool.getRemoved() == null)
                           .collect
                                   (Collectors.toList());
    }

    private StoragePoolVO getStoragePoolLeastResourcesUsed(final List<StoragePoolVO> storagePoolVOList) {
        return storagePoolVOList.stream().min((a, b) -> {

            // Get size for all the non-destroyed volumes
            s_logger.debug("Comparing A:" + a.getName() + " with B:" + b.getName());
            final Pair<Long, Long> sizesA = this._volumeDao.getNonDestroyedCountAndTotalByPool(a.getId());
            s_logger.debug("A has " + sizesA.first() + " volumes with size " + sizesA.second());
            final Pair<Long, Long> sizesB = this._volumeDao.getNonDestroyedCountAndTotalByPool(b.getId());
            s_logger.debug("B has " + sizesB.first() + " volumes with size " + sizesB.second());

            final Double availablePercentageA = 1 - ((double) sizesA.second() / getTotalOverProvCapacity(a));
            s_logger.debug("A has " + (availablePercentageA * 100) + " % available");

            final Double availablePercentageB = 1 - ((double) sizesB.second() / getTotalOverProvCapacity(b));
            s_logger.debug("B has " + (availablePercentageB * 100) + " % available");

            return availablePercentageB.compareTo(availablePercentageA);
        }).orElse(null);
    }

    private List<StoragePoolVO> checkIfStoragePoolWithMatchingTags(final List<StoragePoolVO> storagePoolVOList, final List<String> tags) {
        return storagePoolVOList.stream().filter(storagePoolVO -> {
            final Map<String, String> details = this._storagePoolDao.getDetails(storagePoolVO.getId());

            for (final String tag : tags) {
                if (!details.containsKey(tag)) {
                    return false;
                }
            }

            return true;
        }).collect(Collectors.toList());
    }

    private List<StoragePoolVO> checkIfStoragePoolHasSufficientResources(final List<StoragePoolVO> storagePoolVOList, final VolumeInfo volumeInfo) {
        return storagePoolVOList.stream().filter(storagePoolVO -> {

            // Get size for all the non-destroyed volumes
            final Pair<Long, Long> sizes = this._volumeDao.getNonDestroyedCountAndTotalByPool(storagePoolVO.getId());
            final long totalOverProvCapacity = getTotalOverProvCapacity(storagePoolVO);

            // Capacity left
            final long capacityLeft = totalOverProvCapacity - sizes.first();
            s_logger.debug("Storage pool " + storagePoolVO.getName() + " has capacity available of " + capacityLeft + " bytes. Volume is " + volumeInfo.getSize() + " bytes");

            return capacityLeft >= volumeInfo.getSize();
        }).collect(Collectors.toList());
    }

    private long getTotalOverProvCapacity(final StoragePoolVO storagePoolVO) {
        // Join
        final StoragePoolJoinVO storagePoolJoinVO = _storagePoolJoinDao.findById(storagePoolVO.getId());

        // Available capacity
        final BigDecimal overProvFactor = new BigDecimal(CapacityManager.StorageOverprovisioningFactor.valueIn(storagePoolVO.getId()));

        final long capacity = overProvFactor.multiply(new BigDecimal(storagePoolJoinVO.getCapacityBytes())).longValue();
        s_logger.debug("Storage pool " + storagePoolVO.getName() + " has total capacity " + capacity + " bytes (over provision factor used is " + overProvFactor + ")");

        return capacity;
    }

    private boolean canVolumeBeAttachedToVirtualMachine(final VolumeInfo volumeInfo, final UserVmVO userVmVO) {
        final StoragePoolVO storagePoolOfVolumeToAttach = this._storagePoolDao.findById(volumeInfo.getPoolId());

        final List<StoragePoolVO> storagePoolsInScopeWithVirtualMachine = findAllStoragePoolsInScopeWithVirtualMachine(userVmVO);

        return storagePoolsInScopeWithVirtualMachine.stream().filter(storagePoolOfVolumeToAttach::equals).count() == 1;
    }

    private Volume orchestrateAttachVolumeToVM(final Long vmId, final Long volumeId, final Long deviceId, final DiskControllerType diskController) {
        VolumeInfo volumeToAttach = this.volFactory.getVolume(volumeId);
        final UserVmVO virtualMachine = this._userVmDao.findById(vmId);
        final List<VolumeVO> volumesOfVirtualMachine = this._volsDao.findByInstance(vmId);

        if (volumeToAttach.isAttachedVM()) {
            throw new CloudRuntimeException("This volume is already attached to a VM.");
        }

        if (volumesOfVirtualMachine.stream().filter(volumeVO -> volumeVO.getVolumeType().equals(VolumeType.ROOT)).count() > 1) {
            throw new CloudRuntimeException("The VM " + virtualMachine.getHostName() + " has more than one ROOT volume and is in an invalid state.");
        }

        // Is the volume created on a storage pool, yet?
        if (volumeToAttach.getState().equals(Volume.State.Allocated)) {
            // Nope! Let's go and find out where we're gonna create it.
            final StoragePool storagePool = findStoragePoolToCreateVolumeOn(volumeToAttach, virtualMachine);

            if (storagePool == null) {
                throw new CloudRuntimeException("Failed to create volume on primary storage, no possible primary storage pool found!");
            }

            try {
                volumeToAttach = this._volumeMgr.createVolumeOnPrimaryStorage(virtualMachine, volumeToAttach, virtualMachine.getHypervisorType(), storagePool);
            } catch (final NoTransitionException e) {
                s_logger.debug("Failed to create volume on primary storage", e);
                throw new CloudRuntimeException("Failed to create volume on primary storage", e);
            }
        } else {
            /**
             * Check if storage pool of volume is in scope of most specific volume of virtualmachine
             */
            if (!canVolumeBeAttachedToVirtualMachine(volumeToAttach, virtualMachine)) {
                throw new CloudRuntimeException("Can't attach volume to virtualmachine, volume is not in scope of virtualmachine.");
            }
        }

        final VolumeVO newVol = this._volsDao.findById(volumeToAttach.getId());
        return sendAttachVolumeCommand(virtualMachine, newVol, deviceId, diskController);
    }

    /**
     * End volume placement logic
     */

    public Outcome<Volume> attachVolumeToVmThroughJobQueue(final Long vmId, final Long volumeId, final Long deviceId, final DiskControllerType diskController) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkAttachVolume.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkAttachVolume workInfo = new VmWorkAttachVolume(callingUser.getId(), callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId, deviceId, diskController);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        final AsyncJobVO jobVo = this._jobMgr.getAsyncJob(workJob.getId());
        s_logger.debug("New job " + workJob.getId() + ", result field: " + jobVo.getResult());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobVolumeOutcome(workJob, volumeId);
    }

    private VolumeVO sendAttachVolumeCommand(final UserVmVO vm, VolumeVO volumeToAttach, Long deviceId, final DiskControllerType diskController) {
        String errorMsg = "Failed to attach volume " + volumeToAttach.getName() + " to VM " + vm.getHostName();
        boolean sendCommand = vm.getState() == State.Running;
        AttachAnswer answer = null;
        final Long hostId = vm.getHostId();

        HostVO host = null;
        final StoragePoolVO volumeToAttachStoragePool = this._storagePoolDao.findById(volumeToAttach.getPoolId());

        if (hostId != null) {
            host = this._hostDao.findById(hostId);

            if (host != null && host.getHypervisorType() == HypervisorType.XenServer && volumeToAttachStoragePool != null && volumeToAttachStoragePool.isManaged()) {
                sendCommand = true;
            }
        }

        // volumeToAttachStoragePool should be null if the VM we are attaching the disk to has never been started before
        final DataStore dataStore = volumeToAttachStoragePool != null ? this.dataStoreMgr.getDataStore(volumeToAttachStoragePool.getId(), DataStoreRole.Primary) : null;

        // if we don't have a host, the VM we are attaching the disk to has never been started before
        if (host != null) {
            try {
                this.volService.grantAccess(this.volFactory.getVolume(volumeToAttach.getId()), host, dataStore);
            } catch (final Exception e) {
                this.volService.revokeAccess(this.volFactory.getVolume(volumeToAttach.getId()), host, dataStore);

                throw new CloudRuntimeException(e.getMessage());
            }
        }

        if (sendCommand) {
            if (host != null && host.getHypervisorType() == HypervisorType.KVM &&
                    volumeToAttachStoragePool.isManaged() &&
                    volumeToAttach.getPath() == null) {
                volumeToAttach.setPath(volumeToAttach.get_iScsiName());

                this._volsDao.update(volumeToAttach.getId(), volumeToAttach);
            }

            final DataTO volTO = this.volFactory.getVolume(volumeToAttach.getId()).getTO();

            deviceId = getDeviceId(vm, deviceId);

            if (diskController != null) {
                volumeToAttach.setDiskController(diskController);
            }

            final DiskTO disk =
                    new DiskTO(volTO, deviceId, volumeToAttach.getPath(), volumeToAttach.getVolumeType(), volumeToAttach.getDiskController(), volumeToAttach.getFormat());

            final AttachCommand cmd = new AttachCommand(disk, vm.getInstanceName());

            final ChapInfo chapInfo = this.volService.getChapInfo(this.volFactory.getVolume(volumeToAttach.getId()), dataStore);

            final Map<String, String> details = new HashMap<>();

            disk.setDetails(details);

            details.put(DiskTO.MANAGED, String.valueOf(volumeToAttachStoragePool.isManaged()));
            details.put(DiskTO.STORAGE_HOST, volumeToAttachStoragePool.getHostAddress());
            details.put(DiskTO.STORAGE_PORT, String.valueOf(volumeToAttachStoragePool.getPort()));
            details.put(DiskTO.VOLUME_SIZE, String.valueOf(volumeToAttach.getSize()));
            details.put(DiskTO.IQN, volumeToAttach.get_iScsiName());
            details.put(DiskTO.MOUNT_POINT, volumeToAttach.get_iScsiName());
            details.put(DiskTO.PROTOCOL_TYPE, volumeToAttach.getPoolType() != null ? volumeToAttach.getPoolType().toString() : null);

            if (chapInfo != null) {
                details.put(DiskTO.CHAP_INITIATOR_USERNAME, chapInfo.getInitiatorUsername());
                details.put(DiskTO.CHAP_INITIATOR_SECRET, chapInfo.getInitiatorSecret());
                details.put(DiskTO.CHAP_TARGET_USERNAME, chapInfo.getTargetUsername());
                details.put(DiskTO.CHAP_TARGET_SECRET, chapInfo.getTargetSecret());
            }
            this._userVmDao.loadDetails(vm);

            try {
                answer = (AttachAnswer) this._agentMgr.send(hostId, cmd);
            } catch (final Exception e) {
                if (host != null) {
                    this.volService.revokeAccess(this.volFactory.getVolume(volumeToAttach.getId()), host, dataStore);
                }
                throw new CloudRuntimeException(errorMsg + " due to: " + e.getMessage());
            }
        }

        if (!sendCommand || answer != null && answer.getResult()) {
            // Mark the volume as attached
            if (sendCommand) {
                final DiskTO disk = answer.getDisk();
                this._volsDao.attachVolume(volumeToAttach.getId(), vm.getId(), disk.getDiskSeq(), diskController);

                volumeToAttach = this._volsDao.findById(volumeToAttach.getId());

                if (volumeToAttachStoragePool.isManaged() && volumeToAttach.getPath() == null) {
                    volumeToAttach.setPath(answer.getDisk().getPath());

                    if (diskController != null) {
                        volumeToAttach.setDiskController(diskController);
                    }

                    this._volsDao.update(volumeToAttach.getId(), volumeToAttach);
                }
            } else {
                deviceId = getDeviceId(vm, deviceId);

                this._volsDao.attachVolume(volumeToAttach.getId(), vm.getId(), deviceId, diskController);
            }

            // insert record for disk I/O statistics
            VmDiskStatisticsVO diskstats = this._vmDiskStatsDao.findBy(vm.getAccountId(), vm.getDataCenterId(), vm.getId(), volumeToAttach.getId());
            if (diskstats == null) {
                diskstats = new VmDiskStatisticsVO(vm.getAccountId(), vm.getDataCenterId(), vm.getId(), volumeToAttach.getId());
                this._vmDiskStatsDao.persist(diskstats);
            }

            // Change MaintenancePolicy when adding disk with HOST scope
            if (volumeToAttachStoragePool != null && ScopeType.HOST.equals(volumeToAttachStoragePool.getScope())) {
                vm.setMaintenancePolicy(MaintenancePolicy.ShutdownAndStart);
                s_logger.debug("Setting MaintenancePolicy to '" + MaintenancePolicy.ShutdownAndStart.toString() + "' for VM " + vm.getInstanceName());
                _vmInstanceDao.persist(vm);
            }

            return this._volsDao.findById(volumeToAttach.getId());
        } else {
            if (answer != null) {
                final String details = answer.getDetails();
                if (details != null && !details.isEmpty()) {
                    errorMsg += "; " + details;
                }
            }
            if (host != null) {
                this.volService.revokeAccess(this.volFactory.getVolume(volumeToAttach.getId()), host, dataStore);
            }
            throw new CloudRuntimeException(errorMsg);
        }
    }

    private Long getDeviceId(final UserVmVO vm, Long deviceId) {
        // allocate deviceId
        final int maxDataVolumesSupported = getMaxDataVolumesSupported(vm);
        final List<VolumeVO> vols = this._volsDao.findByInstance(vm.getId());
        if (deviceId != null) {
            if (deviceId.longValue() > maxDataVolumesSupported || deviceId.longValue() == 3) {
                throw new RuntimeException("deviceId should be 1,2,4-" + maxDataVolumesSupported);
            }
            for (final VolumeVO vol : vols) {
                if (vol.getDeviceId().equals(deviceId)) {
                    throw new RuntimeException("deviceId " + deviceId + " is used by vm" + vm.getId());
                }
            }
        } else {
            // allocate deviceId here
            final List<String> devIds = new ArrayList<>();
            for (int i = 1; i < maxDataVolumesSupported; i++) {
                devIds.add(String.valueOf(i));
            }
            devIds.remove("3");
            for (final VolumeVO vol : vols) {
                devIds.remove(vol.getDeviceId().toString().trim());
            }
            deviceId = Long.parseLong(devIds.iterator().next());
        }

        return deviceId;
    }

    @Override
    @DB
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_RESIZE, eventDescription = "resizing volume", async = true)
    public VolumeVO resizeVolume(final ResizeVolumeCmd cmd) throws ResourceAllocationException {
        Long newSize;
        Long newMinIops;
        Long newMaxIops;
        final boolean shrinkOk = cmd.getShrinkOk();

        final VolumeVO volume = this._volsDao.findById(cmd.getEntityId());

        if (volume == null) {
            throw new InvalidParameterValueException("No such volume");
        }

        /* Does the caller have authority to act on this volume? */
        this._accountMgr.checkAccess(CallContext.current().getCallingAccount(), null, true, volume);

        if (volume.getInstanceId() != null) {
            // Check that Vm to which this volume is attached does not have VM Snapshots
            if (this._vmSnapshotDao.findByVm(volume.getInstanceId()).size() > 0) {
                throw new InvalidParameterValueException("Volume cannot be resized which is attached to VM with VM Snapshots");
            }
        }

        final DiskOfferingVO diskOffering = this._diskOfferingDao.findById(volume.getDiskOfferingId());
        DiskOfferingVO newDiskOffering = null;

        if (cmd.getNewDiskOfferingId() != null && volume.getDiskOfferingId() != cmd.getNewDiskOfferingId()) {
            newDiskOffering = this._diskOfferingDao.findById(cmd.getNewDiskOfferingId());
        }

        if (newDiskOffering != null && !newDiskOffering.isCustomized()) {
            throw new InvalidParameterValueException("The disk offering for volume " + volume.getName() + " can only be changed to an offering that supports a custom disk size.");
        }

        if (diskOffering.isCustomized() && newDiskOffering != null && !newDiskOffering.isCustomized()) {
            throw new InvalidParameterValueException("Volume " + volume.getName() + " has a custom size disk offering. Cannot change the disk offering." +
                    " Please change the size instead");
        }

        final HypervisorType hypervisorType = this._volsDao.getHypervisorType(volume.getId());

        if (hypervisorType != HypervisorType.KVM && hypervisorType != HypervisorType.XenServer && hypervisorType != HypervisorType.Any && hypervisorType != HypervisorType.None) {
            throw new InvalidParameterValueException("CloudStack currently supports volume resize only on KVM, or XenServer.");
        }

        if (volume.getState() != Volume.State.Ready && volume.getState() != Volume.State.Allocated) {
            throw new InvalidParameterValueException("Volume should be in ready or allocated state before attempting a resize. Volume " +
                    volume.getUuid() + " is in state " + volume.getState() + ".");
        }

        // if we are to use the existing disk offering
        if (newDiskOffering == null) {
            newSize = cmd.getSize();

            // if the caller is looking to change the size of the volume
            if (newSize != null) {
                if (!diskOffering.isCustomized() && !volume.getVolumeType().equals(VolumeType.ROOT)) {
                    throw new InvalidParameterValueException("To change a volume's size without providing a new disk offering, its current disk offering must be " +
                            "customizable or it must be a root volume (if providing a disk offering, make sure it is different from the current disk offering).");
                }

                // convert from bytes to GiB
                newSize = newSize << 30;
            } else {
                // no parameter provided; just use the original size of the volume
                newSize = volume.getSize();
            }

            newMinIops = cmd.getMinIops();

            if (newMinIops != null) {
                if (diskOffering.isCustomizedIops() == null || !diskOffering.isCustomizedIops()) {
                    throw new InvalidParameterValueException("The current disk offering does not support customization of the 'Min IOPS' parameter.");
                }
            } else {
                // no parameter provided; just use the original min IOPS of the volume
                newMinIops = volume.getMinIops();
            }

            newMaxIops = cmd.getMaxIops();

            if (newMaxIops != null) {
                if (diskOffering.isCustomizedIops() == null || !diskOffering.isCustomizedIops()) {
                    throw new InvalidParameterValueException("The current disk offering does not support customization of the 'Max IOPS' parameter.");
                }
            } else {
                // no parameter provided; just use the original max IOPS of the volume
                newMaxIops = volume.getMaxIops();
            }

            validateIops(newMinIops, newMaxIops);
        } else {
            if (newDiskOffering.getRemoved() != null) {
                throw new InvalidParameterValueException("Requested disk offering has been removed.");
            }

            if (!DiskOfferingVO.Type.Disk.equals(newDiskOffering.getType())) {
                throw new InvalidParameterValueException("Requested disk offering type is invalid.");
            }

            if (diskOffering.getTags() != null) {
                if (!StringUtils.areTagsEqual(diskOffering.getTags(), newDiskOffering.getTags())) {
                    throw new InvalidParameterValueException("The tags on the new and old disk offerings must match.");
                }
            } else if (newDiskOffering.getTags() != null) {
                throw new InvalidParameterValueException("There are no tags on the current disk offering. The new disk offering needs to have no tags, as well.");
            }

            if (!areIntegersEqual(diskOffering.getHypervisorSnapshotReserve(), newDiskOffering.getHypervisorSnapshotReserve())) {
                throw new InvalidParameterValueException("The hypervisor snapshot reverse on the new and old disk offerings must be equal.");
            }

            if (newDiskOffering.getDomainId() != null) {
                // not a public offering; check access
                this._configMgr.checkDiskOfferingAccess(CallContext.current().getCallingAccount(), newDiskOffering);
            }

            if (newDiskOffering.isCustomized()) {
                newSize = cmd.getSize();

                if (newSize == null) {
                    throw new InvalidParameterValueException("The new disk offering requires that a size be specified.");
                }

                // convert from bytes to GiB
                newSize = newSize << 30;
            } else {
                newSize = newDiskOffering.getDiskSize();
            }

            if (!volume.getSize().equals(newSize) && !volume.getVolumeType().equals(VolumeType.DATADISK)) {
                throw new InvalidParameterValueException("Only data volumes can be resized via a new disk offering.");
            }

            if (newDiskOffering.isCustomizedIops() != null && newDiskOffering.isCustomizedIops()) {
                newMinIops = cmd.getMinIops() != null ? cmd.getMinIops() : volume.getMinIops();
                newMaxIops = cmd.getMaxIops() != null ? cmd.getMaxIops() : volume.getMaxIops();

                validateIops(newMinIops, newMaxIops);
            } else {
                newMinIops = newDiskOffering.getMinIops();
                newMaxIops = newDiskOffering.getMaxIops();
            }
        }

        final long currentSize = volume.getSize();

        // if the caller is looking to change the size of the volume
        if (currentSize != newSize) {
            if (!validateVolumeSizeRange(newSize)) {
                throw new InvalidParameterValueException("Requested size out of range");
            }

            /*
             * Let's make certain they (think they) know what they're doing if they
             * want to shrink by forcing them to provide the shrinkok parameter.
             * This will be checked again at the hypervisor level where we can see
             * the actual disk size.
             */
            if (currentSize > newSize && !shrinkOk) {
                throw new InvalidParameterValueException("Going from existing size of " + currentSize + " to size of " + newSize + " would shrink the volume." +
                        "Need to sign off by supplying the shrinkok parameter with value of true.");
            }

            if (newSize > currentSize) {
                /* Check resource limit for this account on primary storage resource */
                this._resourceLimitMgr.checkResourceLimit(this._accountMgr.getAccount(volume.getAccountId()), ResourceType.primary_storage, volume.isDisplayVolume(),
                        new Long(newSize - currentSize).longValue());
            }
        }

        // Note: The storage plug-in in question should perform validation on the IOPS to check if a sufficient number of IOPS is available to perform
        // the requested change

        /* If this volume has never been beyond allocated state, short circuit everything and simply update the database. */
        if (volume.getState() == Volume.State.Allocated) {
            s_logger.debug("Volume is in the allocated state, but has never been created. Simply updating database with new size and IOPS.");

            volume.setSize(newSize);
            volume.setMinIops(newMinIops);
            volume.setMaxIops(newMaxIops);

            if (newDiskOffering != null) {
                volume.setDiskOfferingId(cmd.getNewDiskOfferingId());
            }

            this._volsDao.update(volume.getId(), volume);

            return volume;
        }

        final UserVmVO userVm = this._userVmDao.findById(volume.getInstanceId());

        if (userVm != null) {
            // serialize VM operation
            final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();

            if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
                // avoid re-entrance

                final VmWorkJobVO placeHolder;

                placeHolder = createPlaceHolderWork(userVm.getId());

                try {
                    return orchestrateResizeVolume(volume.getId(), currentSize, newSize, newMinIops, newMaxIops,
                            newDiskOffering != null ? cmd.getNewDiskOfferingId() : null, shrinkOk);
                } finally {
                    this._workJobDao.expunge(placeHolder.getId());
                }
            } else {
                final Outcome<Volume> outcome = resizeVolumeThroughJobQueue(userVm.getId(), volume.getId(), currentSize, newSize, newMinIops, newMaxIops,
                        newDiskOffering != null ? cmd.getNewDiskOfferingId() : null, shrinkOk);

                try {
                    outcome.get();
                } catch (final InterruptedException e) {
                    throw new RuntimeException("Operation was interrupted", e);
                } catch (final java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException("Execution exception", e);
                }

                final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());

                if (jobResult != null) {
                    if (jobResult instanceof ConcurrentOperationException) {
                        throw (ConcurrentOperationException) jobResult;
                    } else if (jobResult instanceof ResourceAllocationException) {
                        throw (ResourceAllocationException) jobResult;
                    } else if (jobResult instanceof RuntimeException) {
                        throw (RuntimeException) jobResult;
                    } else if (jobResult instanceof Throwable) {
                        throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                    } else if (jobResult instanceof Long) {
                        return this._volsDao.findById((Long) jobResult);
                    }
                }

                return volume;
            }
        }

        return orchestrateResizeVolume(volume.getId(), currentSize, newSize, newMinIops, newMaxIops,
                newDiskOffering != null ? cmd.getNewDiskOfferingId() : null, shrinkOk);
    }

    private void validateIops(final Long minIops, final Long maxIops) {
        if (minIops == null && maxIops != null || minIops != null && maxIops == null) {
            throw new InvalidParameterValueException("Either 'miniops' and 'maxiops' must both be provided or neither must be provided.");
        }

        if (minIops != null && maxIops != null) {
            if (minIops > maxIops) {
                throw new InvalidParameterValueException("The 'miniops' parameter must be less than or equal to the 'maxiops' parameter.");
            }
        }
    }

    private static boolean areIntegersEqual(Integer i1, Integer i2) {
        if (i1 == null) {
            i1 = 0;
        }

        if (i2 == null) {
            i2 = 0;
        }

        return i1.equals(i2);
    }

    private VolumeVO orchestrateResizeVolume(final long volumeId, final long currentSize, final long newSize, final Long newMinIops, final Long newMaxIops, final Long
            newDiskOfferingId, final boolean shrinkOk) {
        VolumeVO volume = this._volsDao.findById(volumeId);
        final UserVmVO userVm = this._userVmDao.findById(volume.getInstanceId());
        /*
         * get a list of hosts to send the commands to, try the system the
         * associated vm is running on first, then the last known place it ran.
         * If not attached to a userVm, we pass 'none' and the agent is ok
         * with that since it only needs the vm name to live resize
         */
        long[] hosts = null;
        String instanceName = null;
        if (userVm != null) {
            instanceName = userVm.getInstanceName();
            if (userVm.getHostId() != null) {
                hosts = new long[]{userVm.getHostId()};
            } else if (userVm.getLastHostId() != null) {
                hosts = new long[]{userVm.getLastHostId()};
            }

            final String errorMsg = "The VM must be stopped or the disk detached in order to resize with the XenServer Hypervisor.";

            final StoragePoolVO storagePool = this._storagePoolDao.findById(volume.getPoolId());

            if (storagePool.isManaged() && storagePool.getHypervisor() == HypervisorType.Any && hosts != null && hosts.length > 0) {
                final HostVO host = this._hostDao.findById(hosts[0]);

                if (currentSize != newSize && host.getHypervisorType() == HypervisorType.XenServer && !userVm.getState().equals(State.Stopped)) {
                    throw new InvalidParameterValueException(errorMsg);
                }
            }

            /* Xen only works offline, SR does not support VDI.resizeOnline */
            if (currentSize != newSize && this._volsDao.getHypervisorType(volume.getId()) == HypervisorType.XenServer && !userVm.getState().equals(State.Stopped)) {
                throw new InvalidParameterValueException(errorMsg);
            }
        }

        final ResizeVolumePayload payload = new ResizeVolumePayload(newSize, newMinIops, newMaxIops, shrinkOk, instanceName, hosts);

        try {
            final VolumeInfo vol = this.volFactory.getVolume(volume.getId());
            vol.addPayload(payload);

            final StoragePoolVO storagePool = this._storagePoolDao.findById(vol.getPoolId());

            // managed storage is designed in such a way that the storage plug-in does not
            // talk to the hypervisor layer; as such, if the storage is managed and the
            // current and new sizes are different, then CloudStack (i.e. not a storage plug-in)
            // needs to tell the hypervisor to resize the disk
            if (storagePool.isManaged() && currentSize != newSize) {
                if (hosts != null && hosts.length > 0) {
                    this.volService.resizeVolumeOnHypervisor(volumeId, newSize, hosts[0], instanceName);
                }

                volume.setSize(newSize);

                this._volsDao.update(volume.getId(), volume);
            }

            // this call to resize has a different impact depending on whether the
            // underlying primary storage is managed or not
            // if managed, this is the chance for the plug-in to change IOPS value, if applicable
            // if not managed, this is the chance for the plug-in to talk to the hypervisor layer
            // to change the size of the disk
            final AsyncCallFuture<VolumeApiResult> future = this.volService.resize(vol);
            final VolumeApiResult result = future.get();

            if (result.isFailed()) {
                s_logger.warn("Failed to resize the volume " + volume);
                String details = "";
                if (result.getResult() != null && !result.getResult().isEmpty()) {
                    details = result.getResult();
                }
                throw new CloudRuntimeException(details);
            }

            volume = this._volsDao.findById(volume.getId());

            if (newDiskOfferingId != null) {
                volume.setDiskOfferingId(newDiskOfferingId);
            }
            if (currentSize != newSize) {
                volume.setSize(newSize);
            }

            this._volsDao.update(volume.getId(), volume);

            /* Update resource count for the account on primary storage resource */
            if (!shrinkOk) {
                this._resourceLimitMgr.incrementResourceCount(volume.getAccountId(), ResourceType.primary_storage, volume.isDisplayVolume(), new Long(newSize - currentSize));
            } else {
                this._resourceLimitMgr.decrementResourceCount(volume.getAccountId(), ResourceType.primary_storage, volume.isDisplayVolume(), new Long(currentSize - newSize));
            }
            return volume;
        } catch (final InterruptedException e) {
            s_logger.warn("failed get resize volume result", e);
            throw new CloudRuntimeException(e.getMessage());
        } catch (final ExecutionException e) {
            s_logger.warn("failed get resize volume result", e);
            throw new CloudRuntimeException(e.getMessage());
        } catch (final Exception e) {
            s_logger.warn("failed get resize volume result", e);
            throw new CloudRuntimeException(e.getMessage());
        }
    }

    public Outcome<Volume> resizeVolumeThroughJobQueue(final Long vmId, final long volumeId,
                                                       final long currentSize, final long newSize, final Long newMinIops, final Long newMaxIops, final Long newServiceOfferingId,
                                                       final boolean shrinkOk) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkResizeVolume.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkResizeVolume workInfo = new VmWorkResizeVolume(callingUser.getId(), callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId, currentSize, newSize, newMinIops, newMaxIops, newServiceOfferingId, shrinkOk);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobVolumeOutcome(workJob, volumeId);
    }

    @DB
    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_MIGRATE, eventDescription = "migrating volume", async = true)
    public Volume migrateVolume(final MigrateVolumeCmd cmd) {
        final Long volumeId = cmd.getVolumeId();
        final Long storagePoolId = cmd.getStoragePoolId();

        final VolumeVO vol = this._volsDao.findById(volumeId);
        if (vol == null) {
            throw new InvalidParameterValueException("Failed to find the volume id: " + volumeId);
        }

        if (vol.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("Volume must be in ready state");
        }

        boolean liveMigrateVolume = false;
        final Long instanceId = vol.getInstanceId();
        Long srcClusterId = null;
        VMInstanceVO vm = null;
        if (instanceId != null) {
            vm = this._vmInstanceDao.findById(instanceId);
        }

        // Check that Vm to which this volume is attached does not have VM Snapshots
        if (vm != null && this._vmSnapshotDao.findByVm(vm.getId()).size() > 0) {
            throw new InvalidParameterValueException("Volume cannot be migrated, please remove all VM snapshots for VM to which this volume is attached");
        }

        if (vm != null && vm.getState() == State.Running) {
            // Check if the VM is GPU enabled.
            if (this._serviceOfferingDetailsDao.findDetail(vm.getServiceOfferingId(), GPU.Keys.pciDevice.toString()) != null) {
                throw new InvalidParameterValueException("Live Migration of GPU enabled VM is not supported");
            }

            // Check if the underlying hypervisor supports storage motion.
            final Long hostId = vm.getHostId();
            if (hostId != null) {
                final HostVO host = this._hostDao.findById(hostId);
                HypervisorCapabilitiesVO capabilities = null;
                if (host != null) {
                    capabilities = this._hypervisorCapabilitiesDao.findByHypervisorTypeAndVersion(host.getHypervisorType(), host.getHypervisorVersion());
                    srcClusterId = host.getClusterId();
                }

                if (capabilities != null) {
                    liveMigrateVolume = capabilities.isStorageMotionSupported();
                }
            }

            // If vm is running, and hypervisor doesn't support live migration, then return error
            if (!liveMigrateVolume) {
                throw new InvalidParameterValueException("Volume needs to be detached from VM");
            }
        }

        if (liveMigrateVolume && !cmd.isLiveMigrate()) {
            throw new InvalidParameterValueException("The volume " + vol + "is attached to a vm and for migrating it " + "the parameter livemigrate should be specified");
        }

        final StoragePool destPool = (StoragePool) this.dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);
        if (destPool == null) {
            throw new InvalidParameterValueException("Failed to find the destination storage pool: " + storagePoolId);
        } else if (destPool.isInMaintenance()) {
            throw new InvalidParameterValueException("Cannot migrate volume " + vol + "to the destination storage pool " + destPool.getName() +
                    " as the storage pool is in maintenance mode.");
        }

        if (vol.getPoolId() == destPool.getId()) {
            throw new InvalidParameterValueException("Cannot migrate to the same storage pool the volume is already residing on.");
        }

        if (this._volumeMgr.volumeOnSharedStoragePool(vol)) {
            if (destPool.isLocal()) {
                throw new InvalidParameterValueException("Migration of volume from shared to local storage pool is not supported");
            } else {
                // If the volume is attached to a running vm and the volume is on a shared storage pool, check
                // to make sure that the destination storage pool is in the same cluster as the vm.
                if (liveMigrateVolume && destPool.getClusterId() != null && srcClusterId != null) {
                    if (!srcClusterId.equals(destPool.getClusterId())) {
                        throw new InvalidParameterValueException("Cannot live migrate a volume of a virtual machine to a storage pool in a different cluster." +
                                "You can live migrate the virtual machine itself, rather than the volume.");
                    }
                }
            }
        } else {
            throw new InvalidParameterValueException("Migration of volume from local storage pool is not supported");
        }

        if (vm != null) {
            // serialize VM operation
            final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();
            if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
                // avoid re-entrance

                final VmWorkJobVO placeHolder;
                placeHolder = createPlaceHolderWork(vm.getId());
                try {
                    return orchestrateMigrateVolume(vol.getId(), destPool.getId(), liveMigrateVolume);
                } finally {
                    this._workJobDao.expunge(placeHolder.getId());
                }
            } else {
                final Outcome<Volume> outcome = migrateVolumeThroughJobQueue(vm.getId(), vol.getId(), destPool.getId(), liveMigrateVolume);

                try {
                    outcome.get();
                } catch (final InterruptedException e) {
                    throw new RuntimeException("Operation is interrupted", e);
                } catch (final java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException("Execution excetion", e);
                }

                final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());
                if (jobResult != null) {
                    if (jobResult instanceof ConcurrentOperationException) {
                        throw (ConcurrentOperationException) jobResult;
                    } else if (jobResult instanceof RuntimeException) {
                        throw (RuntimeException) jobResult;
                    } else if (jobResult instanceof Throwable) {
                        throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                    }
                }

                // retrieve the migrated new volume from job result
                if (jobResult != null && jobResult instanceof Long) {
                    return this._entityMgr.findById(VolumeVO.class, (Long) jobResult);
                }
                return null;
            }
        }

        return orchestrateMigrateVolume(vol.getId(), destPool.getId(), liveMigrateVolume);
    }

    /*
     * Upload the volume to secondary storage.
     */
    @Override
    @DB
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_UPLOAD, eventDescription = "uploading volume", async = true)
    public VolumeVO uploadVolume(final UploadVolumeCmd cmd) throws ResourceAllocationException {
        final Account caller = CallContext.current().getCallingAccount();
        final long ownerId = cmd.getEntityOwnerId();
        final Account owner = this._entityMgr.findById(Account.class, ownerId);
        final Long zoneId = cmd.getZoneId();
        final String volumeName = cmd.getVolumeName();
        final String url = cmd.getUrl();
        final String format = cmd.getFormat();
        final Long diskOfferingId = cmd.getDiskOfferingId();
        final String imageStoreUuid = cmd.getImageStoreUuid();
        final DataStore store = this._tmpltMgr.getImageStore(imageStoreUuid, zoneId);

        validateVolume(caller, ownerId, zoneId, volumeName, url, format, diskOfferingId);

        final VolumeVO volume = persistVolume(owner, zoneId, volumeName, url, cmd.getFormat(), diskOfferingId, Volume.State.Allocated);

        final VolumeInfo vol = this.volFactory.getVolume(volume.getId());

        final RegisterVolumePayload payload = new RegisterVolumePayload(cmd.getUrl(), cmd.getChecksum(), cmd.getFormat());
        vol.addPayload(payload);

        this.volService.registerVolume(vol, store);
        return volume;
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_UPLOAD, eventDescription = "uploading volume for post upload", async = true)
    public GetUploadParamsResponse uploadVolume(final GetUploadParamsForVolumeCmd cmd) throws ResourceAllocationException, MalformedURLException {
        final Account caller = CallContext.current().getCallingAccount();
        final long ownerId = cmd.getEntityOwnerId();
        final Account owner = this._entityMgr.findById(Account.class, ownerId);
        final Long zoneId = cmd.getZoneId();
        final String volumeName = cmd.getName();
        final String format = cmd.getFormat();
        final Long diskOfferingId = cmd.getDiskOfferingId();
        final String imageStoreUuid = cmd.getImageStoreUuid();
        final DataStore store = this._tmpltMgr.getImageStore(imageStoreUuid, zoneId);

        validateVolume(caller, ownerId, zoneId, volumeName, null, format, diskOfferingId);

        return Transaction.execute(new TransactionCallbackWithException<GetUploadParamsResponse, MalformedURLException>() {
            @Override
            public GetUploadParamsResponse doInTransaction(final TransactionStatus status) throws MalformedURLException {

                final VolumeVO volume = persistVolume(owner, zoneId, volumeName, null, cmd.getFormat(), diskOfferingId, Volume.State.NotUploaded);

                final VolumeInfo vol = VolumeApiServiceImpl.this.volFactory.getVolume(volume.getId());

                final RegisterVolumePayload payload = new RegisterVolumePayload(null, cmd.getChecksum(), cmd.getFormat());
                vol.addPayload(payload);

                final Pair<EndPoint, DataObject> pair = VolumeApiServiceImpl.this.volService.registerVolumeForPostUpload(vol, store);
                final EndPoint ep = pair.first();
                final DataObject dataObject = pair.second();

                final GetUploadParamsResponse response = new GetUploadParamsResponse();

                final String ssvmUrlDomain = VolumeApiServiceImpl.this._configDao.getValue(Config.SecStorageSecureCopyCert.key());

                final String url = ImageStoreUtil.generatePostUploadUrl(ssvmUrlDomain, ep.getPublicAddr(), vol.getUuid());
                response.setPostURL(new URL(url));

                // set the post url, this is used in the monitoring thread to determine the SSVM
                final VolumeDataStoreVO volumeStore = VolumeApiServiceImpl.this._volumeStoreDao.findByVolume(vol.getId());
                assert volumeStore != null : "sincle volume is registered, volumestore cannot be null at this stage";
                volumeStore.setExtractUrl(url);
                VolumeApiServiceImpl.this._volumeStoreDao.persist(volumeStore);

                response.setId(UUID.fromString(vol.getUuid()));

                final int timeout = ImageStoreUploadMonitorImpl.getUploadOperationTimeout();
                final DateTime currentDateTime = new DateTime(DateTimeZone.UTC);
                final String expires = currentDateTime.plusMinutes(timeout).toString();
                response.setTimeout(expires);

                final String key = VolumeApiServiceImpl.this._configDao.getValue(Config.SSVMPSK.key());
                /*
                 * encoded metadata using the post upload config key
                 */
                final TemplateOrVolumePostUploadCommand command =
                        new TemplateOrVolumePostUploadCommand(vol.getId(), vol.getUuid(), volumeStore.getInstallPath(), cmd.getChecksum(), vol.getType().toString(),
                                vol.getName(), vol.getFormat().toString(), dataObject.getDataStore().getUri(),
                                dataObject.getDataStore().getRole().toString());
                command.setLocalPath(volumeStore.getLocalDownloadPath());
                //using the existing max upload size configuration
                command.setMaxUploadSize(VolumeApiServiceImpl.this._configDao.getValue(Config.MaxUploadVolumeSize.key()));
                command.setDefaultMaxAccountSecondaryStorage(VolumeApiServiceImpl.this._configDao.getValue(Config.DefaultMaxAccountSecondaryStorage.key()));
                command.setAccountId(vol.getAccountId());
                final Gson gson = new GsonBuilder().create();
                final String metadata = EncryptionUtil.encodeData(gson.toJson(command), key);
                response.setMetadata(metadata);

                /*
                 * signature calculated on the url, expiry, metadata.
                 */
                response.setSignature(EncryptionUtil.generateSignature(metadata + url + expires, key));
                return response;
            }
        });
    }

    @Override
    @DB
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_DELETE, eventDescription = "deleting volume")
    public boolean deleteVolume(final long volumeId, final Account caller) throws ConcurrentOperationException {

        final VolumeVO volume = this._volsDao.findById(volumeId);
        if (volume == null) {
            throw new InvalidParameterValueException("Unable to find volume with ID: " + volumeId);
        }

        if (!this._snapshotMgr.canOperateOnVolume(volume)) {
            throw new InvalidParameterValueException("There are snapshot operations in progress on the volume, unable to delete it");
        }

        this._accountMgr.checkAccess(caller, null, true, volume);

        if (volume.getInstanceId() != null) {
            throw new InvalidParameterValueException("Please specify a volume that is not attached to any VM.");
        }

        if (volume.getState() == Volume.State.UploadOp) {
            final VolumeDataStoreVO volumeStore = this._volumeStoreDao.findByVolume(volume.getId());
            if (volumeStore.getDownloadState() == VMTemplateStatus.DOWNLOAD_IN_PROGRESS) {
                throw new InvalidParameterValueException("Please specify a volume that is not uploading");
            }
        }

        if (volume.getState() == Volume.State.NotUploaded || volume.getState() == Volume.State.UploadInProgress) {
            throw new InvalidParameterValueException("The volume is either getting uploaded or it may be initiated shortly, please wait for it to be completed");
        }

        try {
            if (volume.getState() != Volume.State.Destroy && volume.getState() != Volume.State.Expunging && volume.getState() != Volume.State.Expunged) {
                final Long instanceId = volume.getInstanceId();
                if (!this.volService.destroyVolume(volume.getId())) {
                    return false;
                }

                final VMInstanceVO vmInstance = this._vmInstanceDao.findById(instanceId);
                if (instanceId == null || vmInstance.getType().equals(VirtualMachineType.User)) {
                    // Decrement the resource count for volumes and primary storage belonging user VM's only
                    this._resourceLimitMgr.decrementResourceCount(volume.getAccountId(), ResourceType.volume, volume.isDisplayVolume());
                }
            }
            // Mark volume as removed if volume has not been created on primary or secondary
            if (volume.getState() == Volume.State.Allocated) {
                this._volsDao.remove(volumeId);
                stateTransitTo(volume, Volume.Event.DestroyRequested);
                return true;
            }
            // expunge volume from primary if volume is on primary
            final VolumeInfo volOnPrimary = this.volFactory.getVolume(volume.getId(), DataStoreRole.Primary);
            if (volOnPrimary != null) {
                s_logger.info("Expunging volume " + volume.getId() + " from primary data store");
                final AsyncCallFuture<VolumeApiResult> future = this.volService.expungeVolumeAsync(volOnPrimary);
                future.get();
                //decrement primary storage count
                this._resourceLimitMgr.recalculateResourceCount(volume.getAccountId(), volume.getDomainId(), ResourceType.primary_storage.getOrdinal());
            }
            // expunge volume from secondary if volume is on image store
            final VolumeInfo volOnSecondary = this.volFactory.getVolume(volume.getId(), DataStoreRole.Image);
            if (volOnSecondary != null) {
                s_logger.info("Expunging volume " + volume.getId() + " from secondary data store");
                final AsyncCallFuture<VolumeApiResult> future2 = this.volService.expungeVolumeAsync(volOnSecondary);
                future2.get();
                //decrement secondary storage count
                this._resourceLimitMgr.recalculateResourceCount(volume.getAccountId(), volume.getDomainId(), ResourceType.secondary_storage.getOrdinal());
            }
            // delete all cache entries for this volume
            final List<VolumeInfo> cacheVols = this.volFactory.listVolumeOnCache(volume.getId());
            for (final VolumeInfo volOnCache : cacheVols) {
                s_logger.info("Delete volume from image cache store: " + volOnCache.getDataStore().getName());
                volOnCache.delete();
            }
        } catch (final InterruptedException | ExecutionException | NoTransitionException e) {
            s_logger.warn("Failed to expunge volume:", e);
            return false;
        }

        return true;
    }

    private boolean stateTransitTo(final Volume vol, final Volume.Event event) throws NoTransitionException {
        return new StateMachine2Transitions(this._volStateMachine).transitTo(vol, event, null, this._volsDao);
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_ATTACH, eventDescription = "attaching volume", async = true)
    public Volume attachVolumeToVM(final AttachVolumeCmd command) {
        return attachVolumeToVM(command.getVirtualMachineId(), command.getId(), command.getDeviceId());
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_DETACH, eventDescription = "detaching volume", async = true)
    public Volume detachVolumeFromVM(final DetachVolumeCmd cmmd) {
        final Account caller = CallContext.current().getCallingAccount();
        if (cmmd.getId() == null && cmmd.getDeviceId() == null && cmmd.getVirtualMachineId() == null
                || cmmd.getId() != null && (cmmd.getDeviceId() != null || cmmd.getVirtualMachineId() != null)
                || cmmd.getId() == null && (cmmd.getDeviceId() == null || cmmd.getVirtualMachineId() == null)) {
            throw new InvalidParameterValueException("Please provide either a volume id, or a tuple(device id, instance id)");
        }

        final Long volumeId = cmmd.getId();
        final VolumeVO volume;

        if (volumeId != null) {
            volume = this._volsDao.findById(volumeId);
        } else {
            volume = this._volsDao.findByInstanceAndDeviceId(cmmd.getVirtualMachineId(), cmmd.getDeviceId()).get(0);
        }

        // Check that the volume ID is valid
        if (volume == null) {
            throw new InvalidParameterValueException("Unable to find volume with ID: " + volumeId);
        }

        final Long vmId;

        if (cmmd.getVirtualMachineId() == null) {
            vmId = volume.getInstanceId();
        } else {
            vmId = cmmd.getVirtualMachineId();
        }

        // Permissions check
        this._accountMgr.checkAccess(caller, null, true, volume);

        // Check that the volume is currently attached to a VM
        if (vmId == null) {
            throw new InvalidParameterValueException("The specified volume is not attached to a VM.");
        }

        // Check that the VM is in the correct state
        final UserVmVO vm = this._userVmDao.findById(vmId);
        if (vm.getState() != State.Running && vm.getState() != State.Stopped && vm.getState() != State.Destroyed) {
            throw new InvalidParameterValueException("Please specify a VM that is either running or stopped.");
        }

        // Check that the volume is a data/root volume
        if (!(volume.getVolumeType() == VolumeType.ROOT || volume.getVolumeType() == VolumeType.DATADISK)) {
            throw new InvalidParameterValueException("Please specify volume of type " + VolumeType.DATADISK.toString() + " or " + VolumeType.ROOT.toString());
        }

        // Root volume detach is allowed for following hypervisors: Xen/KVM/VmWare
        if (volume.getVolumeType() == VolumeType.ROOT) {
            validateRootVolumeDetachAttach(volume, vm);
        }

        // Don't allow detach if target VM has associated VM snapshots
        final List<VMSnapshotVO> vmSnapshots = this._vmSnapshotDao.findByVm(vmId);
        if (vmSnapshots.size() > 0) {
            throw new InvalidParameterValueException("Unable to detach volume, please specify a VM that does not have VM snapshots");
        }

        final AsyncJobExecutionContext asyncExecutionContext = AsyncJobExecutionContext.getCurrentExecutionContext();
        if (asyncExecutionContext != null) {
            final AsyncJob job = asyncExecutionContext.getJob();

            if (s_logger.isInfoEnabled()) {
                s_logger.info("Trying to attaching volume " + volumeId + "to vm instance:" + vm.getId() + ", update async job-" + job.getId() + " progress status");
            }

            this._jobMgr.updateAsyncJobAttachment(job.getId(), "Volume", volumeId);
        }

        final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();
        if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
            // avoid re-entrance
            final VmWorkJobVO placeHolder;
            placeHolder = createPlaceHolderWork(vmId);
            try {
                return orchestrateDetachVolumeFromVM(vmId, volumeId);
            } finally {
                this._workJobDao.expunge(placeHolder.getId());
            }
        } else {
            final Outcome<Volume> outcome = detachVolumeFromVmThroughJobQueue(vmId, volumeId);

            Volume vol = null;
            try {
                outcome.get();
            } catch (final InterruptedException e) {
                throw new RuntimeException("Operation is interrupted", e);
            } catch (final java.util.concurrent.ExecutionException e) {
                throw new RuntimeException("Execution excetion", e);
            }

            final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());
            if (jobResult != null) {
                if (jobResult instanceof ConcurrentOperationException) {
                    throw (ConcurrentOperationException) jobResult;
                } else if (jobResult instanceof RuntimeException) {
                    throw (RuntimeException) jobResult;
                } else if (jobResult instanceof Throwable) {
                    throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                } else if (jobResult instanceof Long) {
                    vol = this._volsDao.findById((Long) jobResult);
                }
            }
            return vol;
        }
    }

    private Volume orchestrateDetachVolumeFromVM(final long vmId, final long volumeId) {

        final Volume volume = this._volsDao.findById(volumeId);
        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        String errorMsg = "Failed to detach volume " + volume.getName() + " from VM " + vm.getHostName();
        boolean sendCommand = vm.getState() == State.Running;

        final Long hostId = vm.getHostId();

        HostVO host = null;
        final StoragePoolVO volumePool = this._storagePoolDao.findByIdIncludingRemoved(volume.getPoolId());

        if (hostId != null) {
            host = this._hostDao.findById(hostId);

            if (host != null && host.getHypervisorType() == HypervisorType.XenServer && volumePool != null && volumePool.isManaged()) {
                sendCommand = true;
            }
        }

        Answer answer = null;

        if (sendCommand) {
            final DataTO volTO = this.volFactory.getVolume(volume.getId()).getTO();
            final DiskTO disk = new DiskTO(volTO, volume.getDeviceId(), volume.getPath(), volume.getVolumeType(), volume.getDiskController(), volume.getFormat());

            final DettachCommand cmd = new DettachCommand(disk, vm.getInstanceName());

            cmd.setManaged(volumePool.isManaged());

            cmd.setStorageHost(volumePool.getHostAddress());
            cmd.setStoragePort(volumePool.getPort());

            cmd.set_iScsiName(volume.get_iScsiName());

            try {
                answer = this._agentMgr.send(hostId, cmd);
            } catch (final Exception e) {
                throw new CloudRuntimeException(errorMsg + " due to: " + e.getMessage());
            }
        }

        if (!sendCommand || answer != null && answer.getResult()) {
            // Mark the volume as detached
            this._volsDao.detachVolume(volume.getId());

            // volume.getPoolId() should be null if the VM we are detaching the disk from has never been started before
            final DataStore dataStore = volume.getPoolId() != null ? this.dataStoreMgr.getDataStore(volume.getPoolId(), DataStoreRole.Primary) : null;

            this.volService.revokeAccess(this.volFactory.getVolume(volume.getId()), host, dataStore);

            return this._volsDao.findById(volumeId);
        } else {

            if (answer != null) {
                final String details = answer.getDetails();
                if (details != null && !details.isEmpty()) {
                    errorMsg += "; " + details;
                }
            }

            throw new CloudRuntimeException(errorMsg);
        }
    }

    public Outcome<Volume> detachVolumeFromVmThroughJobQueue(final Long vmId, final Long volumeId) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkDetachVolume.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkDetachVolume workInfo = new VmWorkDetachVolume(callingUser.getId(), callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobVolumeOutcome(workJob, volumeId);
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_SNAPSHOT_CREATE, eventDescription = "taking snapshot", async = true)
    public Snapshot takeSnapshot(final Long volumeId, final Long policyId, final Long snapshotId, final Account account, final boolean quiescevm) throws
            ResourceAllocationException {

        final VolumeInfo volume = this.volFactory.getVolume(volumeId);
        if (volume == null) {
            throw new InvalidParameterValueException("Creating snapshot failed due to volume:" + volumeId + " doesn't exist");
        }

        if (volume.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " is not in " + Volume.State.Ready + " state but " + volume.getState() + ". Cannot take snapshot.");
        }

        VMInstanceVO vm = null;
        if (volume.getInstanceId() != null) {
            vm = this._vmInstanceDao.findById(volume.getInstanceId());
        }

        if (vm != null) {
            // serialize VM operation
            final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();
            if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
                // avoid re-entrance

                final VmWorkJobVO placeHolder;
                placeHolder = createPlaceHolderWork(vm.getId());
                try {
                    return orchestrateTakeVolumeSnapshot(volumeId, policyId, snapshotId, account, quiescevm);
                } finally {
                    this._workJobDao.expunge(placeHolder.getId());
                }
            } else {
                final Outcome<Snapshot> outcome = takeVolumeSnapshotThroughJobQueue(vm.getId(), volumeId, policyId, snapshotId, account.getId(), quiescevm);

                try {
                    outcome.get();
                } catch (final InterruptedException e) {
                    throw new RuntimeException("Operation is interrupted", e);
                } catch (final java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException("Execution excetion", e);
                }

                final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());
                if (jobResult != null) {
                    if (jobResult instanceof ConcurrentOperationException) {
                        throw (ConcurrentOperationException) jobResult;
                    } else if (jobResult instanceof ResourceAllocationException) {
                        throw (ResourceAllocationException) jobResult;
                    } else if (jobResult instanceof Throwable) {
                        throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                    }
                }

                return this._snapshotDao.findById(snapshotId);
            }
        } else {
            final CreateSnapshotPayload payload = new CreateSnapshotPayload();
            payload.setSnapshotId(snapshotId);
            payload.setAccount(account);
            payload.setQuiescevm(quiescevm);
            volume.addPayload(payload);
            return this.volService.takeSnapshot(volume);
        }
    }

    private Snapshot orchestrateTakeVolumeSnapshot(final Long volumeId, final Long policyId, final Long snapshotId, final Account account, final boolean quiescevm)
            throws ResourceAllocationException {

        final VolumeInfo volume = this.volFactory.getVolume(volumeId);

        if (volume == null) {
            throw new InvalidParameterValueException("Creating snapshot failed due to volume:" + volumeId + " doesn't exist");
        }

        if (volume.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " is not in " + Volume.State.Ready + " state but " + volume.getState() + ". Cannot take snapshot.");
        }

        final CreateSnapshotPayload payload = new CreateSnapshotPayload();
        payload.setSnapshotId(snapshotId);
        payload.setAccount(account);
        payload.setQuiescevm(quiescevm);
        volume.addPayload(payload);
        return this.volService.takeSnapshot(volume);
    }

    public Outcome<Snapshot> takeVolumeSnapshotThroughJobQueue(final Long vmId, final Long volumeId,
                                                               final Long policyId, final Long snapshotId, final Long accountId, final boolean quiesceVm) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkTakeVolumeSnapshot.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkTakeVolumeSnapshot workInfo = new VmWorkTakeVolumeSnapshot(
                callingUser.getId(), accountId != null ? accountId : callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId, policyId, snapshotId, quiesceVm);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobSnapshotOutcome(workJob, snapshotId);
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_SNAPSHOT_CREATE, eventDescription = "allocating snapshot", create = true)
    public Snapshot allocSnapshot(final Long volumeId, final Long policyId, final String snapshotName) throws ResourceAllocationException {
        final Account caller = CallContext.current().getCallingAccount();

        final VolumeInfo volume = this.volFactory.getVolume(volumeId);
        if (volume == null) {
            throw new InvalidParameterValueException("Creating snapshot failed due to volume:" + volumeId + " doesn't exist");
        }
        final DataCenter zone = this._dcDao.findById(volume.getDataCenterId());
        if (zone == null) {
            throw new InvalidParameterValueException("Can't find zone by id " + volume.getDataCenterId());
        }

        if (volume.getInstanceId() != null) {
            // Check that Vm to which this volume is attached does not have VM Snapshots
            if (this._vmSnapshotDao.findByVm(volume.getInstanceId()).size() > 0) {
                throw new InvalidParameterValueException("Volume snapshot is not allowed, please detach it from VM with VM Snapshots");
            }
        }

        if (AllocationState.Disabled == zone.getAllocationState() && !this._accountMgr.isRootAdmin(caller.getId())) {
            throw new PermissionDeniedException("Cannot perform this operation, Zone is currently disabled: " + zone.getName());
        }

        if (volume.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " is not in " + Volume.State.Ready + " state but " + volume.getState() + ". Cannot take snapshot.");
        }

        if (ImageFormat.DIR.equals(volume.getFormat())) {
            throw new InvalidParameterValueException("Snapshot not supported for volume:" + volumeId);
        }

        if (volume.getTemplateId() != null) {
            final VMTemplateVO template = this._templateDao.findById(volume.getTemplateId());
            if (template != null && template.getTemplateType() == TemplateType.SYSTEM) {
                throw new InvalidParameterValueException("VolumeId: " + volumeId + " is for System VM , Creating snapshot against System VM volumes is not supported");
            }
        }

        final StoragePool storagePool = (StoragePool) volume.getDataStore();
        if (storagePool == null) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " please attach this volume to a VM before create snapshot for it");
        }

        return this.snapshotMgr.allocSnapshot(volumeId, policyId, snapshotName, false);
    }

    @Override
    public Snapshot allocSnapshotForVm(final Long vmId, final Long volumeId, final String snapshotName) throws ResourceAllocationException {
        final Account caller = CallContext.current().getCallingAccount();
        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);
        if (vm == null) {
            throw new InvalidParameterValueException("Creating snapshot failed due to vm:" + vmId + " doesn't exist");
        }
        this._accountMgr.checkAccess(caller, null, true, vm);

        final VolumeInfo volume = this.volFactory.getVolume(volumeId);
        if (volume == null) {
            throw new InvalidParameterValueException("Creating snapshot failed due to volume:" + volumeId + " doesn't exist");
        }
        this._accountMgr.checkAccess(caller, null, true, volume);
        final VirtualMachine attachVM = volume.getAttachedVM();
        if (attachVM == null || attachVM.getId() != vm.getId()) {
            throw new InvalidParameterValueException("Creating snapshot failed due to volume:" + volumeId + " doesn't attach to vm :" + vm);
        }

        final DataCenter zone = this._dcDao.findById(volume.getDataCenterId());
        if (zone == null) {
            throw new InvalidParameterValueException("Can't find zone by id " + volume.getDataCenterId());
        }

        if (AllocationState.Disabled == zone.getAllocationState() && !this._accountMgr.isRootAdmin(caller.getId())) {
            throw new PermissionDeniedException("Cannot perform this operation, Zone is currently disabled: " + zone.getName());
        }

        if (volume.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " is not in " + Volume.State.Ready + " state but " + volume.getState() + ". Cannot take snapshot.");
        }

        if (volume.getTemplateId() != null) {
            final VMTemplateVO template = this._templateDao.findById(volume.getTemplateId());
            if (template != null && template.getTemplateType() == TemplateType.SYSTEM) {
                throw new InvalidParameterValueException("VolumeId: " + volumeId + " is for System VM , Creating snapshot against System VM volumes is not supported");
            }
        }

        final StoragePool storagePool = (StoragePool) volume.getDataStore();
        if (storagePool == null) {
            throw new InvalidParameterValueException("VolumeId: " + volumeId + " please attach this volume to a VM before create snapshot for it");
        }

        return this.snapshotMgr.allocSnapshot(volumeId, Snapshot.MANUAL_POLICY_ID, snapshotName, true);
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_UPDATE, eventDescription = "updating volume", async = true)
    public Volume updateVolume(final long volumeId, final String path, final String state, final Long storageId, final Boolean displayVolume, final String customId, final long
            entityOwnerId, final String chainInfo, final DiskControllerType diskControllerType) {

        final VolumeVO volume = this._volsDao.findById(volumeId);

        if (volume.getInstanceId() != null) {
            final VMInstanceVO vmInstanceVO = this._vmInstanceDao.findById(volume.getInstanceId());

            if (vmInstanceVO.getState() != State.Stopped) {
                throw new CloudRuntimeException("Volume is attached to a running VM, unable to update volume at this point.");
            }
        }

        if (volume == null) {
            throw new InvalidParameterValueException("The volume id doesn't exist");
        }

        if (path != null) {
            volume.setPath(path);
        }

        if (chainInfo != null) {
            volume.setChainInfo(chainInfo);
        }

        if (diskControllerType != null) {
            volume.setDiskController(diskControllerType);
        }

        if (state != null) {
            try {
                final Volume.State volumeState = Volume.State.valueOf(state);
                volume.setState(volumeState);
            } catch (final IllegalArgumentException ex) {
                throw new InvalidParameterValueException("Invalid volume state specified");
            }
        }

        if (storageId != null) {
            final StoragePool pool = this._storagePoolDao.findById(storageId);
            if (pool.getDataCenterId() != volume.getDataCenterId()) {
                throw new InvalidParameterValueException("Invalid storageId specified; refers to the pool outside of the volume's zone");
            }
            volume.setPoolId(pool.getId());
        }

        if (customId != null) {
            volume.setUuid(customId);
        }

        updateDisplay(volume, displayVolume);

        this._volsDao.update(volumeId, volume);

        return volume;
    }

    private void updateResourceCount(final Volume volume, final Boolean displayVolume) {
        // Update only when the flag has changed.
        if (displayVolume != null && displayVolume != volume.isDisplayVolume()) {
            this._resourceLimitMgr.changeResourceCount(volume.getAccountId(), ResourceType.volume, displayVolume);
            this._resourceLimitMgr.changeResourceCount(volume.getAccountId(), ResourceType.primary_storage, displayVolume, new Long(volume.getSize()));
        }
    }

    private boolean isVolumeDestroyed(final Volume volume) {
        if (volume.getState() == Volume.State.Destroy || volume.getState() == Volume.State.Expunging && volume.getState() == Volume.State.Expunged) {
            return true;
        }
        return false;
    }

    @Override
    @ActionEvent(eventType = EventTypes.EVENT_VOLUME_EXTRACT, eventDescription = "extracting volume", async = true)
    public String extractVolume(final ExtractVolumeCmd cmd) {
        final Long volumeId = cmd.getId();
        final Long zoneId = cmd.getZoneId();
        final String mode = cmd.getMode();
        final Account account = CallContext.current().getCallingAccount();

        if (!this._accountMgr.isRootAdmin(account.getId()) && ApiDBUtils.isExtractionDisabled()) {
            throw new PermissionDeniedException("Extraction has been disabled by admin");
        }

        final VolumeVO volume = this._volsDao.findById(volumeId);
        if (volume == null) {
            final InvalidParameterValueException ex = new InvalidParameterValueException("Unable to find volume with specified volumeId");
            ex.addProxyObject(volumeId.toString(), "volumeId");
            throw ex;
        }

        // perform permission check
        this._accountMgr.checkAccess(account, null, true, volume);

        if (this._dcDao.findById(zoneId) == null) {
            throw new InvalidParameterValueException("Please specify a valid zone.");
        }
        if (volume.getPoolId() == null) {
            throw new InvalidParameterValueException("The volume doesnt belong to a storage pool so cant extract it");
        }
        // Extract activity only for detached volumes or for volumes whose
        // instance is stopped
        if (volume.getInstanceId() != null && ApiDBUtils.findVMInstanceById(volume.getInstanceId()).getState() != State.Stopped) {
            s_logger.debug("Invalid state of the volume with ID: " + volumeId + ". It should be either detached or the VM should be in stopped state.");
            final PermissionDeniedException ex = new PermissionDeniedException(
                    "Invalid state of the volume with specified ID. It should be either detached or the VM should be in stopped state.");
            ex.addProxyObject(volume.getUuid(), "volumeId");
            throw ex;
        }

        if (volume.getVolumeType() != VolumeType.DATADISK) {
            // Datadisk dont have any template dependence.

            final VMTemplateVO template = ApiDBUtils.findTemplateById(volume.getTemplateId());
            if (template != null) { // For ISO based volumes template = null and
                // we allow extraction of all ISO based
                // volumes
                final boolean isExtractable = template.isExtractable() && template.getTemplateType() != TemplateType.SYSTEM;
                if (!isExtractable && account != null && !this._accountMgr.isRootAdmin(account.getId())) {
                    // Global admins are always allowed to extract
                    final PermissionDeniedException ex = new PermissionDeniedException("The volume with specified volumeId is not allowed to be extracted");
                    ex.addProxyObject(volume.getUuid(), "volumeId");
                    throw ex;
                }
            }
        }

        if (mode == null || !mode.equals(Upload.Mode.FTP_UPLOAD.toString()) && !mode.equals(Upload.Mode.HTTP_DOWNLOAD.toString())) {
            throw new InvalidParameterValueException("Please specify a valid extract Mode ");
        }

        // Check if the url already exists
        final VolumeDataStoreVO volumeStoreRef = this._volumeStoreDao.findByVolume(volumeId);
        if (volumeStoreRef != null && volumeStoreRef.getExtractUrl() != null) {
            return volumeStoreRef.getExtractUrl();
        }

        VMInstanceVO vm = null;
        if (volume.getInstanceId() != null) {
            vm = this._vmInstanceDao.findById(volume.getInstanceId());
        }

        if (vm != null) {
            // serialize VM operation
            final AsyncJobExecutionContext jobContext = AsyncJobExecutionContext.getCurrentExecutionContext();
            if (jobContext.isJobDispatchedBy(VmWorkConstants.VM_WORK_JOB_DISPATCHER)) {
                // avoid re-entrance

                final VmWorkJobVO placeHolder;
                placeHolder = createPlaceHolderWork(vm.getId());
                try {
                    return orchestrateExtractVolume(volume.getId(), zoneId);
                } finally {
                    this._workJobDao.expunge(placeHolder.getId());
                }
            } else {
                final Outcome<String> outcome = extractVolumeThroughJobQueue(vm.getId(), volume.getId(), zoneId);

                try {
                    outcome.get();
                } catch (final InterruptedException e) {
                    throw new RuntimeException("Operation is interrupted", e);
                } catch (final java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException("Execution excetion", e);
                }

                final Object jobResult = this._jobMgr.unmarshallResultObject(outcome.getJob());
                if (jobResult != null) {
                    if (jobResult instanceof ConcurrentOperationException) {
                        throw (ConcurrentOperationException) jobResult;
                    } else if (jobResult instanceof RuntimeException) {
                        throw (RuntimeException) jobResult;
                    } else if (jobResult instanceof Throwable) {
                        throw new RuntimeException("Unexpected exception", (Throwable) jobResult);
                    }
                }

                // retrieve the entity url from job result
                if (jobResult != null && jobResult instanceof String) {
                    return (String) jobResult;
                }
                return null;
            }
        }

        return orchestrateExtractVolume(volume.getId(), zoneId);
    }

    private String orchestrateExtractVolume(final long volumeId, final long zoneId) {
        // get latest volume state to make sure that it is not updated by other parallel operations
        final VolumeVO volume = this._volsDao.findById(volumeId);
        if (volume == null || volume.getState() != Volume.State.Ready) {
            throw new InvalidParameterValueException("Volume to be extracted has been removed or not in right state!");
        }
        // perform extraction
        final ImageStoreEntity secStore = (ImageStoreEntity) this.dataStoreMgr.getImageStore(zoneId);
        final String value = this._configDao.getValue(Config.CopyVolumeWait.toString());
        NumbersUtil.parseInt(value, Integer.parseInt(Config.CopyVolumeWait.getDefaultValue()));

        // Copy volume from primary to secondary storage
        final VolumeInfo srcVol = this.volFactory.getVolume(volumeId);
        final AsyncCallFuture<VolumeApiResult> cvAnswer = this.volService.copyVolume(srcVol, secStore);
        // Check if you got a valid answer.
        final VolumeApiResult cvResult;
        try {
            cvResult = cvAnswer.get();
        } catch (final InterruptedException e1) {
            s_logger.debug("failed copy volume", e1);
            throw new CloudRuntimeException("Failed to copy volume", e1);
        } catch (final ExecutionException e1) {
            s_logger.debug("failed copy volume", e1);
            throw new CloudRuntimeException("Failed to copy volume", e1);
        }
        if (cvResult == null || cvResult.isFailed()) {
            final String errorString = "Failed to copy the volume from the source primary storage pool to secondary storage.";
            throw new CloudRuntimeException(errorString);
        }

        final VolumeInfo vol = cvResult.getVolume();

        final String extractUrl = secStore.createEntityExtractUrl(vol.getPath(), vol.getFormat(), vol);
        final VolumeDataStoreVO volumeStoreRef = this._volumeStoreDao.findByVolume(volumeId);
        volumeStoreRef.setExtractUrl(extractUrl);
        volumeStoreRef.setExtractUrlCreated(DateUtil.now());
        this._volumeStoreDao.update(volumeStoreRef.getId(), volumeStoreRef);
        return extractUrl;
    }

    public Outcome<String> extractVolumeThroughJobQueue(final Long vmId, final long volumeId,
                                                        final long zoneId) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkExtractVolume.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkExtractVolume workInfo = new VmWorkExtractVolume(callingUser.getId(), callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId, zoneId);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobVolumeUrlOutcome(workJob);
    }

    @Override
    public boolean isDisplayResourceEnabled(final Long id) {
        final Volume volume = this._volsDao.findById(id);
        if (volume == null) {
            return true; // bad id given, default to true
        }
        return volume.isDisplayVolume();
    }

    @Override
    public void updateDisplay(final Volume volume, final Boolean displayVolume) {
        // 1. Resource limit changes
        updateResourceCount(volume, displayVolume);

        // 2. Set the flag
        if (displayVolume != null && displayVolume != volume.isDisplayVolume()) {
            // FIXME - Confused - typecast for now.
            ((VolumeVO) volume).setDisplayVolume(displayVolume);
            this._volsDao.update(volume.getId(), (VolumeVO) volume);
        }
    }

    private boolean validateVolume(final Account caller, final long ownerId, final Long zoneId, final String volumeName, final String url,
                                   final String format, final Long diskOfferingId) throws ResourceAllocationException {

        // permission check
        final Account volumeOwner = this._accountMgr.getActiveAccountById(ownerId);
        this._accountMgr.checkAccess(caller, null, true, volumeOwner);

        // Check that the resource limit for volumes won't be exceeded
        this._resourceLimitMgr.checkResourceLimit(volumeOwner, ResourceType.volume);

        // Verify that zone exists
        final DataCenterVO zone = this._dcDao.findById(zoneId);
        if (zone == null) {
            throw new InvalidParameterValueException("Unable to find zone by id " + zoneId);
        }

        // Check if zone is disabled
        if (AllocationState.Disabled == zone.getAllocationState() && !this._accountMgr.isRootAdmin(caller.getId())) {
            throw new PermissionDeniedException("Cannot perform this operation, Zone is currently disabled: " + zoneId);
        }

        //validating the url only when url is not null. url can be null incase of form based post upload
        if (url != null) {
            if (url.toLowerCase().contains("file://")) {
                throw new InvalidParameterValueException("File:// type urls are currently unsupported");
            }
            UriUtils.validateUrl(format, url);
            // check URL existence
            UriUtils.checkUrlExistence(url);
            // Check that the resource limit for secondary storage won't be exceeded
            this._resourceLimitMgr.checkResourceLimit(this._accountMgr.getAccount(ownerId), ResourceType.secondary_storage, UriUtils.getRemoteSize(url));
        } else {
            this._resourceLimitMgr.checkResourceLimit(this._accountMgr.getAccount(ownerId), ResourceType.secondary_storage);
        }

        try {
            ImageFormat.valueOf(format.toUpperCase());
        } catch (final IllegalArgumentException e) {
            s_logger.debug("ImageFormat IllegalArgumentException: " + e.getMessage());
            throw new IllegalArgumentException("Image format: " + format + " is incorrect. Supported formats are " + EnumUtils.listValues(ImageFormat.values()));
        }

        // Check that the the disk offering specified is valid
        if (diskOfferingId != null) {
            final DiskOfferingVO diskOffering = this._diskOfferingDao.findById(diskOfferingId);
            if (diskOffering == null || diskOffering.getRemoved() != null
                    || !DiskOfferingVO.Type.Disk.equals(diskOffering.getType())) {
                throw new InvalidParameterValueException("Please specify a valid disk offering.");
            }
            if (!diskOffering.isCustomized()) {
                throw new InvalidParameterValueException("Please specify a custom sized disk offering.");
            }

            if (diskOffering.getDomainId() == null) {
                // do nothing as offering is public
            } else {
                this._configMgr.checkDiskOfferingAccess(volumeOwner, diskOffering);
            }
        }

        return false;
    }

    @DB
    protected VolumeVO persistVolume(final Account owner, final Long zoneId, final String volumeName, final String url,
                                     final String format, final Long diskOfferingId, final Volume.State state) {
        return Transaction.execute(new TransactionCallback<VolumeVO>() {
            @Override
            public VolumeVO doInTransaction(final TransactionStatus status) {
                VolumeVO volume =
                        new VolumeVO(volumeName, zoneId, -1, -1, -1, new Long(-1), null, null, StorageProvisioningType.THIN, 0, VolumeType.DATADISK, getDiskControllerType());
                volume.setPoolId(null);
                volume.setDataCenterId(zoneId);
                volume.setPodId(null);
                volume.setState(state); // initialize the state
                // to prevent a null pointer deref I put the system account id here when no owner is given.
                // TODO Decide if this is valid or whether  throwing a CloudRuntimeException is more appropriate
                volume.setAccountId(owner == null ? Account.ACCOUNT_ID_SYSTEM : owner.getAccountId());
                volume.setDomainId(owner == null ? Domain.ROOT_DOMAIN : owner.getDomainId());

                if (diskOfferingId == null) {
                    final DiskOfferingVO diskOfferingVO = VolumeApiServiceImpl.this._diskOfferingDao.findByUniqueName("Cloud.com-Custom");
                    if (diskOfferingVO != null) {
                        final long defaultDiskOfferingId = diskOfferingVO.getId();
                        volume.setDiskOfferingId(defaultDiskOfferingId);
                    }
                } else {
                    volume.setDiskOfferingId(diskOfferingId);
                }
                // volume.setSize(size);
                volume.setInstanceId(null);
                volume.setUpdated(new Date());
                volume.setDomainId(owner == null ? Domain.ROOT_DOMAIN : owner.getDomainId());
                volume.setFormat(ImageFormat.valueOf(format));
                volume = VolumeApiServiceImpl.this._volsDao.persist(volume);
                CallContext.current().setEventDetails("Volume Id: " + volume.getId());

                // Increment resource count during allocation; if actual creation fails,
                // decrement it
                VolumeApiServiceImpl.this._resourceLimitMgr.incrementResourceCount(volume.getAccountId(), ResourceType.volume);
                //url can be null incase of postupload
                if (url != null) {
                    VolumeApiServiceImpl.this._resourceLimitMgr.incrementResourceCount(volume.getAccountId(), ResourceType.secondary_storage, UriUtils.getRemoteSize(url));
                }

                return volume;
            }
        });
    }

    private Volume orchestrateMigrateVolume(final long volumeId, final long destPoolId, final boolean liveMigrateVolume) {
        final VolumeVO vol = this._volsDao.findById(volumeId);
        assert vol != null;
        final StoragePool destPool = (StoragePool) this.dataStoreMgr.getDataStore(destPoolId, DataStoreRole.Primary);
        assert destPool != null;

        final Volume newVol;
        try {
            if (liveMigrateVolume) {
                newVol = liveMigrateVolume(vol, destPool);
            } else {
                newVol = this._volumeMgr.migrateVolume(vol, destPool);
            }
        } catch (final StorageUnavailableException e) {
            s_logger.debug("Failed to migrate volume", e);
            throw new CloudRuntimeException(e.getMessage());
        } catch (final Exception e) {
            s_logger.debug("Failed to migrate volume", e);
            throw new CloudRuntimeException(e.getMessage());
        }
        return newVol;
    }

    public Outcome<Volume> migrateVolumeThroughJobQueue(final Long vmId, final long volumeId,
                                                        final long destPoolId, final boolean liveMigrate) {

        final CallContext context = CallContext.current();
        final User callingUser = context.getCallingUser();
        final Account callingAccount = context.getCallingAccount();

        final VMInstanceVO vm = this._vmInstanceDao.findById(vmId);

        final VmWorkJobVO workJob = new VmWorkJobVO(context.getContextId());

        workJob.setDispatcher(VmWorkConstants.VM_WORK_JOB_DISPATCHER);
        workJob.setCmd(VmWorkMigrateVolume.class.getName());

        workJob.setAccountId(callingAccount.getId());
        workJob.setUserId(callingUser.getId());
        workJob.setStep(VmWorkJobVO.Step.Starting);
        workJob.setVmType(VirtualMachineType.Instance);
        workJob.setVmInstanceId(vm.getId());
        workJob.setRelated(AsyncJobExecutionContext.getOriginJobId());

        // save work context info (there are some duplications)
        final VmWorkMigrateVolume workInfo = new VmWorkMigrateVolume(callingUser.getId(), callingAccount.getId(), vm.getId(),
                VolumeApiServiceImpl.VM_WORK_JOB_HANDLER, volumeId, destPoolId, liveMigrate);
        workJob.setCmdInfo(VmWorkSerializer.serialize(workInfo));

        this._jobMgr.submitAsyncJob(workJob, VmWorkConstants.VM_WORK_QUEUE, vm.getId());

        AsyncJobExecutionContext.getCurrentExecutionContext().joinJob(workJob.getId());

        return new VmJobVolumeOutcome(workJob, volumeId);
    }

    @DB
    protected Volume liveMigrateVolume(final Volume volume, final StoragePool destPool) throws StorageUnavailableException {
        final VolumeInfo vol = this.volFactory.getVolume(volume.getId(), true);
        final AsyncCallFuture<VolumeApiResult> future = this.volService.migrateVolume(vol, (DataStore) destPool);
        try {
            final VolumeApiResult result = future.get();
            if (result.isFailed()) {
                s_logger.debug("Live migrate volume failed:" + result.getResult());
                throw new StorageUnavailableException("Live migrate volume failed: " + result.getResult(), destPool.getId());
            }
            return result.getVolume();
        } catch (final InterruptedException e) {
            s_logger.debug("Live migrate volume failed", e);
            throw new CloudRuntimeException(e.getMessage());
        } catch (final ExecutionException e) {
            s_logger.debug("Live migrate volume failed", e);
            throw new CloudRuntimeException(e.getMessage());
        }
    }

    private String getFormatForPool(final StoragePool pool) {
        final ClusterVO cluster = ApiDBUtils.findClusterById(pool.getClusterId());

        if (cluster.getHypervisorType() == HypervisorType.XenServer) {
            return "vhd";
        } else if (cluster.getHypervisorType() == HypervisorType.KVM) {
            return "qcow2";
        } else {
            return null;
        }
    }

    @Override
    public boolean configure(final String name, final Map<String, Object> params) {

        final String maxVolumeSizeInGbString = this._configDao.getValue(Config.MaxVolumeSize.toString());
        this._maxVolumeSizeInGb = NumbersUtil.parseLong(maxVolumeSizeInGbString, 2000);
        return true;
    }

    public List<StoragePoolAllocator> getStoragePoolAllocators() {
        return this._storagePoolAllocators;
    }

    @Inject
    public void setStoragePoolAllocators(final List<StoragePoolAllocator> storagePoolAllocators) {
        this._storagePoolAllocators = storagePoolAllocators;
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateExtractVolume(final VmWorkExtractVolume work) throws Exception {
        final String volUrl = orchestrateExtractVolume(work.getVolumeId(), work.getZoneId());
        return new Pair<>(JobInfo.Status.SUCCEEDED, this._jobMgr.marshallResultObject(volUrl));
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateAttachVolumeToVM(final VmWorkAttachVolume work) throws Exception {
        final Volume vol = orchestrateAttachVolumeToVM(work.getVmId(), work.getVolumeId(), work.getDeviceId(), work.getDiskController());

        return new Pair<>(JobInfo.Status.SUCCEEDED,
                this._jobMgr.marshallResultObject(new Long(vol.getId())));
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateDetachVolumeFromVM(final VmWorkDetachVolume work) throws Exception {
        final Volume vol = orchestrateDetachVolumeFromVM(work.getVmId(), work.getVolumeId());
        return new Pair<>(JobInfo.Status.SUCCEEDED,
                this._jobMgr.marshallResultObject(new Long(vol.getId())));
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateResizeVolume(final VmWorkResizeVolume work) throws Exception {
        final Volume vol = orchestrateResizeVolume(work.getVolumeId(), work.getCurrentSize(), work.getNewSize(), work.getNewMinIops(), work.getNewMaxIops(),
                work.getNewServiceOfferingId(), work.isShrinkOk());
        return new Pair<>(JobInfo.Status.SUCCEEDED,
                this._jobMgr.marshallResultObject(new Long(vol.getId())));
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateMigrateVolume(final VmWorkMigrateVolume work) throws Exception {
        final Volume newVol = orchestrateMigrateVolume(work.getVolumeId(), work.getDestPoolId(), work.isLiveMigrate());
        return new Pair<>(JobInfo.Status.SUCCEEDED,
                this._jobMgr.marshallResultObject(new Long(newVol.getId())));
    }

    @ReflectionUse
    private Pair<JobInfo.Status, String> orchestrateTakeVolumeSnapshot(final VmWorkTakeVolumeSnapshot work) throws Exception {
        final Account account = this._accountDao.findById(work.getAccountId());
        orchestrateTakeVolumeSnapshot(work.getVolumeId(), work.getPolicyId(), work.getSnapshotId(),
                account, work.isQuiesceVm());
        return new Pair<>(JobInfo.Status.SUCCEEDED,
                this._jobMgr.marshallResultObject(work.getSnapshotId()));
    }

    @Override
    public Pair<JobInfo.Status, String> handleVmWorkJob(final VmWork work) throws CloudException {
        return this._jobHandlerProxy.handleVmWorkJob(work);
    }

    public class VmJobVolumeUrlOutcome extends OutcomeImpl<String> {

        public VmJobVolumeUrlOutcome(final AsyncJob job) {
            super(String.class, job, VmJobCheckInterval.value(), new Predicate() {
                @Override
                public boolean checkCondition() {
                    final AsyncJobVO jobVo = VolumeApiServiceImpl.this._entityMgr.findById(AsyncJobVO.class, job.getId());
                    assert jobVo != null;
                    if (jobVo == null || jobVo.getStatus() != JobInfo.Status.IN_PROGRESS) {
                        return true;
                    }

                    return false;
                }
            }, AsyncJob.Topics.JOB_STATE);
        }
    }

    public class VmJobVolumeOutcome extends OutcomeImpl<Volume> {
        private final long _volumeId;

        public VmJobVolumeOutcome(final AsyncJob job, final long volumeId) {
            super(Volume.class, job, VmJobCheckInterval.value(), new Predicate() {
                @Override
                public boolean checkCondition() {
                    final AsyncJobVO jobVo = VolumeApiServiceImpl.this._entityMgr.findById(AsyncJobVO.class, job.getId());
                    assert jobVo != null;
                    if (jobVo == null || jobVo.getStatus() != JobInfo.Status.IN_PROGRESS) {
                        return true;
                    }

                    return false;
                }
            }, AsyncJob.Topics.JOB_STATE);
            this._volumeId = volumeId;
        }

        @Override
        protected Volume retrieve() {
            return VolumeApiServiceImpl.this._volsDao.findById(this._volumeId);
        }
    }

    public class VmJobSnapshotOutcome extends OutcomeImpl<Snapshot> {
        private final long _snapshotId;

        public VmJobSnapshotOutcome(final AsyncJob job, final long snapshotId) {
            super(Snapshot.class, job, VmJobCheckInterval.value(), new Predicate() {
                @Override
                public boolean checkCondition() {
                    final AsyncJobVO jobVo = VolumeApiServiceImpl.this._entityMgr.findById(AsyncJobVO.class, job.getId());
                    assert jobVo != null;
                    if (jobVo == null || jobVo.getStatus() != JobInfo.Status.IN_PROGRESS) {
                        return true;
                    }

                    return false;
                }
            }, AsyncJob.Topics.JOB_STATE);
            this._snapshotId = snapshotId;
        }

        @Override
        protected Snapshot retrieve() {
            return VolumeApiServiceImpl.this._snapshotDao.findById(this._snapshotId);
        }
    }
}
