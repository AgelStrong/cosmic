package com.cloud.vpc;

import com.cloud.configuration.ConfigurationManager;
import com.cloud.context.CallContext;
import com.cloud.dao.EntityManager;
import com.cloud.engine.orchestration.service.NetworkOrchestrationService;
import com.cloud.framework.messagebus.MessageBus;
import com.cloud.legacymodel.network.Network;
import com.cloud.legacymodel.network.vpc.NetworkACLItem;
import com.cloud.legacymodel.network.vpc.NetworkACLItem.State;
import com.cloud.legacymodel.network.vpc.PrivateGateway;
import com.cloud.legacymodel.network.vpc.VpcGateway;
import com.cloud.legacymodel.user.Account;
import com.cloud.legacymodel.user.User;
import com.cloud.network.NetworkModel;
import com.cloud.network.dao.IPAddressDao;
import com.cloud.network.dao.NetworkDao;
import com.cloud.network.dao.NetworkVO;
import com.cloud.network.element.NetworkACLServiceProvider;
import com.cloud.network.vpc.NetworkACLItemDao;
import com.cloud.network.vpc.NetworkACLItemVO;
import com.cloud.network.vpc.NetworkACLManager;
import com.cloud.network.vpc.NetworkACLManagerImpl;
import com.cloud.network.vpc.NetworkACLVO;
import com.cloud.network.vpc.VpcGatewayVO;
import com.cloud.network.vpc.VpcManager;
import com.cloud.network.vpc.VpcService;
import com.cloud.network.vpc.dao.NetworkACLDao;
import com.cloud.network.vpc.dao.VpcGatewayDao;
import com.cloud.tags.dao.ResourceTagDao;
import com.cloud.test.utils.SpringUtils;
import com.cloud.user.AccountManager;
import com.cloud.user.AccountVO;
import com.cloud.user.UserVO;
import com.cloud.utils.component.ComponentContext;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class NetworkACLManagerTest extends TestCase {
    @Inject
    NetworkACLManager _aclMgr;

    @Inject
    IPAddressDao _ipAddressDao;
    @Inject
    AccountManager _accountMgr;
    @Inject
    VpcManager _vpcMgr;
    @Inject
    NetworkACLDao _networkACLDao;
    @Inject
    NetworkACLItemDao _networkACLItemDao;
    @Inject
    NetworkDao _networkDao;
    @Inject
    ConfigurationManager _configMgr;
    @Inject
    EntityManager _entityMgr;
    @Inject
    NetworkModel _networkModel;
    @Inject
    List<NetworkACLServiceProvider> _networkAclElements;
    @Inject
    VpcService _vpcSvc;
    @Inject
    VpcGatewayDao _vpcGatewayDao;

    private NetworkACLVO acl;
    private NetworkACLItemVO aclItem;

    @Override
    @Before
    public void setUp() {
        ComponentContext.initComponentsLifeCycle();
        final Account account = new AccountVO("testaccount", 1, "testdomain", (short) 0, UUID.randomUUID().toString());
        final UserVO user = new UserVO(1, "testuser", "password", "firstname", "lastName", "email", "timezone", UUID.randomUUID().toString(), User.Source.UNKNOWN);

        CallContext.register(user, account);
        acl = Mockito.mock(NetworkACLVO.class);
        aclItem = Mockito.mock(NetworkACLItemVO.class);
    }

    @Override
    @After
    public void tearDown() {
        CallContext.unregister();
    }

    @Test
    public void testCreateACL() throws Exception {
        Mockito.when(_networkACLDao.persist(Matchers.any(NetworkACLVO.class))).thenReturn(acl);
        assertNotNull(_aclMgr.createNetworkACL("acl_new", "acl desc", 1L, true));
    }

    @Test
    public void testApplyACL() throws Exception {
        final NetworkVO network = Mockito.mock(NetworkVO.class);
        Mockito.when(_networkDao.findById(Matchers.anyLong())).thenReturn(network);
        Mockito.when(_networkModel.isProviderSupportServiceInNetwork(Matchers.anyLong(), Matchers.any(Network.Service.class), Matchers.any(Network.Provider.class)))
               .thenReturn(true);
        Mockito.when(_networkAclElements.get(0).applyNetworkACLs(Matchers.any(Network.class), Matchers.anyList())).thenReturn(true);
        assertTrue(_aclMgr.applyACLToNetwork(1L));
    }

    @Test
    public void testApplyNetworkACL() throws Exception {
        driveTestApplyNetworkACL(true, true, true);
        driveTestApplyNetworkACL(false, false, true);
        driveTestApplyNetworkACL(false, true, false);
    }

    public void driveTestApplyNetworkACL(final boolean result, final boolean applyNetworkACLs, final boolean applyACLToPrivateGw) throws Exception {
        // In order to test ONLY our scope method, we mock the others
        final NetworkACLManager aclManager = Mockito.spy(_aclMgr);

        // Prepare
        // Reset mocked objects to reuse
        Mockito.reset(_networkACLItemDao);

        // Make sure it is handled
        final long aclId = 1L;
        final NetworkVO network = Mockito.mock(NetworkVO.class);
        final List<NetworkVO> networks = new ArrayList<>();
        networks.add(network);
        Mockito.when(_networkDao.listByAclId(Matchers.anyLong()))
               .thenReturn(networks);
        Mockito.when(_networkDao.findById(Matchers.anyLong())).thenReturn(network);
        Mockito.when(_networkModel.isProviderSupportServiceInNetwork(Matchers.anyLong(),
                Matchers.any(Network.Service.class), Matchers.any(Network.Provider.class)))
               .thenReturn(true);
        Mockito.when(_networkAclElements.get(0).applyNetworkACLs(Matchers.any(Network.class),
                Matchers.anyList())).thenReturn(applyNetworkACLs);

        // Make sure it applies ACL to private gateway
        final List<VpcGatewayVO> vpcGateways = new ArrayList<>();
        final VpcGatewayVO vpcGateway = Mockito.mock(VpcGatewayVO.class);
        final PrivateGateway privateGateway = Mockito.mock(PrivateGateway.class);
        Mockito.when(_vpcSvc.getVpcPrivateGateway(Mockito.anyLong())).thenReturn(privateGateway);
        vpcGateways.add(vpcGateway);
        Mockito.when(_vpcGatewayDao.listByAclIdAndType(aclId, VpcGateway.Type.Private))
               .thenReturn(vpcGateways);

        // Create 4 rules to test all 4 scenarios: only revoke should
        // be deleted, only add should update
        final List<NetworkACLItemVO> rules = new ArrayList<>();
        final NetworkACLItemVO ruleActive = Mockito.mock(NetworkACLItemVO.class);
        final NetworkACLItemVO ruleStaged = Mockito.mock(NetworkACLItemVO.class);
        final NetworkACLItemVO rule2Revoke = Mockito.mock(NetworkACLItemVO.class);
        final NetworkACLItemVO rule2Add = Mockito.mock(NetworkACLItemVO.class);
        Mockito.when(ruleActive.getState()).thenReturn(NetworkACLItem.State.Active);
        Mockito.when(ruleStaged.getState()).thenReturn(NetworkACLItem.State.Staged);
        Mockito.when(rule2Add.getState()).thenReturn(NetworkACLItem.State.Add);
        Mockito.when(rule2Revoke.getState()).thenReturn(NetworkACLItem.State.Revoke);
        rules.add(ruleActive);
        rules.add(ruleStaged);
        rules.add(rule2Add);
        rules.add(rule2Revoke);

        final long revokeId = 8;
        Mockito.when(rule2Revoke.getId()).thenReturn(revokeId);

        final long addId = 9;
        Mockito.when(rule2Add.getId()).thenReturn(addId);
        Mockito.when(_networkACLItemDao.findById(addId)).thenReturn(rule2Add);

        Mockito.when(_networkACLItemDao.listByACL(aclId))
               .thenReturn(rules);
        // Mock methods to avoid
        Mockito.doReturn(applyACLToPrivateGw).when(aclManager).applyACLToPrivateGw(privateGateway);

        // Execute
        assertEquals("Result was not congruent with applyNetworkACLs and applyACLToPrivateGw", result, aclManager.applyNetworkACL(aclId));

        // Assert if conditions met, network ACL was applied
        final int timesProcessingDone = applyNetworkACLs && applyACLToPrivateGw ? 1 : 0;
        Mockito.verify(_networkACLItemDao, Mockito.times(timesProcessingDone)).remove(revokeId);
        Mockito.verify(rule2Add, Mockito.times(timesProcessingDone)).setState(NetworkACLItem.State.Active);
        Mockito.verify(_networkACLItemDao, Mockito.times(timesProcessingDone)).update(addId, rule2Add);
    }

    @Test
    public void testRevokeACLItem() throws Exception {
        Mockito.when(_networkACLItemDao.findById(Matchers.anyLong())).thenReturn(aclItem);
        assertTrue(_aclMgr.revokeNetworkACLItem(1L));
    }

    @Test
    public void testUpdateACLItem() throws Exception {
        Mockito.when(_networkACLItemDao.findById(Matchers.anyLong())).thenReturn(aclItem);
        Mockito.when(_networkACLItemDao.update(Matchers.anyLong(), Matchers.any(NetworkACLItemVO.class))).thenReturn(true);
        assertNotNull(_aclMgr.updateNetworkACLItem(1L, "UDP", null, NetworkACLItem.TrafficType.Ingress, "Deny", 10, 22, 32, null, null, null, true));
    }

    @Test
    public void deleteNonEmptyACL() throws Exception {
        final List<NetworkACLItemVO> aclItems = new ArrayList<>();
        aclItems.add(aclItem);
        Mockito.when(_networkACLItemDao.listByACL(Matchers.anyLong())).thenReturn(aclItems);
        Mockito.when(acl.getId()).thenReturn(3l);
        Mockito.when(_networkACLItemDao.findById(Matchers.anyLong())).thenReturn(aclItem);
        Mockito.when(aclItem.getState()).thenReturn(State.Add);
        Mockito.when(aclItem.getId()).thenReturn(3l);
        Mockito.when(_networkACLDao.remove(Matchers.anyLong())).thenReturn(true);

        final boolean result = _aclMgr.deleteNetworkACL(acl);

        Mockito.verify(aclItem, Mockito.times(4)).getState();

        assertTrue("Operation should be successfull!", result);
    }

    @Configuration
    @ComponentScan(basePackageClasses = {NetworkACLManagerImpl.class}, includeFilters = {@ComponentScan.Filter(value = NetworkACLTestConfiguration.Library.class,
            type = FilterType.CUSTOM)}, useDefaultFilters = false)
    public static class NetworkACLTestConfiguration extends SpringUtils.CloudStackTestConfiguration {

        @Bean
        public AccountManager accountManager() {
            return Mockito.mock(AccountManager.class);
        }

        @Bean
        public NetworkOrchestrationService networkManager() {
            return Mockito.mock(NetworkOrchestrationService.class);
        }

        @Bean
        public IPAddressDao ipAddressDao() {
            return Mockito.mock(IPAddressDao.class);
        }

        @Bean
        public NetworkModel networkModel() {
            return Mockito.mock(NetworkModel.class);
        }

        @Bean
        public VpcManager vpcManager() {
            return Mockito.mock(VpcManager.class);
        }

        @Bean
        public EntityManager entityManager() {
            return Mockito.mock(EntityManager.class);
        }

        @Bean
        public ResourceTagDao resourceTagDao() {
            return Mockito.mock(ResourceTagDao.class);
        }

        @Bean
        public NetworkACLDao networkACLDao() {
            return Mockito.mock(NetworkACLDao.class);
        }

        @Bean
        public NetworkACLItemDao networkACLItemDao() {
            return Mockito.mock(NetworkACLItemDao.class);
        }

        @Bean
        public NetworkDao networkDao() {
            return Mockito.mock(NetworkDao.class);
        }

        @Bean
        public ConfigurationManager configMgr() {
            return Mockito.mock(ConfigurationManager.class);
        }

        @Bean
        public NetworkACLServiceProvider networkElements() {
            return Mockito.mock(NetworkACLServiceProvider.class);
        }

        @Bean
        public VpcGatewayDao vpcGatewayDao() {
            return Mockito.mock(VpcGatewayDao.class);
        }

        @Bean
        public VpcService vpcService() {
            return Mockito.mock(VpcService.class);
        }

        @Bean
        public MessageBus messageBus() {
            return Mockito.mock(MessageBus.class);
        }

        public static class Library implements TypeFilter {
            @Override
            public boolean match(final MetadataReader mdr, final MetadataReaderFactory arg1) throws IOException {
                mdr.getClassMetadata().getClassName();
                final ComponentScan cs = NetworkACLTestConfiguration.class.getAnnotation(ComponentScan.class);
                return SpringUtils.includedInBasePackageClasses(mdr.getClassMetadata().getClassName(), cs);
            }
        }
    }
}
