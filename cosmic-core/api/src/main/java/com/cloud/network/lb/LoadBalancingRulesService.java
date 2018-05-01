package com.cloud.network.lb;

import com.cloud.api.command.user.loadbalancer.CreateLBHealthCheckPolicyCmd;
import com.cloud.api.command.user.loadbalancer.CreateLBStickinessPolicyCmd;
import com.cloud.api.command.user.loadbalancer.ListLBHealthCheckPoliciesCmd;
import com.cloud.api.command.user.loadbalancer.ListLBStickinessPoliciesCmd;
import com.cloud.api.command.user.loadbalancer.ListLoadBalancerRuleInstancesCmd;
import com.cloud.api.command.user.loadbalancer.ListLoadBalancerRulesCmd;
import com.cloud.api.command.user.loadbalancer.UpdateLoadBalancerRuleCmd;
import com.cloud.legacymodel.exceptions.InsufficientAddressCapacityException;
import com.cloud.legacymodel.exceptions.NetworkRuleConflictException;
import com.cloud.legacymodel.exceptions.ResourceUnavailableException;
import com.cloud.legacymodel.utils.Pair;
import com.cloud.network.rules.HealthCheckPolicy;
import com.cloud.network.rules.LoadBalancer;
import com.cloud.network.rules.LoadBalancerContainer.Scheme;
import com.cloud.network.rules.StickinessPolicy;
import com.cloud.uservm.UserVm;
import com.cloud.utils.net.Ip;

import java.util.List;
import java.util.Map;

public interface LoadBalancingRulesService {
    /**
     * Create a load balancer rule from the given ipAddress/port to the given private port
     *
     * @param openFirewall TODO
     * @param forDisplay   TODO
     * @param cmd          the command specifying the ip address, public port, protocol, private port, and algorithm
     * @return the newly created LoadBalancerVO if successful, null otherwise
     * @throws InsufficientAddressCapacityException
     */
    LoadBalancer createPublicLoadBalancerRule(String xId, String name, String description, int srcPortStart, int srcPortEnd, int defPortStart, int defPortEnd,
                                              Long ipAddrId, String protocol, String algorithm, long networkId, long lbOwnerId, boolean openFirewall, String lbProtocol,
                                              Boolean forDisplay, Integer clientTimeout, Integer serverTimeout) throws NetworkRuleConflictException,
            InsufficientAddressCapacityException;

    LoadBalancer updateLoadBalancerRule(UpdateLoadBalancerRuleCmd cmd);

    boolean deleteLoadBalancerRule(long lbRuleId, boolean apply);

    /**
     * Create a stickiness policy to a load balancer from the given stickiness method name and parameters in
     * (name,value) pairs.
     *
     * @param cmd the command specifying the stickiness method name, params (name,value pairs), policy name and
     *            description.
     * @return the newly created stickiness policy if successfull, null otherwise
     * @thows NetworkRuleConflictException
     */
    public StickinessPolicy createLBStickinessPolicy(CreateLBStickinessPolicyCmd cmd) throws NetworkRuleConflictException;

    public boolean applyLBStickinessPolicy(CreateLBStickinessPolicyCmd cmd) throws ResourceUnavailableException;

    boolean deleteLBStickinessPolicy(long stickinessPolicyId, boolean apply);

    /**
     * Create a healthcheck policy to a load balancer from the given healthcheck
     * parameters in (name,value) pairs.
     *
     * @param cmd the command specifying the stickiness method name, params
     *            (name,value pairs), policy name and description.
     * @return the newly created stickiness policy if successfull, null
     * otherwise
     * @thows NetworkRuleConflictException
     */
    public HealthCheckPolicy createLBHealthCheckPolicy(CreateLBHealthCheckPolicyCmd cmd);

    public boolean applyLBHealthCheckPolicy(CreateLBHealthCheckPolicyCmd cmd) throws ResourceUnavailableException;

    boolean deleteLBHealthCheckPolicy(long healthCheckPolicyId, boolean apply);

    /**
     * Assign a virtual machine or list of virtual machines, or Map of <vmId vmIp> to a load balancer.
     */
    boolean assignToLoadBalancer(long lbRuleId, List<Long> vmIds, Map<Long, List<String>> vmIdIpMap);

    boolean assignSSLCertToLoadBalancerRule(Long lbRuleId, String certName, String publicCert, String privateKey);

    boolean removeFromLoadBalancer(long lbRuleId, List<Long> vmIds, Map<Long, List<String>> vmIdIpMap);

    boolean applyLoadBalancerConfig(long lbRuleId) throws ResourceUnavailableException;

    boolean assignCertToLoadBalancer(long lbRuleId, Long certId);

    boolean removeCertFromLoadBalancer(long lbRuleId);

    /**
     * List instances that have either been applied to a load balancer or are eligible to be assigned to a load
     * balancer.
     *
     * @param cmd
     * @return list of vm instances that have been or can be applied to a load balancer along with service state,
     * if the LB has health check policy created on it from cloudstack.
     */
    Pair<List<? extends UserVm>, List<String>> listLoadBalancerInstances(ListLoadBalancerRuleInstancesCmd cmd);

    /**
     * List load balancer rules based on the given criteria
     *
     * @param cmd the command that specifies the criteria to use for listing load balancers. Load balancers can be
     *            listed
     *            by id, name, public ip, and vm instance id
     * @return list of load balancers that match the criteria
     */
    Pair<List<? extends LoadBalancer>, Integer> searchForLoadBalancers(ListLoadBalancerRulesCmd cmd);

    /**
     * List stickiness policies based on the given criteria
     *
     * @param cmd the command specifies the load balancing rule id.
     * @return list of stickiness policies that match the criteria.
     */
    List<? extends StickinessPolicy> searchForLBStickinessPolicies(ListLBStickinessPoliciesCmd cmd);

    /**
     * List healthcheck policies based on the given criteria
     *
     * @param cmd the command specifies the load balancing rule id.
     * @return list of healthcheck policies that match the criteria.
     */

    List<? extends HealthCheckPolicy> searchForLBHealthCheckPolicies(ListLBHealthCheckPoliciesCmd cmd);

    LoadBalancer findById(long loadBalancer);

    public void updateLBHealthChecks(Scheme scheme) throws ResourceUnavailableException;

    Map<Ip, UserVm> getLbInstances(long lbId);

    boolean isLbRuleMappedToVmGuestIp(String vmSecondaryIp);

    List<String> listLbVmIpAddress(long id, long vmId);

    StickinessPolicy updateLBStickinessPolicy(long id, String customId, Boolean forDisplay);

    HealthCheckPolicy updateLBHealthCheckPolicy(long id, String customId, Boolean forDisplay);

    LoadBalancer findLbByStickinessId(long stickinessPolicyId);

    Long findLBIdByHealtCheckPolicyId(long lbHealthCheckPolicy);
}
