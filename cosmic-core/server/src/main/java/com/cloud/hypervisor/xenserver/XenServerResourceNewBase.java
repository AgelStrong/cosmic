package com.cloud.hypervisor.xenserver;

import com.cloud.hypervisor.xenserver.resource.XenServer620SP1Resource;
import com.cloud.legacymodel.communication.command.startup.StartupCommand;
import com.cloud.legacymodel.exceptions.CloudRuntimeException;
import com.cloud.legacymodel.utils.Pair;
import com.cloud.legacymodel.vm.VirtualMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Event;
import com.xensource.xenapi.EventBatch;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Pool;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.Types.XenAPIException;
import com.xensource.xenapi.VM;
import org.apache.xmlrpc.XmlRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XenServerResourceNewBase is an abstract base class that encapsulates how
 * CloudStack should interact with XenServer after a special XenServer
 * 6.2 hotfix.  From here on, every Resource for future versions of
 * XenServer should use this as the base class.  This base class lessens
 * the amount of load CloudStack places on Xapi because it doesn't use
 * polling as a means to collect data and figure out task completion.
 * <p>
 * This base class differs from CitrixResourceBase in the following ways:
 * - VM states are detected using Event.from instead of polling.  This
 * increases the number of threads CloudStack uses but the threads
 * are mostly idle just waiting for events from XenServer.
 * - stats are collected through the http interface rather than Xapi plugin.
 * This change may be promoted to CitrixResourceBase as it's also possible
 * in previous versions of XenServer.
 * - Asynchronous task completion is done throught Event.from rather than
 * polling.
 */
public class XenServerResourceNewBase extends XenServer620SP1Resource {
    private static final Logger s_logger = LoggerFactory.getLogger(XenServerResourceNewBase.class);
    protected VmEventListener _listener = null;

    @Override
    public StartupCommand[] initialize() throws IllegalArgumentException {
        final StartupCommand[] cmds = super.initialize();

        final Connection conn = getConnection();
        final Pool pool;
        try {
            pool = Pool.getByUuid(conn, _host.getPool());
            final Pool.Record poolr = pool.getRecord(conn);

            final Host.Record masterRecord = poolr.master.getRecord(conn);
            if (_host.getUuid().equals(masterRecord.uuid)) {
                _listener = new VmEventListener(true);

                //
                // TODO disable event listener for now. Wait until everything else is ready
                //

                // _listener.start();
            } else {
                _listener = new VmEventListener(false);
            }
        } catch (final XenAPIException e) {
            throw new CloudRuntimeException("Unable to determine who is the master", e);
        } catch (final XmlRpcException e) {
            throw new CloudRuntimeException("Unable to determine who is the master", e);
        }
        return cmds;
    }

    protected void waitForTask2(final Connection c, final Task task, final long pollInterval, final long timeout) throws XenAPIException, XmlRpcException, TimeoutException {
        final long beginTime = System.currentTimeMillis();
        if (s_logger.isTraceEnabled()) {
            s_logger.trace("Task " + task.getNameLabel(c) + " (" + task.getType(c) + ") sent to " + c.getSessionReference() + " is pending completion with a " + timeout +
                    "ms timeout");
        }
        final Set<String> classes = new HashSet<>();
        classes.add("Task/" + task.toWireString());
        String token = "";
        final Double t = new Double(timeout / 1000);
        while (true) {
            final EventBatch map = Event.from(c, classes, token, t);
            token = map.token;
            final Set<Event.Record> events = map.events;
            if (events.size() == 0) {
                final String msg = "No event for task " + task.toWireString();
                s_logger.warn(msg);
                task.cancel(c);
                throw new TimeoutException(msg);
            }
            for (final Event.Record rec : events) {
                if (!(rec.snapshot instanceof Task.Record)) {
                    if (s_logger.isDebugEnabled()) {
                        s_logger.debug("Skipping over " + rec);
                    }
                    continue;
                }

                final Task.Record taskRecord = (Task.Record) rec.snapshot;

                if (taskRecord.status != Types.TaskStatusType.PENDING) {
                    if (s_logger.isDebugEnabled()) {
                        s_logger.debug("Task, ref:" + task.toWireString() + ", UUID:" + taskRecord.uuid + " is done " + taskRecord.status);
                    }
                    return;
                } else {
                    if (s_logger.isDebugEnabled()) {
                        s_logger.debug("Task: ref:" + task.toWireString() + ", UUID:" + taskRecord.uuid + " progress: " + taskRecord.progress);
                    }
                }
            }
            if (System.currentTimeMillis() - beginTime > timeout) {
                final String msg = "Async " + timeout / 1000 + " seconds timeout for task " + task.toString();
                s_logger.warn(msg);
                task.cancel(c);
                throw new TimeoutException(msg);
            }
        }
    }

    protected class VmEventListener extends Thread {
        boolean _stop = false;
        HashMap<String, Pair<String, VirtualMachine.State>> _changes = new HashMap<>();
        boolean _isMaster;
        Set<String> _classes;
        String _token = "";

        public VmEventListener(final boolean isMaster) {
            _isMaster = isMaster;
            _classes = new HashSet<>();
            _classes.add("VM");
        }

        @Override
        public void start() {
            if (_isMaster) {
                // Throw away the initial set of events because they're history
                final Connection conn = getConnection();
                final EventBatch results;
                try {
                    results = Event.from(conn, _classes, _token, new Double(30));
                } catch (final Exception e) {
                    s_logger.error("Retrying the waiting on VM events due to: ", e);
                    throw new CloudRuntimeException("Unable to start a listener thread to listen to VM events", e);
                }
                _token = results.token;
                s_logger.debug("Starting the event listener thread for " + _host.getUuid());
                super.start();
            }
        }

        @Override
        public void run() {
            setName("XS-Listener-" + _host.getIp());
            while (!_stop) {
                final Connection conn = getConnection();
                final EventBatch results;
                try {
                    results = Event.from(conn, _classes, _token, new Double(30));
                } catch (final Exception e) {
                    s_logger.error("Retrying the waiting on VM events due to: ", e);
                    continue;
                }

                _token = results.token;
                final Set<Event.Record> events = results.events;
                for (final Event.Record event : events) {
                    try {
                        if (!(event.snapshot instanceof VM.Record)) {
                            if (s_logger.isDebugEnabled()) {
                                s_logger.debug("The snapshot is not a VM: " + event);
                            }
                            continue;
                        }
                        final VM.Record vm = (VM.Record) event.snapshot;

                        String hostUuid = null;
                        if (vm.residentOn != null && !vm.residentOn.toWireString().contains("OpaqueRef:NULL")) {
                            hostUuid = vm.residentOn.getUuid(conn);
                        }
                        recordChanges(conn, vm, hostUuid);
                    } catch (final Exception e) {
                        s_logger.error("Skipping over " + event, e);
                    }
                }
            }
        }

        protected void recordChanges(final Connection conn, final VM.Record rec, final String hostUuid) {

        }

        public boolean isListening() {
            return _isMaster;
        }

        public HashMap<String, Pair<String, VirtualMachine.State>> getChanges() {
            synchronized (_cluster.intern()) {
                if (_changes.size() == 0) {
                    return null;
                }
                final HashMap<String, Pair<String, VirtualMachine.State>> diff = _changes;
                _changes = new HashMap<>();
                return diff;
            }
        }

        public void signalStop() {
            _stop = true;
            interrupt();
        }
    }
}
