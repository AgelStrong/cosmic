package com.cloud.utils.testcase;

import com.cloud.legacymodel.exceptions.NioConnectionException;
import com.cloud.utils.nio.HandlerFactory;
import com.cloud.utils.nio.Link;
import com.cloud.utils.nio.NioClient;
import com.cloud.utils.nio.NioServer;
import com.cloud.utils.nio.Task;
import com.cloud.utils.nio.Task.Type;

import java.nio.channels.ClosedChannelException;
import java.util.Random;

import junit.framework.TestCase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 *
 *
 */

public class NioTest extends TestCase {

    private static final Logger s_logger = LoggerFactory.getLogger(NioTest.class);
    Random randomGenerator = new Random();
    byte[] _testBytes;
    private NioServer _server;
    private NioClient _client;
    private Link _clientLink;
    private int _testCount;
    private int _completedCount;

    @Override
    public void setUp() {
        s_logger.info("Test");

        _testCount = 0;
        _completedCount = 0;

        _server = new NioServer("NioTestServer", 7777, 5, new NioTestServer());
        try {
            _server.start();
        } catch (final NioConnectionException e) {
            fail(e.getMessage());
        }

        _client = new NioClient("NioTestServer", "127.0.0.1", 7777, 5, new NioTestClient());
        try {
            _client.start();
        } catch (final NioConnectionException e) {
            fail(e.getMessage());
        }

        while (_clientLink == null) {
            try {
                s_logger.debug("Link is not up! Waiting ...");
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void tearDown() {
        while (!isTestsDone()) {
            try {
                s_logger.debug(_completedCount + "/" + _testCount + " tests done. Waiting for completion");
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        stopClient();
        stopServer();
    }

    private boolean isTestsDone() {
        final boolean result;
        synchronized (this) {
            result = _testCount == _completedCount;
        }
        return result;
    }

    protected void stopClient() {
        _client.stop();
        s_logger.info("Client stopped.");
    }

    protected void stopServer() {
        _server.stop();
        s_logger.info("Server stopped.");
    }

    protected void setClientLink(final Link link) {
        _clientLink = link;
    }

    public void testConnection() {
        _testBytes = new byte[1000000];
        randomGenerator.nextBytes(_testBytes);
        try {
            getOneMoreTest();
            _clientLink.send(_testBytes);
            s_logger.info("Client: Data sent");
            getOneMoreTest();
            _clientLink.send(_testBytes);
            s_logger.info("Client: Data sent");
        } catch (final ClosedChannelException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void getOneMoreTest() {
        synchronized (this) {
            _testCount++;
        }
    }

    protected void doServerProcess(final byte[] data) {
        oneMoreTestDone();
        Assert.assertArrayEquals(_testBytes, data);
        s_logger.info("Verify done.");
    }

    private void oneMoreTestDone() {
        synchronized (this) {
            _completedCount++;
        }
    }

    public class NioTestClient implements HandlerFactory {

        @Override
        public Task create(final Type type, final Link link, final byte[] data) {
            return new NioTestClientHandler(type, link, data);
        }

        public class NioTestClientHandler extends Task {

            public NioTestClientHandler(final Type type, final Link link, final byte[] data) {
                super(type, link, data);
            }

            @Override
            public void doTask(final Task task) {
                if (task.getType() == Task.Type.CONNECT) {
                    s_logger.info("Client: Received CONNECT task");
                    setClientLink(task.getLink());
                } else if (task.getType() == Task.Type.DATA) {
                    s_logger.info("Client: Received DATA task");
                } else if (task.getType() == Task.Type.DISCONNECT) {
                    s_logger.info("Client: Received DISCONNECT task");
                    stopClient();
                } else if (task.getType() == Task.Type.OTHER) {
                    s_logger.info("Client: Received OTHER task");
                }
            }
        }
    }

    public class NioTestServer implements HandlerFactory {

        public class NioTestServerHandler extends Task {

            public NioTestServerHandler(final Type type, final Link link, final byte[] data) {
                super(type, link, data);
            }

            @Override
            public void doTask(final Task task) {
                if (task.getType() == Task.Type.CONNECT) {
                    s_logger.info("Server: Received CONNECT task");
                } else if (task.getType() == Task.Type.DATA) {
                    s_logger.info("Server: Received DATA task");
                    doServerProcess(task.getData());
                } else if (task.getType() == Task.Type.DISCONNECT) {
                    s_logger.info("Server: Received DISCONNECT task");
                    stopServer();
                } else if (task.getType() == Task.Type.OTHER) {
                    s_logger.info("Server: Received OTHER task");
                }
            }
        }

        @Override
        public Task create(final Type type, final Link link, final byte[] data) {
            return new NioTestServerHandler(type, link, data);
        }
    }
}
