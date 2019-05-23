package com.cloud.cluster;

import com.cloud.common.managed.context.ManagedContextRunnable;
import com.cloud.utils.concurrency.NamedThreadFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpException;
import org.apache.http.impl.DefaultBHttpServerConnection;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.HttpService;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.http.protocol.UriHttpRequestHandlerMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterServiceServletContainer {
    private static final Logger s_logger = LoggerFactory.getLogger(ClusterServiceServletContainer.class);

    private ListenerThread listenerThread;

    public ClusterServiceServletContainer() {
    }

    public boolean start(final HttpRequestHandler requestHandler, final int port) {

        listenerThread = new ListenerThread(requestHandler, port);
        listenerThread.start();

        return true;
    }

    public void stop() {
        if (listenerThread != null) {
            listenerThread.stopRunning();
        }
    }

    static class ListenerThread extends Thread {
        private ExecutorService _executor;
        private HttpService _httpService = null;
        private volatile ServerSocket _serverSocket = null;

        public ListenerThread(final HttpRequestHandler requestHandler, final int port) {
            _executor = Executors.newCachedThreadPool(new NamedThreadFactory("Cluster-Listener"));

            try {
                _serverSocket = new ServerSocket(port);
            } catch (final IOException ioex) {
                s_logger.error("error initializing cluster service servlet container", ioex);
                return;
            }

            // Set up the HTTP protocol processor
            final HttpProcessor httpproc = HttpProcessorBuilder.create()
                                                               .add(new ResponseDate())
                                                               .add(new ResponseServer("HttpComponents/1.1"))
                                                               .add(new ResponseContent())
                                                               .add(new ResponseConnControl())
                                                               .build();

            // Set up request handlers
            final UriHttpRequestHandlerMapper registry = new UriHttpRequestHandlerMapper();
            registry.register("/clusterservice", requestHandler);

            // Set up the HTTP service
            _httpService = new HttpService(httpproc, new DefaultConnectionReuseStrategy(), new DefaultHttpResponseFactory(), registry);
        }

        public void stopRunning() {
            if (_serverSocket != null) {
                try {
                    _serverSocket.close();
                } catch (final IOException e) {
                    s_logger.info("[ignored] error on closing server socket", e);
                }
                _serverSocket = null;
            }
        }

        @Override
        public void run() {
            if (s_logger.isInfoEnabled()) {
                s_logger.info("Cluster service servlet container listening on port " + _serverSocket.getLocalPort());
            }

            while (_serverSocket != null) {
                try {
                    // Set up HTTP connection
                    final Socket socket = _serverSocket.accept();
                    final DefaultBHttpServerConnection conn = new DefaultBHttpServerConnection(8 * 1024);
                    conn.setSocketTimeout(5000);
                    conn.bind(socket);

                    _executor.execute(new ManagedContextRunnable() {
                        @Override
                        protected void runInContext() {
                            final HttpContext context = new BasicHttpContext(null);
                            try {
                                while (!Thread.interrupted() && conn.isOpen()) {
                                    if (s_logger.isTraceEnabled()) {
                                        s_logger.trace("dispatching cluster request from " + conn.getRemoteAddress().toString());
                                    }

                                    _httpService.handleRequest(conn, context);

                                    if (s_logger.isTraceEnabled()) {
                                        s_logger.trace("Cluster request from " + conn.getRemoteAddress().toString() + " is processed");
                                    }
                                }
                            } catch (final ConnectionClosedException ex) {
                                // client close and read time out exceptions are expected
                                // when KEEP-ALIVE is enabled
                                s_logger.trace("Client closed connection", ex);
                            } catch (final IOException ex) {
                                s_logger.trace("I/O error", ex);
                            } catch (final HttpException ex) {
                                s_logger.error("Unrecoverable HTTP protocol violation", ex);
                            } finally {
                                try {
                                    conn.shutdown();
                                } catch (final IOException ignore) {
                                    s_logger.error("unexpected exception", ignore);
                                }
                            }
                        }
                    });
                } catch (final IOException e) {
                    s_logger.error("Unexpected exception ", e);

                    // back off to avoid spinning if the exception condition keeps coming back
                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException e1) {
                        s_logger.debug("[ignored] interrupted while waiting to retry running the servlet container.");
                    }
                }

                _executor.shutdown();
                if (s_logger.isInfoEnabled()) {
                    s_logger.info("Cluster service servlet container shutdown");
                }
            }
        }
    }
}
