package com.cloud.agent.resource.kvm.xml;

import com.cloud.agent.resource.kvm.xml.LibvirtVmDef.InterfaceDef;
import com.cloud.agent.resource.kvm.xml.LibvirtVmDef.RngDef;
import com.cloud.agent.resource.kvm.xml.LibvirtVmDef.WatchDogDef;
import com.cloud.model.enumeration.DiskControllerType;
import com.cloud.model.enumeration.ImageFormat;
import com.cloud.model.enumeration.NicModel;
import com.cloud.model.enumeration.RngBackendModel;
import com.cloud.model.enumeration.WatchDogAction;
import com.cloud.model.enumeration.WatchDogModel;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class LibvirtDomainXmlParser {
    private static final Logger s_logger = LoggerFactory.getLogger(LibvirtDomainXmlParser.class);
    private final List<InterfaceDef> interfaces = new ArrayList<>();
    private final List<LibvirtDiskDef> diskDefs = new ArrayList<>();
    private final List<RngDef> rngDefs = new ArrayList<>();
    private final List<WatchDogDef> watchDogDefs = new ArrayList<>();
    private Integer vncPort;
    private String desc;

    public boolean parseDomainXml(final String domXml) {
        final DocumentBuilder builder;
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            final InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(domXml));
            final Document doc = builder.parse(is);

            final Element rootElement = doc.getDocumentElement();

            this.desc = getTagValue("description", rootElement);

            final Element devices = (Element) rootElement.getElementsByTagName("devices").item(0);
            final NodeList disks = devices.getElementsByTagName("disk");
            for (int i = 0; i < disks.getLength(); i++) {
                final Element disk = (Element) disks.item(i);
                final String type = disk.getAttribute("type");
                final LibvirtDiskDef def = new LibvirtDiskDef();
                if (type.equalsIgnoreCase("network")) {
                    final String diskFormatType = getAttrValue("driver", "type", disk);
                    final String diskCacheMode = getAttrValue("driver", "cache", disk);
                    final String diskPath = getAttrValue("source", "name", disk);
                    final String protocol = getAttrValue("source", "protocol", disk);
                    final String authUserName = getAttrValue("auth", "username", disk);
                    final String poolUuid = getAttrValue("secret", "uuid", disk);
                    final String host = getAttrValue("host", "name", disk);
                    final int port = Integer.parseInt(getAttrValue("host", "port", disk));
                    final String diskLabel = getAttrValue("target", "dev", disk);
                    final String bus = getAttrValue("target", "bus", disk);

                    ImageFormat imageFormat = null;
                    if (diskFormatType != null) {
                        imageFormat = ImageFormat.valueOf(diskFormatType.toUpperCase());
                    }

                    def.defNetworkBasedDisk(diskPath, host, port, authUserName, poolUuid, diskLabel,
                            DiskControllerType.valueOf(bus.toUpperCase()),
                            LibvirtDiskDef.DiskProtocol.valueOf(protocol.toUpperCase()), imageFormat);
                    def.setCacheMode(LibvirtDiskDef.DiskCacheMode.valueOf(diskCacheMode.toUpperCase()));
                } else {
                    final String diskFormatType = getAttrValue("driver", "type", disk);
                    final String diskCacheMode = getAttrValue("driver", "cache", disk);
                    final String diskFile = getAttrValue("source", "file", disk);
                    final String diskDev = getAttrValue("source", "dev", disk);

                    final String diskLabel = getAttrValue("target", "dev", disk);
                    final String bus = getAttrValue("target", "bus", disk);
                    final String device = disk.getAttribute("device");

                    if (type.equalsIgnoreCase("file")) {
                        if (device.equalsIgnoreCase("disk")) {
                            ImageFormat imageFormat = null;
                            if (diskFormatType != null) {
                                imageFormat = ImageFormat.valueOf(diskFormatType.toUpperCase());
                            }
                            def.defFileBasedDisk(diskFile, diskLabel, DiskControllerType.valueOf(bus.toUpperCase()), imageFormat);
                        } else if (device.equalsIgnoreCase("cdrom")) {
                            def.defIsoDisk(diskFile);
                        }
                    } else if (type.equalsIgnoreCase("block")) {
                        def.defBlockBasedDisk(diskDev, diskLabel,
                                DiskControllerType.valueOf(bus.toUpperCase()));
                    }
                    if (diskCacheMode != null) {
                        def.setCacheMode(LibvirtDiskDef.DiskCacheMode.valueOf(diskCacheMode.toUpperCase()));
                    }
                }

                final NodeList iotune = disk.getElementsByTagName("iotune");
                if (iotune != null && iotune.getLength() != 0) {
                    final String bytesReadRateStr = getTagValue("read_bytes_sec", (Element) iotune.item(0));
                    if (bytesReadRateStr != null) {
                        final Long bytesReadRate = Long.parseLong(bytesReadRateStr);
                        def.setBytesReadRate(bytesReadRate);
                    }
                    final String bytesWriteRateStr = getTagValue("write_bytes_sec", (Element) iotune.item(0));
                    if (bytesWriteRateStr != null) {
                        final Long bytesWriteRate = Long.parseLong(bytesWriteRateStr);
                        def.setBytesWriteRate(bytesWriteRate);
                    }
                    final String iopsReadRateStr = getTagValue("read_iops_sec", (Element) iotune.item(0));
                    if (iopsReadRateStr != null) {
                        final Long iopsReadRate = Long.parseLong(iopsReadRateStr);
                        def.setIopsReadRate(iopsReadRate);
                    }
                    final String iopsWriteRateStr = getTagValue("write_iops_sec", (Element) iotune.item(0));
                    if (iopsWriteRateStr != null) {
                        final Long iopsWriteRate = Long.parseLong(iopsWriteRateStr);
                        def.setIopsWriteRate(iopsWriteRate);
                    }
                    final String iopsTotalRateStr = getTagValue("total_iops_sec", (Element) iotune.item(0));
                    if (iopsTotalRateStr != null) {
                        final Long iopsTotalRate = Long.parseLong(iopsTotalRateStr);
                        def.setIopsTotalRate(iopsTotalRate);
                    }
                }

                this.diskDefs.add(def);
            }

            final NodeList nics = devices.getElementsByTagName("interface");
            for (int i = 0; i < nics.getLength(); i++) {
                final Element nic = (Element) nics.item(i);

                final String type = nic.getAttribute("type");
                final String mac = getAttrValue("mac", "address", nic);
                final String dev = getAttrValue("target", "dev", nic);
                final String model = getAttrValue("model", "type", nic);
                final InterfaceDef def = new InterfaceDef();
                final NodeList bandwidth = nic.getElementsByTagName("bandwidth");
                Integer networkRateKBps = 0;
                if (bandwidth != null && bandwidth.getLength() != 0) {
                    final Integer inbound = Integer.valueOf(getAttrValue("inbound", "average", (Element) bandwidth.item(0)));
                    final Integer outbound = Integer.valueOf(getAttrValue("outbound", "average", (Element) bandwidth.item(0)));
                    if (inbound.equals(outbound)) {
                        networkRateKBps = inbound;
                    }
                }
                if (type.equalsIgnoreCase("network")) {
                    final String network = getAttrValue("source", "network", nic);
                    def.defPrivateNet(network, dev, mac, NicModel.valueOf(model.toUpperCase()), networkRateKBps);
                } else if (type.equalsIgnoreCase("bridge")) {
                    final String bridge = getAttrValue("source", "bridge", nic);
                    def.defBridgeNet(bridge, dev, mac, NicModel.valueOf(model.toUpperCase()), networkRateKBps);
                } else if (type.equalsIgnoreCase("ethernet")) {
                    final String scriptPath = getAttrValue("script", "path", nic);
                    def.defEthernet(dev, mac, NicModel.valueOf(model.toUpperCase()), scriptPath, networkRateKBps);
                }
                this.interfaces.add(def);
            }

            final Element graphic = (Element) devices.getElementsByTagName("graphics").item(0);

            if (graphic != null) {
                final String port = graphic.getAttribute("port");
                if (port != null) {
                    try {
                        this.vncPort = Integer.parseInt(port);
                        if (this.vncPort != -1) {
                            this.vncPort = this.vncPort - 5900;
                        } else {
                            this.vncPort = null;
                        }
                    } catch (final NumberFormatException nfe) {
                        this.vncPort = null;
                    }
                }
            }

            final NodeList rngs = devices.getElementsByTagName("rng");
            for (int i = 0; i < rngs.getLength(); i++) {
                RngDef def = null;
                final Element rng = (Element) rngs.item(i);
                final String backendModel = getAttrValue("backend", "model", rng);
                final String path = getTagValue("backend", rng);

                if (Strings.isNullOrEmpty(backendModel)) {
                    def = new RngDef(path);
                } else {
                    def = new RngDef(path, RngBackendModel.valueOf(backendModel.toUpperCase()));
                }

                this.rngDefs.add(def);
            }

            final NodeList watchDogs = devices.getElementsByTagName("watchdog");
            for (int i = 0; i < watchDogs.getLength(); i++) {
                WatchDogDef def = null;
                final Element watchDog = (Element) watchDogs.item(i);
                final String action = watchDog.getAttribute("action");
                final String model = watchDog.getAttribute("model");

                if (Strings.isNullOrEmpty(action)) {
                    def = new WatchDogDef(WatchDogModel.valueOf(model.toUpperCase()));
                } else {
                    def = new WatchDogDef(WatchDogAction.valueOf(action.toUpperCase()),
                            WatchDogModel.valueOf(model.toUpperCase()));
                }

                this.watchDogDefs.add(def);
            }

            return true;
        } catch (final ParserConfigurationException e) {
            s_logger.debug(e.toString());
        } catch (final SAXException e) {
            s_logger.debug(e.toString());
        } catch (final IOException e) {
            s_logger.debug(e.toString());
        }
        return false;
    }

    private static String getTagValue(final String tag, final Element element) {
        final NodeList tagNodeList = element.getElementsByTagName(tag);
        if (tagNodeList == null || tagNodeList.getLength() == 0) {
            return null;
        }

        final NodeList nlList = tagNodeList.item(0).getChildNodes();

        final Node nValue = nlList.item(0);

        return nValue.getNodeValue();
    }

    private static String getAttrValue(final String tag, final String attr, final Element element) {
        final NodeList tagNode = element.getElementsByTagName(tag);
        if (tagNode.getLength() == 0) {
            return null;
        }
        final Element node = (Element) tagNode.item(0);
        return node.getAttribute(attr);
    }

    public Integer getVncPort() {
        return this.vncPort;
    }

    public List<InterfaceDef> getInterfaces() {
        return this.interfaces;
    }

    public List<LibvirtDiskDef> getDisks() {
        return this.diskDefs;
    }

    public List<RngDef> getRngs() {
        return this.rngDefs;
    }

    public List<WatchDogDef> getWatchDogs() {
        return this.watchDogDefs;
    }

    public String getDescription() {
        return this.desc;
    }
}
