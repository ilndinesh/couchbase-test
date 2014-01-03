
package dev.test;

import java.util.HashMap;
import java.util.Map;
import dev.test.bean.Report;
import dev.test.bean.Report.Conn;

/**
 * Bean for representing storage data.
 *
 * @version 1.0 Aug 23, 2013
 * @author Dinesh.Ilindra
 * @since 2.2
 */
public class StoreBean {

    /**
     * status enumeration.
     *
     * @version 1.0 Aug 23, 2013
     * @author Dinesh.Ilindra
     * @since 2.2
     */
    public static enum Status {
        /**
         * Online.
         */
        online,
        /**
         * unresponsive.
         */
        unresponsive,
        /**
         * Offline.
         */
        offline,
        /**
         * Unknown.
         */
        unknown,
    }

    /**
     * The node info for this bean.
     *
     * @version 1.0 Aug 23, 2013
     * @author Dinesh.Ilindra
     * @since 2.2
     */
    public static class NodeInfo {

        /**
         * node ID.
         */
        private String nodeId = "slim";

        /**
         * node IP.
         */
        private String nodeHost = "localhost";

        /**
         * node IP.
         */
        private String nodeIp = "127.0.0.1";

        /**
         * node port.
         */
        private int nodePort = 3400;

        /**
         * Last contacted at millis.
         */
        private long lastContactedTime;

        /**
         * Last active at millis.
         */
        private long lastActiveTime;

        /**
         * Last active at millis.
         */
        private long lastRegTime;

        /**
         * The payload from PNG message.
         */
        private String pngPayload;

        /**
         * The connection info for RPT.
         */
        private Map<Object, Conn> connInfoMap;

        /**
         * Default one.
         */
        public NodeInfo() {
            super();
        }

        /**
         * Using fields.
         *
         * @param nodeId
         * @param nodeHost
         * @param nodeIp
         * @param nodePort
         * @param connInfoMap
         */
        public NodeInfo(final String nodeId, final String nodeHost,
                final String nodeIp, final int nodePort,
                final Map<Object, Conn> connInfoMap) {
            this.nodeId = nodeId;
            this.nodeHost = nodeHost;
            this.nodeIp = nodeIp;
            this.nodePort = nodePort;
            this.connInfoMap = connInfoMap;
        }

        /**
         * @return the nodeId
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * @param nodeId
         *            the nodeId to set
         */
        public void setNodeId(final String nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @return the nodeHost
         */
        public String getNodeHost() {
            return nodeHost;
        }

        /**
         * @param nodeHost
         *            the nodeHost to set
         */
        public void setNodeHost(final String nodeHost) {
            this.nodeHost = nodeHost;
        }

        /**
         * @return the nodeIp
         */
        public String getNodeIp() {
            return nodeIp;
        }

        /**
         * @param nodeIp
         *            the nodeIp to set
         */
        public void setNodeIp(final String nodeIp) {
            this.nodeIp = nodeIp;
        }

        /**
         * @return the nodePort
         */
        public int getNodePort() {
            return nodePort;
        }

        /**
         * @param nodePort
         *            the nodePort to set
         */
        public void setNodePort(final int nodePort) {
            this.nodePort = nodePort;
        }

        /**
         * @return the lastContactedTime
         */
        public long getLastContactedTime() {
            return lastContactedTime;
        }

        /**
         * @param lastContactedTime
         *            the lastContactedTime to set
         */
        public void setLastContactedTime(final long lastContactedTime) {
            this.lastContactedTime = lastContactedTime;
        }

        /**
         * @return the lastActiveTime
         */
        public long getLastActiveTime() {
            return lastActiveTime;
        }

        /**
         * @param lastActiveTime
         *            the lastActiveTime to set
         */
        public void setLastActiveTime(final long lastActiveTime) {
            this.lastActiveTime = lastActiveTime;
        }

        /**
         * @return the lastRegTime
         */
        public long getLastRegTime() {
            return lastRegTime;
        }

        /**
         * @param lastRegTime
         *            the lastRegTime to set
         */
        public void setLastRegTime(final long lastRegTime) {
            this.lastRegTime = lastRegTime;
        }

        /**
         * @return the pngPayload
         */
        public String getPngPayload() {
            return pngPayload;
        }

        /**
         * @param pngPayload
         *            the pngPayload to set
         */
        public void setPngPayload(final String pngPayload) {
            this.pngPayload = pngPayload;
        }

        /**
         * @return the connInfoMap
         */
        public Map<Object, Conn> getConnInfoMap() {
            if (connInfoMap == null) {
                connInfoMap = new HashMap<Object, Report.Conn>();
            }
            return connInfoMap;
        }

        /**
         * @param connInfoMap
         *            the connInfoMap to set
         */
        public void setConnInfoMap(final Map<Object, Conn> connInfoMap) {
            this.connInfoMap = connInfoMap;
        }
    }

    /**
     * Empty node info map used in case of ndoe info unavailable.
     */
    public static final Map<String, NodeInfo> EMPTY_INFO_MAP = new HashMap<String, StoreBean.NodeInfo>(
            0);

    /**
     * Create a store bean from the given key.
     *
     * @param key
     * @return
     */
    public static StoreBean createBean(final String key) {
        final StoreBean bean = new StoreBean();
        bean.updateBeanData(key);
        return bean;
    }

    /**
     * doc ID.
     */
    private String docId = "";

    /**
     * doc type.
     */
    private String docType = "default";

    /**
     * devicve status.
     */
    private Status status = Status.unknown;

    /**
     * Registration timestamp as millis from epoch.
     */
    private long registeredAt;

    /**
     * Update timestamp as millis from epoch.
     */
    private long updatedAt;

    /**
     * Busy / not
     */
    private boolean busy;

    /**
     * Communication start time.
     */
    private long communicationStartTime;

    /**
     * Communication end time.
     */
    private long communicationEndTime;

    /**
     * Communication count.
     */
    private long communicationCount;

    /**
     * node info map.
     */
    private Map<String, NodeInfo> nodeInfoMap = new HashMap<String, StoreBean.NodeInfo>();

    /**
     * @return the docId
     */
    public String getDocId() {
        return docId;
    }

    /**
     * @param docId
     *            the docId to set
     */
    public void setDocId(final String docId) {
        this.docId = docId;
    }

    /**
     * @return the docType
     */
    public String getDocType() {
        return docType;
    }

    /**
     * @param docType
     *            the docType to set
     */
    public void setDocType(final String docType) {
        this.docType = docType;
    }

    /**
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setStatus(final Status status) {
        this.status = status;
    }

    /**
     * @return the registeredAt
     */
    public long getRegisteredAt() {
        return registeredAt;
    }

    /**
     * @param registeredAt
     *            the registeredAt to set
     */
    public void setRegisteredAt(final long registeredAt) {
        this.registeredAt = registeredAt;
    }

    /**
     * @return the updatedAt
     */
    public long getUpdatedAt() {
        return updatedAt;
    }

    /**
     * @param updatedAt
     *            the updatedAt to set
     */
    public void setUpdatedAt(final long updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * @return the busy
     */
    public boolean isBusy() {
        return busy;
    }

    /**
     * @param busy
     *            the busy to set
     */
    public void setBusy(final boolean busy) {
        this.busy = busy;
    }

    /**
     * @return the communicationStartTime
     */
    public long getCommunicationStartTime() {
        return communicationStartTime;
    }

    /**
     * @param communicationStartTime
     *            the communicationStartTime to set
     */
    public void setCommunicationStartTime(final long communicationStartTime) {
        this.communicationStartTime = communicationStartTime;
    }

    /**
     * @return the communicationEndTime
     */
    public long getCommunicationEndTime() {
        return communicationEndTime;
    }

    /**
     * @param communicationEndTime
     *            the communicationEndTime to set
     */
    public void setCommunicationEndTime(final long communicationEndTime) {
        this.communicationEndTime = communicationEndTime;
    }

    /**
     * @return the communicationCount
     */
    public long getCommunicationCount() {
        return communicationCount;
    }

    /**
     * @param communicationCount
     *            the communicationCount to set
     */
    public void setCommunicationCount(final long communicationCount) {
        this.communicationCount = communicationCount;
    }

    /**
     * @return the nodeInfoMap
     */
    public Map<String, NodeInfo> getNodeInfoMap() {
        return nodeInfoMap;
    }

    /**
     * @param nodeInfoMap
     *            the nodeInfoMap to set
     */
    public void setNodeInfoMap(final Map<String, NodeInfo> nodeInfoMap) {
        this.nodeInfoMap = nodeInfoMap;
    }

    /**
     * Update bean data from key
     *
     * @param key
     */
    public void updateBeanData(final String key) {
        final int cutIndex = key.indexOf('_');
        if (cutIndex == -1) {
            docId = key;
            docType = "default";
        } else {
            docId = key.substring(cutIndex + 1);
            docType = key.substring(0, cutIndex);
        }
    }

    /**
     * Puts the node info into this bean.
     *
     * @param nodeInfo
     */
    public void put(final NodeInfo nodeInfo) {
        if (!nodeInfoMap.containsKey(nodeInfo.nodeId)
                || nodeInfoMap.get(nodeInfo.nodeId).getConnInfoMap().size() < nodeInfo
                        .getConnInfoMap().size()) {
            registeredAt = System.currentTimeMillis();
            setBusy(false);
        }
        nodeInfoMap.put(nodeInfo.nodeId, nodeInfo);
        setStatus(Status.online);
    }

    /**
     * Removes the node info from this bean.
     *
     * @param nodeId
     */
    public void remove(final String nodeId) {
        nodeInfoMap.remove(nodeId);
        if (nodeInfoMap.isEmpty()) {
            setStatus(Status.offline);
        }
    }

    /**
     * Increments a communication count. Not thread safe.
     */
    public void incrementCommunicationCount() {
        communicationCount++;
    }

    /**
     * Fetches the node info of the given node id.
     *
     * @param nodeId
     * @return
     */
    public NodeInfo fetchNodeInfo(final String nodeId) {
        return nodeInfoMap.get(nodeId);
    }

    /**
     * The size of the underlying node info map.
     *
     * @return
     */
    public int nodeSize() {
        return nodeInfoMap.size();
    }

    /**
     * The sum of size of the underlying conn maps of all node infos.
     *
     * @return
     */
    public int connSize() {
        int size = 0;
        for (final NodeInfo info : nodeInfoMap.values()) {
            size += info.getConnInfoMap().size();
        }
        return size;
    }

}
