package de.tum.i13.Cloud;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;


public class Configuration {
    private static final Logger log = LogManager.getLogger(Configuration.class);

    private final Properties properties;

    public Configuration(String configFile) {
        this.properties = new Properties();

        try {
            this.properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            log.warn("could not load configuration file");
        }
    }

    public HostConfig getHost(int hostNum) {
        String val = this.properties.getProperty("cloud.workers." + hostNum);
        String isMaster = this.properties.getProperty("cloud.workers." + hostNum + "master");

        if (val == null) {
            log.warn("host not found");
            return null;
        }

        return new HostConfig(val, isMaster != null && isMaster.equals("true"));
    }

    public HostConfig[] getHosts() {
        List<HostConfig> results = new LinkedList<>();
        String host;

        for(int i = 0; (host = this.properties.getProperty("cloud.workers." + i)) != null; i++) {
            String isMaster = this.properties.getProperty("cloud.workers." + i + "master");
            results.add(new HostConfig(host, isMaster != null && isMaster.equals("true")));
        }

        return results.toArray(new HostConfig[0]);
    }

    static public class HostConfig {
        private final String host;
        private final int port;
        private final boolean isMaster;

        public HostConfig(String hostAndPort, boolean isMaster) {
            String[] tokens = hostAndPort.split(":");
            this.host = tokens[0];
            this.port = Integer.parseInt(tokens[1]);
            this.isMaster = false;
        }

        public boolean isMaster() {
            return isMaster;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
