package de.tum.i13.Cloud;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;

public class Configuration {
    private static final Logger log = LogManager.getLogger(Configuration.class);

    private Properties properties;

    public Configuration(String configFile) {
        this.properties = new Properties();

        try {
            this.properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            log.warn("could not load configuration file");
        }
    }

    public List<String> getHosts() {
        List<String> results = new LinkedList<>();
        String host;

        // iterate over all servers.*.host and servers.*.port to discover servers
        for(int i = 0; (host = this.properties.getProperty("cloud.workers." + i)) != null; i++) {
            results.add(host);
        }

        return results;
    }

    public List<String> getMyHosts() {
        Iterator<NetworkInterface> it;
        Map<InetAddress, List<String>> validAddresses = new HashMap<>();

        // make a map from network address to ServerConfig
        for (String host : getHosts()) {
            try {
                InetAddress inet = InetAddress.getByName(Utils.getAddr(host));
                if (!validAddresses.containsKey(inet)) {
                    validAddresses.put(inet, new LinkedList<>());

                }
                validAddresses.get(inet).add(host);
            } catch (UnknownHostException e) {
                log.warn("unknown host " + host);
            }
        }

        // get an iterator of network interfaces
        NetworkInterface ni;

        try {
            it = NetworkInterface.getNetworkInterfaces().asIterator();
        } catch (SocketException e) {
            log.error("could not discover ip address");
            return null;
        }

        // check if there is a network address in any interface that matches
        // a configuration in config file
        List<String> myHosts = new LinkedList<>();

        while (it.hasNext()) {
            ni = it.next();
            ni.getInterfaceAddresses().stream()
                    .map(InterfaceAddress::getAddress)
                    .filter(validAddresses::containsKey)
                    .map(validAddresses::get)
                    .forEach(myHosts::addAll);
        }
        return myHosts;
    }
}
