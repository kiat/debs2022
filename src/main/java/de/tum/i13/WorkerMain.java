package de.tum.i13;

import de.tum.i13.Cloud.Configuration;
import de.tum.i13.Cloud.Utils;
import de.tum.i13.Cloud.WorkerService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class WorkerMain {
	private static final Logger log = LogManager.getLogger(WorkerMain.class);
	static final int DEFAULT_PORT = 50051;

	private final int port;
	private final Server server;

	public WorkerMain(int port) throws IOException {
		this.port = port;
		this.server = ServerBuilder.forPort(port).addService(new WorkerService()).build().start();
	}

	public int getPort() {
		return this.port;
	}

	public void awaitTermination() throws InterruptedException {
		if (this.server != null) {
			this.server.awaitTermination();
		}
	}

	public static void main(String[] args) {
		WorkerMain server = null;

		if (args.length > 0) {
			int port = Integer.parseInt(args[0]);
			try {
				server = new WorkerMain(port);
			} catch (IOException e) {
				log.error("could not bind port " + port);
				System.exit(1);
			}

		} else {
			Configuration conf = new Configuration("cloud.properties");
			List<String> hosts = conf.getMyHosts();

			for (String host : hosts) {
				int port = Utils.getPort(host);

				try {
					server = new WorkerMain(port);
					break;
				} catch (IOException e) {
					log.warn("could not bind port " + port);
				}
			}
		}

		if (server == null) {
			try {
				server = new WorkerMain(DEFAULT_PORT);
			} catch (IOException e) {
				log.error("could not bind to default port " + DEFAULT_PORT);
				System.exit(1);
			}
			log.error("could not find a port to start the server");
			System.exit(1);
		}

		log.info("server listening on port " + server.getPort());

		try {
			server.awaitTermination();
		} catch (InterruptedException e) {
			log.warn("server interrupted");
		}

	}
}
