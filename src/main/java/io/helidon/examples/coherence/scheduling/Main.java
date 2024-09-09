package io.helidon.examples.coherence.scheduling;

import io.helidon.logging.common.LogConfig;
import io.helidon.config.Config;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;

import com.tangosol.net.Coherence;

public class Main {

    private Main() {
    }

    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException {

        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config config = Config.create();
        Config.global(config);

        Coherence coherence = Coherence.clusterMember();
        coherence.startAndWait();

        WebServer server = WebServer.builder()
                .config(config.get("server"))
                .routing(Main::routing)
                .build()
                .start();

        System.out.println("WEB server is up! http://localhost:" + server.port());
    }

    static void routing(HttpRouting.Builder routing) {
        routing.register("/scheduler", new SchedulerService());
    }
}
