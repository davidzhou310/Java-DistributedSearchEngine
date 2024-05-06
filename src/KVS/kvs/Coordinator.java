package KVS.kvs;

import Webserver.Server;

public class Coordinator extends KVS.generic.Coordinator {
    public static void main(String[] args) throws Exception {
        Server server = new Server();

        server.port(Integer.parseInt(args[0]));
        registerRoute(server);
    }
}
