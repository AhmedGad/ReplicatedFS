package Client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import API.MasterServerClientInterface;
import API.ReplicaServerClientInterface;
import Impl.ReplicaLoc;

public class ReadClient {
	public static void main(String[] args) {
		try {
			Registry reg = LocateRegistry.getRegistry();
			MasterServerClientInterface masterServer = (MasterServerClientInterface) reg
					.lookup("masterServer");
			ReplicaLoc loc = masterServer.read("file2.txt")[0];
			ReplicaServerClientInterface repServer = (ReplicaServerClientInterface) reg
					.lookup(loc.getName());

			for (int i = 0; i < 10; i++) {
				System.out.println(repServer.read("file2.txt").getData());
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
