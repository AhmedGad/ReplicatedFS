package Client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import API.MasterServerClientInterface;
import API.ReplicaServerClientInterface;
import Impl.FileContent;
import Impl.ReplicaLoc;
import Impl.WriteMsg;

public class WriteClient {
	public static void main(String[] args) throws Exception {
		Registry reg = LocateRegistry.getRegistry();
		MasterServerClientInterface masterServer = (MasterServerClientInterface) reg
				.lookup("masterServer");
		ReplicaLoc loc = masterServer.read("file2.txt")[0];
		ReplicaServerClientInterface repServer = (ReplicaServerClientInterface) reg
				.lookup(loc.getName());
		WriteMsg msg = null;
		for (int i = 0; i < 10; i++) {
			if (i % 3 == 0) {
				if (msg != null) {
					repServer.commit(msg.getTransactionId(), 3);
				}
				FileContent data = new FileContent("file2.txt");
				data.appendData("\n\n");
				msg = masterServer.write(data);
				data = new FileContent("file2.txt");
				data.appendData(i + "===\n");
				repServer.write(msg.getTransactionId(), 0, data);
			} else {
				FileContent data = new FileContent("file2.txt");
				data.appendData(i + "===\n");
				repServer.write(msg.getTransactionId(), 0, data);
			}
			Thread.sleep(1000);
		}
		System.out.println(repServer.commit(msg.getTransactionId(), 3));
	}
}
