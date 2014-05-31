package Client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import API.MasterServerClientInterface;
import API.ReplicaServerClientInterface;
import Impl.FileContent;
import Impl.ReplicaLoc;
import Impl.WriteMsg;

public class Client {
	public static void main(String[] args) {
		try {
			Registry reg = LocateRegistry.getRegistry();
			MasterServerClientInterface masterServer = (MasterServerClientInterface) reg
					.lookup("masterServer");
			ReplicaLoc loc = masterServer.read("file2.txt")[0];
			ReplicaServerClientInterface repServer = (ReplicaServerClientInterface) reg
					.lookup(loc.getName());
			FileContent content = new FileContent("file2.txt");
			content.appendData("test line 2 file2 must not be writter\n");
			WriteMsg msg = masterServer.write(content);
			System.out.println(repServer.abort(msg.getTransactionId()));

			content = new FileContent("file2.txt");
			content.appendData("test line 2 file2 write it it's ok\n");
			msg = masterServer.write(content);
			System.out.println(repServer.commit(msg.getTransactionId(), 1));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
