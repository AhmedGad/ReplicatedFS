package API;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import Impl.FileContent;
import Impl.ReplicaLoc;
import Impl.WriteMsg;

public interface MasterServerClientInterface extends Remote {
	/**
	 * Read file from server
	 * 
	 * @param fileName
	 * @return the addresses of  of its different replicas
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteException
	 */
	public ReplicaLoc[] read(String fileName) throws FileNotFoundException,
			IOException, RemoteException;

	/**
	 * Start a new write transaction
	 * 
	 * @param fileName
	 * @return the requiref info
	 * @throws RemoteException
	 * @throws IOException
	 */
	public WriteMsg write(FileContent data) throws RemoteException, IOException;

}
