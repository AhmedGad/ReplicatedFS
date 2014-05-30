package Impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import API.MasterServerClientInterface;
import API.ReplicaServerClientInterface;

public class ReplicaServerImpl implements ReplicaServerClientInterface {

	private ConcurrentHashMap<Long, String> fileNameTransaction;
	private ConcurrentHashMap<String, Lock> fileLock;
	private ConcurrentHashMap<String, FileContent> cache;
	private MasterServerClientInterface masterServer;
	private String dir;
	private String name;

	public ReplicaServerImpl(String dir, String name,
			MasterServerClientInterface masterServer) {
		fileNameTransaction = new ConcurrentHashMap<>();
		fileLock = new ConcurrentHashMap<>();
		this.dir = dir;
		this.name = name;
	}

	@Override
	public WriteMsg write(long txnID, long msgSeqNum, FileContent data)
			throws RemoteException, IOException {
		String fileName = data.getFileName();
		// if this is the first message, we obtain a lock on file first
		if (msgSeqNum == 1) {
			Lock lock = new ReentrantLock();
			lock = fileLock.putIfAbsent(fileName, lock);
			lock.lock();
			fileNameTransaction.put(txnID, fileName);
			cache.put(fileName, data);
		} else {
			cache.get(fileName).appendData(data.getData());
		}
		return null;
	}

	@Override
	public FileContent read(String fileName) throws FileNotFoundException,
			IOException, RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean commit(long txnID, long numOfMsgs)
			throws MessageNotFoundException, RemoteException {
		if (fileNameTransaction.contains(txnID)) {
			String fileName = fileNameTransaction.remove(txnID);
			FileContent content = cache.remove(fileName);
			ReplicaLoc[] locations = null;
			try {
				locations = masterServer.read(fileName);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			boolean success = true;

			// check if this is the master replica
			if (locations[0].getName().equals(name)) {
				for (int i = 1; i < locations.length; i++) {
					ReplicaLoc loc = locations[i];
					Registry reg = LocateRegistry.getRegistry(loc.getHost(),
							loc.getPort());
					try {
						ReplicaServerClientInterface repServer = (ReplicaServerClientInterface) reg
								.lookup(loc.getName());
						repServer.write(txnID, 1, content);
						success |= repServer.commit(txnID, 1);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			try {
				File f = new File(dir + "/" + fileName);
				FileWriter fw = new FileWriter(f, true);
				fw.write(content.getData());
				fw.close();
			} catch (IOException e) {
				success = false;
			}
			fileLock.get(fileName).unlock();
			return success;
		}
		return false;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException {
		if (fileNameTransaction.contains(txnID)) {
			String fileName = fileNameTransaction.remove(txnID);
			cache.remove(fileName);
			fileLock.get(fileName).unlock();
			return true;
		}
		return false;
	}

	// Arguments: name dir port masterName masterAddress masterPort
	public static void main(String[] args) {
		String name = args[0];
		String dir = args[1];
		int port = new Integer(args[2]);
		String masterServerName = args[3];
		String masterServerAdd = args[4];
		int masterServerPort = new Integer(args[5]);

		try {
			File dir1 = new File(dir);
			if (!dir1.exists())
				dir1.mkdir();
			File subDir = new File(dir + "/" + name);
			if (!subDir.exists())
				subDir.mkdir();
			Registry registry = null;
			try {
				registry = LocateRegistry.createRegistry(port);
				System.out.println("hasdaj");
			} catch (Exception e) {
				registry = LocateRegistry.getRegistry(port);
			}

			Registry reg2 = LocateRegistry.getRegistry(masterServerAdd,
					masterServerPort);
			MasterServerClientInterface masterServer = (MasterServerClientInterface) reg2
					.lookup(masterServerName);

			ReplicaServerImpl replicaServerObj = new ReplicaServerImpl(dir
					+ "/" + name, name, masterServer);
			ReplicaServerClientInterface replicaServerStub = (ReplicaServerClientInterface) UnicastRemoteObject
					.exportObject(replicaServerObj, 0);
			System.out.println("here!!");
			registry.bind(name, replicaServerStub);

			System.out.println("----" + masterServer.read("file1.txt").length);

			System.out.println("here also!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
