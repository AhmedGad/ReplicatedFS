package Impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import API.MasterServerClientInterface;
import API.ReplicaServerClientInterface;

public class MasterServerImpl implements MasterServerClientInterface {

	private ConcurrentHashMap<String, ReplicaLoc[]> locMap;
	private ConcurrentHashMap<String, Lock> fileLock;
	private ReplicaLoc[] replicaServerAddresses;
	private AtomicInteger txnID, timeStamp;
	private String dir;
	private static int NUM_REPLICA_PER_FILE = 2;

	private static Random r = new Random(System.nanoTime());

	private static synchronized int rand(int size) {
		return r.nextInt();
	}

	public MasterServerImpl(String dir, File metaData, File repServers) {
		locMap = new ConcurrentHashMap<String, ReplicaLoc[]>();
		txnID = new AtomicInteger(0);
		timeStamp = new AtomicInteger(0);
		this.dir = dir;
	}

	@Override
	public ReplicaLoc[] read(String fileName) throws FileNotFoundException,
			IOException, RemoteException {
		return locMap.get(fileName);
	}

	private ReplicaLoc[] selectRandomReplicas() {
		ReplicaLoc[] result = new ReplicaLoc[NUM_REPLICA_PER_FILE];
		boolean[] visited = new boolean[replicaServerAddresses.length];
		for (int i = 0; i < result.length; i++) {
			int randomReplicaServer = rand(replicaServerAddresses.length);
			while (visited[randomReplicaServer])
				randomReplicaServer = rand(replicaServerAddresses.length);
			visited[randomReplicaServer] = true;
			result[i] = replicaServerAddresses[randomReplicaServer];
		}
		return result;
	}

	@Override
	public WriteMsg write(FileContent data) throws RemoteException, IOException {
		String fileName = data.getFileName();
		// This step guarantees that clients who request same file reach out the
		// primary replica in the order which they obtain their transaction id's
		Lock lock = new ReentrantLock(true);
		try {
			(lock = fileLock.putIfAbsent(fileName, lock)).lock();
			int tId = txnID.incrementAndGet();
			int ts = timeStamp.incrementAndGet();
			ReplicaLoc[] locations = null;
			if (locMap.containsKey(fileName)) {
				locations = locMap.get(fileName);
			} else {
				locations = selectRandomReplicas();
			}
			ReplicaLoc primary = locations[0];
			ReplicaServerClientInterface primaryServer = null;
			try {
				primaryServer = (ReplicaServerClientInterface) LocateRegistry
						.getRegistry(primary.getHost(), primary.getPort())
						.lookup(primary.getName());
			} catch (Exception e) {
			}
			primaryServer.write(tId, 1, data);
			return new WriteMsg(tId, ts, primary);
		} finally {
			lock.unlock();
		}
	}

	public static void main(String[] args) {
		int port = 1099;
		String dir = "./FS";
		File metaData = new File("metaData.txt");
		File repServers = new File("repServers.txt");

		try {
			Registry registry = LocateRegistry.createRegistry(port);
			MasterServerImpl masterServerObj = new MasterServerImpl(dir,
					metaData, repServers);
			MasterServerClientInterface masterServerStub = (MasterServerClientInterface) UnicastRemoteObject
					.exportObject(masterServerObj, 0);
			registry.bind("masterServer", masterServerStub);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}
}
