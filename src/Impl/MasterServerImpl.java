package Impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;
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
	private static int NUM_REPLICA_PER_FILE = 2;
	FileWriter metaDataWriter;

	private static Random r = new Random(System.nanoTime());

	private static synchronized int rand(int size) {
		return r.nextInt();
	}

	public MasterServerImpl(File metaData,
			TreeMap<String, ReplicaLoc> nameToLocMap) throws IOException {
		locMap = new ConcurrentHashMap<String, ReplicaLoc[]>();
		txnID = new AtomicInteger(0);
		timeStamp = new AtomicInteger(0);
		replicaServerAddresses = new ReplicaLoc[nameToLocMap.size()];

		int ii = 0;
		for (ReplicaLoc loc : nameToLocMap.values()) {
			replicaServerAddresses[ii++] = loc;
		}

		Scanner scanner = new Scanner(metaData);
		while (scanner.hasNext()) {
			StringTokenizer tok = new StringTokenizer(scanner.nextLine());
			String fName = tok.nextToken();
			ReplicaLoc[] fileLocations = new ReplicaLoc[tok.countTokens()];
			for (int i = 0; i < fileLocations.length; i++) {
				fileLocations[i] = nameToLocMap.get(fName);
			}
			locMap.put(fName, fileLocations);
		}
		scanner.close();

		metaDataWriter = new FileWriter(metaData, true);
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

		// check if this is a commit acknowledgment
		if (data.getData() == null) {
			synchronized (this) {
				ReplicaLoc[] locations = locMap.get(fileName);
				metaDataWriter.write(fileName);
				for (int i = 0; i < locations.length; i++) {
					metaDataWriter.write(" " + locations[i].getName());
				}
				metaDataWriter.write("\n");
				metaDataWriter.flush();
			}
			return null;
		} else {
			// This step guarantees that clients who request same file reach out
			// the
			// primary replica in the order which they obtain their transaction
			// id's
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
	}

	static class StreamReader implements Runnable {

		private BufferedReader reader;
		private boolean isErrorStream;

		public StreamReader(InputStream is, boolean isErrorStream) {
			this.reader = new BufferedReader(new InputStreamReader(is));
			this.isErrorStream = isErrorStream;
		}

		public void run() {
			try {
				String line = reader.readLine();
				while (line != null) {
					if (isErrorStream)
						System.err.println(line);
					else
						System.out.println(line);
					line = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// arguments: ip-address port dir
	public static void main(String[] args) throws FileNotFoundException {
		String masterName = "masterServer";
		String masterAdd = args[0];
		int masterPort = new Integer(args[1]);
		String dir = args[2];
		File metaData = new File("metaData.txt");
		File repServers = new File("repServers.txt");
		TreeMap<String, ReplicaLoc> nameToLocMap = new TreeMap<String, ReplicaLoc>();

		Scanner scanner = new Scanner(repServers);

		while (scanner.hasNext()) {
			String repName = scanner.next(), repAddress = scanner.next();
			int repPort = scanner.nextInt();
			nameToLocMap.put(repName, new ReplicaLoc(repAddress, repName,
					repPort));
		}
		scanner.close();

		try {
			Registry registry = null;
			try {
				registry = LocateRegistry.createRegistry(masterPort);
			} catch (Exception e) {
				registry = LocateRegistry.getRegistry(masterPort);
			}

			MasterServerImpl masterServerObj = new MasterServerImpl(metaData,
					nameToLocMap);
			MasterServerClientInterface masterServerStub = (MasterServerClientInterface) UnicastRemoteObject
					.exportObject(masterServerObj, 0);
			registry.bind(masterName, masterServerStub);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// pkill -f 'java.*ReplicaServerImpl'
		try {
			for (ReplicaLoc repLoc : nameToLocMap.values()) {
				Process p = Runtime.getRuntime()
						.exec("ssh " + repLoc.getHost());
				PrintStream out = new PrintStream(p.getOutputStream(), true);
				StreamReader ls = new StreamReader(p.getInputStream(), false);
				StreamReader es = new StreamReader(p.getErrorStream(), true);

				Thread t = new Thread(ls);
				Thread t2 = new Thread(es);
				t.start();
				t2.start();

				out.println("cd workspace/ReplicatedFS/src");
				out.println("javac API/ReplicaServerClientInterface.java Impl/ReplicaServerImpl.java");
				// args: name dir port masterName masterAddress masterPort
				out.println("java  Impl/ReplicaServerImpl " + repLoc.getName()
						+ " " + dir + " " + repLoc.getPort() + " " + masterName
						+ " " + masterAdd + " " + masterPort);
				out.println("exit");

				// t.join();
				// t2.join();
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
