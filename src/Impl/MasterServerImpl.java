package Impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import API.MasterServerClientInterface;

public class MasterServerImpl implements MasterServerClientInterface {

	private ConcurrentHashMap<String, ReplicaLoc[]> locMap;
	private AtomicInteger txnID, timeStamp;
	private String dir;

	public MasterServerImpl(String dir, File metaData, File repServers) {
		locMap = new ConcurrentHashMap<String, ReplicaLoc[]>();
		txnID = new AtomicInteger(0);
		timeStamp = new AtomicInteger(0);
		this.dir = dir;
	}

	@Override
	public ReplicaLoc[] read(String fileName) throws FileNotFoundException,
			IOException, RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WriteMsg write(FileContent data) throws RemoteException, IOException {
		// TODO Auto-generated method stub
		return null;
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
