package Impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;

import API.ReplicaServerClientInterface;

public class ReplicaServerImpl implements ReplicaServerClientInterface {

	@Override
	public WriteMsg write(long txnID, long msgSeqNum, FileContent data)
			throws RemoteException, IOException {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

}
