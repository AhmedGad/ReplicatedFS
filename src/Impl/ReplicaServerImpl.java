package Impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import API.ReplicaServerClientInterface;

public class ReplicaServerImpl implements ReplicaServerClientInterface {

	ConcurrentHashMap<Long, String> fileNameTransaction;
	ConcurrentHashMap<String, Lock> fileLock;
	ConcurrentHashMap<String, FileContent> cache;

	public ReplicaServerImpl() {
		fileNameTransaction = new ConcurrentHashMap<>();
		fileLock = new ConcurrentHashMap<>();
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
			boolean success = true;
			try {
				FileWriter fw = new FileWriter(fileName, true);
				fw.write(content.getData());
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
}
