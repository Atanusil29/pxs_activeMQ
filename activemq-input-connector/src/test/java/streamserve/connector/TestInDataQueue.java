package streamserve.connector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Hashtable;

public class TestInDataQueue implements StrsInDataQueueable {

	ByteArrayOutputStream buffer;
	private int signal = 0;
	
	public TestInDataQueue(){
		buffer = new ByteArrayOutputStream();
	}

	public byte[] getBytes() {
		byte[] bytes = buffer.toByteArray();
		buffer.reset();
		return bytes;
	}
	public int getSignal() {
		return signal;
	}
	
	@Override
	public long getInternalJobId() throws RemoteException {
		return 0;
	}

	@Override
	public boolean put(byte b) throws RemoteException {
		buffer.write(b);
		return false;
	}

	@Override
	public boolean putArray(byte[] bytes) throws RemoteException {
		try {
			buffer.write(bytes);
		} catch (IOException e) {
			throw new RemoteException("exception writing array", e);
		}
		return false;
	}

	@Override
	public boolean putString(String str) throws RemoteException {
		return putArray(str.getBytes());
	}

	@Override
	public void setExtJobId(String jobId) throws RemoteException {
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void setJobHeader(Hashtable jobHeader) throws RemoteException {
	}

	@Override
	public void setJobName(String jobName) throws RemoteException {
	}

	@Override
	public void setJobOwner(String jobOwner) throws RemoteException {
	}

	@Override
	public void setJobPriority(int jobPriority) throws RemoteException {
	}

	@Override
	public boolean signalEvent(int signal) throws RemoteException {
		this.signal  = signal;
		return false;
	}

}
