package vfs.master;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.json.JSONObject;

import vfs.struct.ChunkInfo;

public class SlaveCommunication {

	public String IP;
	public int port;

	public SlaveCommunication(String iP, int port) {
		super();
		IP = iP;
		this.port = port;
	}

	public ChunkInfo requestChunkInfo() throws UnknownHostException, IOException {
		Socket socket = new Socket(IP, port);
		OutputStream out = socket.getOutputStream();
		
		return null;
	}

}
