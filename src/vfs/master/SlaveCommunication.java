package vfs.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;
import vfs.struct.VSFProtocols;

public class SlaveCommunication {

	public String IP;
	public int port;

	public SlaveCommunication(String iP, int port) {
		super();
		IP = iP;
		this.port = port;
	}

	public HashMap<Integer, ChunkInfo> requestChunkInfo() throws UnknownHostException, IOException {
		Socket socket = new Socket(IP, port);
		
		// Send Request
		Util.sendProtocol(socket.getOutputStream(), VSFProtocols.INITIALIZE_CHUNK_INFO);
		
		// Receive Data
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JSONArray chucks = new JSONArray(in.readLine());
		socket.close();

		HashMap<Integer, ChunkInfo> chunkInfoList = new HashMap<Integer, ChunkInfo>();
		for (int i = 0; i < chucks.length(); i++) {
			JSONObject chunk = chucks.getJSONObject(i);
			ChunkInfo chunkInfo = new ChunkInfo(chunk.getInt("chunk_id"), chunk.getString("slave_ip"),
					chunk.getInt("port"), chunk.getInt("file_index"), chunk.getInt("chunk_left"));
			chunkInfoList.put(chunkInfo.chunkId, chunkInfo);
		}
		return chunkInfoList;
	}

	public ChunkInfo createChunk(int chunkID, boolean isRent) throws UnknownHostException, IOException {
		Socket socket = new Socket(IP, port);
		
		// Send Request
		OutputStream out = socket.getOutputStream();
		Util.sendProtocol(out, VSFProtocols.NEW_CHUNK);
		JSONObject createChunkInfo = new JSONObject();
		createChunkInfo.put("chunk_id", chunkID);
		createChunkInfo.put("is_rent", isRent);
		Util.sendJSON(out, createChunkInfo);
		
		// Receive Data
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JSONObject chunk = new JSONObject(in.readLine());
		socket.close();

		ChunkInfo chunkInfo = new ChunkInfo(chunk.getInt("chunk_id"), chunk.getString("slave_ip"), chunk.getInt("port"),
				chunk.getInt("file_index"), chunk.getInt("chunk_left"));
		return chunkInfo;
	}
	
	public boolean removeChunk(int chunkID) throws UnknownHostException, IOException {
		Socket socket = new Socket(IP, port);
		Util.sendProtocol(socket.getOutputStream(), VSFProtocols.RELEASE_CHUNK);
		boolean succeed = Util.receiveOK(socket.getInputStream(), VSFProtocols.RELEASE_CHUNK);
		socket.close();
		return succeed;
	}
	
	public boolean detectHeartBeat() throws UnknownHostException, IOException {
		Socket socket = new Socket(IP, port);
		Util.sendProtocol(socket.getOutputStream(), VSFProtocols.HEART_BEAT_DETECT_TO_SLAVE);
		boolean succeed = Util.receiveOK(socket.getInputStream(), VSFProtocols.RELEASE_CHUNK);
		socket.close();
		return succeed;
	}
	
//	private Socket connectSocket() throws SocketTimeoutException {
//		Socket socket = new Socket();
//		socket.connect(new InetSocketAddress(IP, port), 15000);
//		return socket;
//	}
	

	

}
