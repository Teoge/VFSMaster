package vfs.master;

import java.io.BufferedReader;
import java.io.IOException;
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
		OutputStream out = socket.getOutputStream();
		byte[] protocolBuff = new byte[8];
		byte[] protocolBytes = (Integer.toString(VSFProtocols.INITIALIZE_CHUNK)).getBytes();
		for(int i = 0; i < protocolBytes.length; ++i){
			protocolBuff[i] = protocolBytes[i];
		}
		protocolBuff[protocolBytes.length] = '\0';
		out.write(protocolBuff, 0, protocolBuff.length);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		JSONArray chucks = new JSONArray(in.readLine());
		HashMap<Integer, ChunkInfo> chunkInfoList = new HashMap<Integer, ChunkInfo>();
		for (int i = 0; i < chucks.length(); i++) {
			JSONObject chunk = chucks.getJSONObject(i);
			ChunkInfo chunkInfo = new ChunkInfo(
					chunk.getInt("chunk_id"),
					chunk.getString("slave_ip"),
					chunk.getInt("port"),
					chunk.getInt("file_index"),
					chunk.getInt("chunk_left"));
			chunkInfoList.put(chunkInfo.chunkId, chunkInfo);
		}
		return chunkInfoList;
	}

}
