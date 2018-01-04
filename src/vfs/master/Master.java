package vfs.master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;
import vfs.struct.FileNode;

public class Master {

	FileHierarchy fileHierarchy;

	private int nextFileHandleID;
	private int nextChunkID;

	private ArrayList<SlaveCommunication> slaves;

	private HashMap<Integer, ChunkInfo> chunkInfoList;

	private static String serializedJSONFileName = "configuration.json";

	public Master() {
		readFromJSONFile();
		chunkInfoList = new HashMap<Integer, ChunkInfo>();
		for (SlaveCommunication slave : slaves) {
			try {
				chunkInfoList.putAll(slave.requestChunkInfo());
			} catch (UnknownHostException e) {
				// TODO
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void readFromJSONFile() {
		try {
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new FileReader(serializedJSONFileName));
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			br.close();

			JSONObject config = new JSONObject(sb.toString());
			fileHierarchy = new FileHierarchy(config.getJSONObject("file_hierarchy"));
			nextFileHandleID = config.getInt("next_file_handle_id");
			nextChunkID = config.getInt("next_chunk_id");
			JSONArray slavesArray = config.getJSONArray("slaves");
			slaves = new ArrayList<SlaveCommunication>();
			for (int i = 0; i < slavesArray.length(); i++) {
				JSONObject obj = slavesArray.getJSONObject(i);
				SlaveCommunication slave = new SlaveCommunication(obj.getString("ip"), obj.getInt("port"));
				slaves.add(slave);
			}
		} catch (FileNotFoundException | JSONException e) {
			fileHierarchy = new FileHierarchy();
			nextFileHandleID = 0;
			nextChunkID = 0;
			slaves = new ArrayList<SlaveCommunication>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void saveToJSONFile() {
		JSONObject config = new JSONObject();
		config.put("next_file_handle_id", nextFileHandleID);
		config.put("next_chunk_id", nextChunkID);
		config.put("file_hierarchy", fileHierarchy.toJSON());
		JSONArray slavesArray = new JSONArray();
		for (SlaveCommunication slave : slaves) {
			JSONObject obj = new JSONObject();
			obj.put("ip", slave.IP);
			obj.put("port", slave.port);
			slavesArray.put(obj);
		}
		config.put("slaves", slavesArray);
		try {
			FileWriter fileWriter = new FileWriter(serializedJSONFileName);
			fileWriter.write(config.toString());
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private JSONObject open(String path, String name, String mode) {
		FileNode fileNode = fileHierarchy.openFile(path, name);
		if (fileNode == null)
			return null;

		JSONObject handle = new JSONObject();
		handle.put("handle", nextFileHandleID++);
		handle.put("offset", 0);
		handle.put("mode", -1);
		JSONObject fileInfo = new JSONObject();
		fileInfo.put("fileName", fileNode.fileName);
		fileInfo.put("fileType", fileNode.isDir ? 1 : 0);
		fileInfo.put("remotePath", fileNode.getPath());
		handle.put("fileInfo", fileInfo);

		JSONArray chunkList = new JSONArray();
		for (Integer chunkID : fileNode.chunkIDList) {
			JSONObject chunk = new JSONObject();
			ChunkInfo chunkInfo = chunkInfoList.get(chunkID);
			chunk.put("chunkId", chunkInfo.chunkId);
			chunk.put("slaveIP", chunkInfo.slaveIP);
			chunk.put("port", chunkInfo.port);
			chunk.put("fileIndex", chunkInfo.fileIndex);
			chunk.put("chunkLeft", chunkInfo.chunkLeft);
			chunkList.put(chunk);
		}
		handle.put("chunkList", chunkList);
		return handle;
	}

	private boolean remove(String path, String name) throws UnknownHostException, IOException {
		return releaseFileNode(fileHierarchy.remove(path, name));
	}

	private boolean releaseFileNode(FileNode fileNode) throws UnknownHostException, IOException {
		if (fileNode.isDir) {
			if (fileNode.child != null)
				releaseFileNode(fileNode.child);
		} else {
			for (int chunkID : fileNode.chunkIDList) {
				SlaveCommunication slave = findSlaveWithIP(chunkInfoList.get(chunkID).slaveIP);
				if (slave == null)
					continue;
				if (!slave.removeChunk(chunkID))
					return false;
			}
		}
		if (fileNode.brother != null)
			releaseFileNode(fileNode.brother);
		return true;
	}

	private SlaveCommunication findSlaveWithIP(String IP) {
		for (SlaveCommunication slave : slaves) {
			if (slave.IP.equals(IP)) {
				return slave;
			}
		}
		return null;
	}

	public class ClientWorker extends Thread {

		int protocol;
		InputStream in;
		OutputStream out;

		public ClientWorker(int protocol, InputStream in, OutputStream out) {
			super();
			this.protocol = protocol;
			this.in = in;
			this.out = out;
		}

		@Override
		public void run() {
			try {
				String path = null;
				String name = null;
				String mode = null;
				// TODO read path
				switch (protocol) {
				case 1:
					Util.sendJSON(out, open(path, name, mode));
					break;
				case 2:
					if (remove(path, name))
						Util.sendProtocol(out, protocol);
					break;
				case 3:
					// add chunk
					break;
				case 4:
					// remove chunk
					break;
				default:
					break;
				}
			} catch (IOException e) {

			}
		}
	}

	public static void main(String[] args) {
		Master master = new Master();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println();
				System.out.println("Exiting...");
				master.saveToJSONFile();
			}
		}));
		while (true) {
			// Listen to client
			// 1. request file handle
			// 2. remove file/folder?
			// 3. add chunk? file handle?
			// 4. remove chunk?
			// Listen to slave
			// 1. slave rent request?
			// 2. heart beat request?
			try (ServerSocket serverSocket = new ServerSocket(8192); // port
					Socket clientSocket = serverSocket.accept();
					OutputStream out = clientSocket.getOutputStream();
					InputStream in = clientSocket.getInputStream();) {
				byte[] protocolBuff = new byte[8];
				while (true) {
					in.read(protocolBuff, 0, 8);
					int protocol = Integer.parseInt(protocolBuff.toString());
					ClientWorker clientWorker = master.new ClientWorker(protocol, in, out);
					clientWorker.start();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
