package vfs.master;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;
import vfs.struct.FileNode;
import vfs.struct.VSFProtocols;

public class Master {

	FileHierarchy fileHierarchy;

	private int nextFileHandleId;
	private int nextChunkId;

	private ArrayList<SlaveCommunication> slaves;

	private HashMap<Integer, ChunkInfo> chunkInfoList;

	private HashMap<Integer, ArrayList<Integer>> mainCopyLookup;

	private static String serializedJSONFileName = "configuration.json";

	private static int numOfCopies = 3;

	public Master() {
		readFromJSONFile();
		chunkInfoList = new HashMap<Integer, ChunkInfo>();
		for (SlaveCommunication slave : slaves) {
			try {
				chunkInfoList.putAll(slave.requestChunkInfo());
			} catch (ConnectException e) {
				System.err.println("Slave " + slave.IP + ":" + slave.port + " connection timeout.");
			} catch (UnknownHostException e) {
				e.printStackTrace();
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
			nextFileHandleId = config.getInt("next_file_handle_id");
			nextChunkId = config.getInt("next_chunk_id");
			JSONArray slavesArray = config.getJSONArray("slaves");
			slaves = new ArrayList<SlaveCommunication>();
			for (int i = 0; i < slavesArray.length(); i++) {
				JSONObject obj = slavesArray.getJSONObject(i);
				SlaveCommunication slave = new SlaveCommunication(obj.getString("ip"), obj.getInt("port"));
				slaves.add(slave);
			}
			mainCopyLookup = new HashMap<Integer, ArrayList<Integer>>();
			JSONObject mainCopyLookupJSON = config.getJSONObject("main_copy_lookup");
			for (String key : mainCopyLookupJSON.keySet()) {
				ArrayList<Integer> ids = new ArrayList<Integer>();
				JSONArray idsJSON = mainCopyLookupJSON.getJSONArray(key);
				for (int i = 0; i < idsJSON.length(); i++) {
					ids.add(idsJSON.getInt(i));
				}
				mainCopyLookup.put(Integer.parseInt(key), ids);
			}
		} catch (FileNotFoundException | JSONException e) {
			fileHierarchy = new FileHierarchy();
			nextFileHandleId = 0;
			nextChunkId = 0;
			slaves = new ArrayList<SlaveCommunication>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void saveToJSONFile() {
		JSONObject config = new JSONObject();
		config.put("next_file_handle_id", nextFileHandleId);
		config.put("next_chunk_id", nextChunkId);
		config.put("file_hierarchy", fileHierarchy.toJSON());
		JSONArray slavesArray = new JSONArray();
		for (SlaveCommunication slave : slaves) {
			JSONObject obj = new JSONObject();
			obj.put("ip", slave.IP);
			obj.put("port", slave.port);
			slavesArray.put(obj);
		}
		config.put("slaves", slavesArray);
		JSONObject mainCopyLookupJSON = new JSONObject();
		for (Map.Entry<Integer, ArrayList<Integer>> entry : mainCopyLookup.entrySet()) {
			mainCopyLookupJSON.put(entry.getKey().toString(), entry.getValue());
		}
		config.put("main_copy_lookup", mainCopyLookupJSON);
		try {
			FileWriter fileWriter = new FileWriter(serializedJSONFileName);
			fileWriter.write(config.toString());
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private JSONObject open(String path, String name) {
		FileNode fileNode = fileHierarchy.openFile(path, name);
		if (fileNode == null)
			return null;

		JSONObject handle = new JSONObject();
		handle.put("handle", nextFileHandleId++);
		handle.put("offset", 0);
		handle.put("mode", -1);
		JSONObject fileInfo = new JSONObject();
		fileInfo.put("fileName", fileNode.fileName);
		fileInfo.put("fileType", fileNode.isDir ? 1 : 0);
		fileInfo.put("remotePath", fileNode.getPath());
		handle.put("fileInfo", fileInfo);

		JSONArray chunkList = new JSONArray();
		for (Integer chunkId : fileNode.chunkIDList) {
			JSONObject chunk = new JSONObject();
			ChunkInfo chunkInfo = chunkInfoList.get(chunkId);
			if (chunkInfo != null) {
				chunk.put("chunkId", chunkInfo.chunkId);
				chunk.put("slaveIP", chunkInfo.slaveIP);
				chunk.put("port", chunkInfo.port);
				chunk.put("fileIndex", chunkInfo.fileIndex);
				chunk.put("chunkLeft", chunkInfo.chunkLeft);
				chunkList.put(chunk);
			}
		}
		handle.put("chunkList", chunkList);
		return handle;
	}

	private boolean remove(String path, String name) throws UnknownHostException, IOException {
		return releaseFileNode(fileHierarchy.remove(path, name));
	}

	private JSONArray addChunk(String path, String name) throws UnknownHostException, IOException {
		FileNode fileNode = fileHierarchy.openFile(path, name);
		ArrayList<Integer> chunkIdList = new ArrayList<Integer>();
		ArrayList<ChunkInfo> tempChunkInfoList = createChunk(fileNode.getChunkSize(), chunkIdList);
		if (tempChunkInfoList == null)
			return null;
		ChunkInfo chunkInfo = null;
		for (int i = 0; i < tempChunkInfoList.size(); i++) {
			chunkInfo = tempChunkInfoList.get(i);
			chunkInfoList.put(chunkInfo.chunkId, chunkInfo);
		}
		fileNode.addChunk(chunkInfo.chunkId);
		mainCopyLookup.put(chunkInfo.chunkId, chunkIdList);
		JSONArray chunkList = new JSONArray();
		for (Integer chunkId : fileNode.chunkIDList) {
			JSONObject chunk = new JSONObject();
			chunkInfo = chunkInfoList.get(chunkId);
			chunk.put("chunkId", chunkInfo.chunkId);
			chunk.put("slaveIP", chunkInfo.slaveIP);
			chunk.put("port", chunkInfo.port);
			chunk.put("fileIndex", chunkInfo.fileIndex);
			chunk.put("chunkLeft", chunkInfo.chunkLeft);
			chunkList.put(chunk);
		}
		return chunkList;
	}

	private ArrayList<ChunkInfo> createChunk(int chunkIndexInFile, ArrayList<Integer> chunkIdList) {
		ArrayList<ChunkInfo> tempChunkInfoList = new ArrayList<ChunkInfo>();
		int originalNextChunkId = nextChunkId;
		for (int i = 0; i < numOfCopies; i++) {
			SlaveCommunication slave = slaves.get(i);// TODO choose a slave
			int currentId = nextChunkId++;
			try {
				if (i == numOfCopies - 1)
					slave.createChunk(currentId, true, chunkIdList);
				else
					slave.createChunk(currentId, false, null);
				tempChunkInfoList
						.add(new ChunkInfo(currentId, slave.getIP(), slaves.get(i).getPort(), chunkIndexInFile, 0));
				chunkIdList.add(currentId);
			} catch (IOException e) {
				nextChunkId = originalNextChunkId;
				return null;
			}
		}
		return null;
	}

	private boolean releaseFileNode(FileNode fileNode) throws UnknownHostException, IOException {
		if (fileNode.isDir) {
			if (fileNode.child != null)
				releaseFileNode(fileNode.child);
		} else {
			for (int chunkId : fileNode.chunkIDList) {
				return eraseChunk(chunkId);
			}
		}
		if (fileNode.brother != null)
			releaseFileNode(fileNode.brother);
		return true;
	}

	private boolean eraseChunk(int mainChunkId) throws UnknownHostException, IOException {
		ArrayList<Integer> chunkIds = mainCopyLookup.get(mainChunkId);
		for (int chunkId : chunkIds) {
			ChunkInfo chunkInfo = chunkInfoList.get(chunkId);
			if (chunkInfo != null) {
				SlaveCommunication slave = findSlaveWithIP(chunkInfo.slaveIP);
				if (slave == null)
					return false; // TODO if there is no slave, continue erasing
				if (!slave.removeChunk(chunkId))
					return false;
				else {
					chunkInfoList.remove(chunkId);
				}
			}
		}
		mainCopyLookup.remove(mainChunkId);
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
		Socket socket;
		InputStream in;
		OutputStream out;

		public ClientWorker(Socket socket) throws IOException {
			super();
			this.socket = socket;
			out = socket.getOutputStream();
			in = socket.getInputStream();
			protocol = Util.receiveProtocol(in);
		}

		@Override
		public void run() {
			try {
				String fullPath = null;
				int delimeter = 0;
				String path = null;
				String name = null;
				switch (protocol) {
				case VSFProtocols.OPEN_FILE:
					// 1. request file handle
					fullPath = Util.receiveString(in);
					delimeter = fullPath.lastIndexOf("/");
					path = fullPath.substring(0, delimeter);
					name = fullPath.substring(delimeter + 1);
					JSONObject fileHandle = open(path, name);
					if (fileHandle != null) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						Util.sendJSON(out, fileHandle);
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.REMOVE_FILE:
					// 2. remove file/folder?
					fullPath = Util.receiveString(in);
					delimeter = fullPath.lastIndexOf("/");
					path = fullPath.substring(0, delimeter);
					name = fullPath.substring(delimeter + 1);
					if (remove(path, name))
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
					else
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					break;
				case VSFProtocols.ADD_CHUNK:
					fullPath = Util.receiveString(in);
					delimeter = fullPath.lastIndexOf("/");
					path = fullPath.substring(0, delimeter);
					name = fullPath.substring(delimeter + 1);
					int chunkSize = Util.receiveInt(in);
					JSONArray array = null;
					for (int i = 0; i < chunkSize; i++) {
						array = addChunk(path, name);
					}
					if (array != null) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						Util.sendJSON(out, array);
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.MK_DIR:
					fullPath = Util.receiveString(in);
					delimeter = fullPath.lastIndexOf("/");
					path = fullPath.substring(0, delimeter);
					name = fullPath.substring(delimeter + 1);
					if (fileHierarchy.mkdir(path, name)) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.GET_FILE_NODE:
					Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
					Util.sendJSON(out, fileHierarchy.toJSON());
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
		// 1. slave rent request?
		// 2. heart beat request?
		try {
			ServerSocket serverSocket = new ServerSocket(8877); // port
			while (true) {
				Socket clientSocket = serverSocket.accept();
				ClientWorker clientWorker = master.new ClientWorker(clientSocket);
				clientWorker.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println();
				System.out.println("Exiting...");
				master.saveToJSONFile();
			}
		}));
	}

}
