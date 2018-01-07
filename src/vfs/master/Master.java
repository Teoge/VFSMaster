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
import java.util.concurrent.TimeUnit;
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

	private ArrayList<SlaveSocket> slaves;

	private HashMap<Integer, ChunkInfo> chunkInfoList;

	private HashMap<Integer, MainChunk> mainChunkList;

	private static String serializedJSONFileName = "configuration.json";

	private static int numOfCopies = 3;

	public Master() {
		readFromJSONFile();
		chunkInfoList = new HashMap<Integer, ChunkInfo>();
		for (SlaveSocket slave : slaves) {
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
		new HeartBeatDetector().start();
		new RentImpl().start();
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
			slaves = new ArrayList<SlaveSocket>();
			for (int i = 0; i < slavesArray.length(); i++) {
				JSONObject obj = slavesArray.getJSONObject(i);
				SlaveSocket slave = new SlaveSocket(obj.getString("ip"), obj.getInt("port"));
				slaves.add(slave);
			}
			mainChunkList = new HashMap<Integer, MainChunk>();
			JSONObject mainChunkListJSON = config.getJSONObject("main_copy_lookup");
			for (String key : mainChunkListJSON.keySet()) {
				JSONObject mainChunkJSON = mainChunkListJSON.getJSONObject(key);
				JSONArray idsJSON = mainChunkJSON.getJSONArray("chunk_ids");
				ArrayList<Integer> ids = new ArrayList<Integer>();
				for (int i = 0; i < idsJSON.length(); i++) {
					ids.add(idsJSON.getInt(i));
				}
				mainChunkList.put(Integer.parseInt(key),
						new MainChunk(Integer.parseInt(key), ids, mainChunkJSON.getString("file_path")));
			}
		} catch (FileNotFoundException | JSONException e) {
			fileHierarchy = new FileHierarchy();
			nextFileHandleId = 0;
			nextChunkId = 0;
			slaves = new ArrayList<SlaveSocket>();
			mainChunkList = new HashMap<Integer, MainChunk>();
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
		for (SlaveSocket slave : slaves) {
			JSONObject obj = new JSONObject();
			obj.put("ip", slave.IP);
			obj.put("port", slave.port);
			slavesArray.put(obj);
		}
		config.put("slaves", slavesArray);
		JSONObject mainChunkListJSON = new JSONObject();
		for (Map.Entry<Integer, MainChunk> entry : mainChunkList.entrySet()) {
			JSONObject mainChunkJSON = new JSONObject();
			JSONArray ids = new JSONArray();
			for (int id : entry.getValue().getChunkIds()) {
				ids.put(id);
			}
			mainChunkJSON.put("file_path", entry.getValue().getFilePath());
			mainChunkJSON.put("chunk_ids", ids);
			mainChunkListJSON.put(entry.getKey().toString(), mainChunkJSON);
		}
		config.put("main_chunk_list", mainChunkListJSON);
		try {
			FileWriter fileWriter = new FileWriter(serializedJSONFileName);
			fileWriter.write(config.toString());
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private JSONObject open(String path) {
		int delimeter = path.lastIndexOf("/");
		FileNode fileNode = fileHierarchy.openFile(path.substring(0, delimeter), path.substring(delimeter + 1));
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

	private boolean remove(String path) throws UnknownHostException, IOException {
		int delimeter = path.lastIndexOf("/");
		return releaseFileNode(fileHierarchy.remove(path.substring(0, delimeter), path.substring(delimeter + 1)));
	}

	private JSONArray addChunk(String path) throws UnknownHostException, IOException {
		int delimeter = path.lastIndexOf("/");
		FileNode fileNode = fileHierarchy.openFile(path.substring(0, delimeter), path.substring(delimeter + 1));
		if (fileNode == null)
			return null;
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
		mainChunkList.put(chunkInfo.chunkId, new MainChunk(chunkInfo.chunkId, chunkIdList, path));
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
			SlaveSocket slave = slaves.get(i);// TODO choose a slave
			int currentId = nextChunkId++;
			try {
				if (i == numOfCopies - 1)
					slave.createChunk(currentId, true, tempChunkInfoList);
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
		return tempChunkInfoList;
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
		ArrayList<Integer> chunkIds = mainChunkList.get(mainChunkId).getChunkIds();
		for (int chunkId : chunkIds) {
			ChunkInfo chunkInfo = chunkInfoList.get(chunkId);
			if (chunkInfo != null) {
				SlaveSocket slave = findSlaveWithIP(chunkInfo.slaveIP);
				if (slave == null)
					return false; // TODO if there is no slave, continue erasing
				if (!slave.removeChunk(chunkId))
					return false;
				else {
					chunkInfoList.remove(chunkId);
				}
			}
		}
		mainChunkList.remove(mainChunkId);
		return true;
	}

	private SlaveSocket findSlaveWithIP(String IP) {
		for (SlaveSocket slave : slaves) {
			if (slave.IP.equals(IP)) {
				return slave;
			}
		}
		return null;
	}

	private void alterMainChunk(int mainChunkId) {
		MainChunk mainChunk = mainChunkList.get(mainChunkId);
		int newMainChunkId = mainChunk.changeMainChunck();
		String path = mainChunk.getFilePath();
		int delimeter = path.lastIndexOf("/");
		String folderPath = path.substring(0, delimeter);
		String fileName = path.substring(delimeter + 1);
		FileNode fileNode = fileHierarchy.openFile(folderPath, fileName);
		if (fileNode != null) {
			if (newMainChunkId == -1) {
				fileHierarchy.remove(folderPath, fileName);
				return;
			}
			fileNode.removeChunk(mainChunkId);
			fileNode.addChunk(newMainChunkId);
		}
		mainChunkList.put(newMainChunkId, mainChunkList.remove(mainChunkId));
		chunkInfoList.remove(mainChunkId);
		try {
			findSlaveWithIP(chunkInfoList.get(newMainChunkId).slaveIP).assignNewMainChunk(newMainChunkId);
		} catch (IOException e) {
			fileHierarchy.remove(folderPath, fileName);
		}
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
				String path = null;
				switch (protocol) {
				case VSFProtocols.OPEN_FILE:
					// 1. request file handle
					JSONObject fileHandle = open(Util.receiveString(in));
					if (fileHandle != null) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						Util.sendJSON(out, fileHandle);
						saveToJSONFile();
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.REMOVE_FILE:
					// 2. remove file/folder?
					if (remove(Util.receiveString(in))) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						saveToJSONFile();
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.ADD_CHUNK:
					path = Util.receiveString(in);
					int chunkSize = Util.receiveInt(in);
					JSONArray array = null;
					for (int i = 0; i < chunkSize; i++) {
						array = addChunk(path);
					}
					if (array != null) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						Util.sendJSON(out, array);
						saveToJSONFile();
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.MK_DIR:
					path = Util.receiveString(in);
					int delimeter = path.lastIndexOf("/");
					if (fileHierarchy.mkdir(path.substring(0, delimeter), path.substring(delimeter + 1))) {
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
						saveToJSONFile();
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
					break;
				case VSFProtocols.GET_FILE_NODE:
					Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
					Util.sendJSON(out, fileHierarchy.toJSON());
					break;
				case VSFProtocols.RENEW_LEASE:
					MainChunk mainChunk = mainChunkList.get(Util.receiveInt(in));
					if (mainChunk != null) {
						mainChunk.renewLease();
						Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
					} else {
						Util.sendSignal(out, VSFProtocols.MASTER_REJECT);
					}
				case VSFProtocols.HEART_BEAT_DETECT_TO_MASTER:
					Util.sendSignal(out, VSFProtocols.MESSAGE_OK);
				default:
					break;
				}
			} catch (ConnectException e) {
				System.out.println("Connection timeout.");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public class RentImpl extends Thread {

		@Override
		public void run() {
			super.run();
			while (true) {
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				ArrayList<Integer> invaildChunkIds = new ArrayList<Integer>();
				for (Map.Entry<Integer, MainChunk> entry : mainChunkList.entrySet()) {
					if (entry.getValue().decreaseLease()) {
						invaildChunkIds.add(entry.getKey());
					}
				}
				for (int invaildChunkId : invaildChunkIds) {
					alterMainChunk(invaildChunkId);
				}
			}
		}

	}

	public class HeartBeatDetector extends Thread {

		private static final int heartBeatDetectInterval = 60;

		@Override
		public void run() {
			while (true) {
				try {
					TimeUnit.SECONDS.sleep(heartBeatDetectInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (SlaveSocket slave : slaves) {
					try {
						if (slave.detectHeartBeat())
							continue;
						else
							throw new ConnectException();
					} catch (ConnectException e) {
						System.out.println("Slave " + slave.IP + ":" + slave.port + " not responding.");
						ArrayList<Integer> invaildChunkIds = new ArrayList<Integer>();
						for (int mainChunkId : mainChunkList.keySet()) {
							ChunkInfo chunkInfo = chunkInfoList.get(mainChunkId);
							if (chunkInfo.slaveIP.equals(slave.IP) && chunkInfo.port == slave.port) {
								invaildChunkIds.add(mainChunkId);
							}
						}
						for (int invaildChunkId : invaildChunkIds) {
							alterMainChunk(invaildChunkId);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
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
