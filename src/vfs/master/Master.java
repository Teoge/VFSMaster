package vfs.master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;
import vfs.struct.FileNode;

public class Master {

	private FileNode root;

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
				slaves.remove(slave);
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
			root = new FileNode();
			root.parseJSON(config.getJSONObject("file_hierarchy"), null);
			nextFileHandleID = config.getInt("next_file_handle_id");
			nextChunkID = config.getInt("next_chunk_id");
			JSONArray slavesArray = config.getJSONArray("slaves");
			slaves = new ArrayList<SlaveCommunication>();
			for (int i = 0; i < slavesArray.length(); i++) {
				JSONObject obj = slavesArray.getJSONObject(i);
				SlaveCommunication slave = new SlaveCommunication(obj.getString("IP"), obj.getInt("port"));
				slaves.add(slave);
			}
		} catch (FileNotFoundException | JSONException e) {
			root = new FileNode("vfs", true, null);
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
		config.put("file_hierarchy", root.toJSON());
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

	private boolean mkdir(String path, String dirName) {
		FileNode fileNode = pathToFileNode(path);
		if (fileNode == null)
			return false;
		FileNode parent = fileNode;
		fileNode = fileNode.child;
		while (fileNode != null) {
			fileNode = fileNode.brother;
		}
		fileNode = new FileNode(dirName, true, parent);
		return true;
	}

	private JSONObject open(String path, String mode) {
		FileNode fileNode = pathToFileNode(path);
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

	private FileNode pathToFileNode(String path) {
		String[] names = path.split("[/\\\\]");
		FileNode fileNode = root;
		for (int i = 1; i < names.length; i++) {
			fileNode = fileNode.findChildWithName(names[i]);
			if (fileNode == null)
				return null;
		}
		return fileNode;
	}

//	private static void testWrite() {
//		FileNode test = new FileNode("vfs", true, null);
//		test.child = new FileNode("a", true, test);
//		test.child.brother = new FileNode("b", false, test);
//		test.child.brother.fileChunkTable = new HashMap<Integer, ChunkInfo>();
//		test.child.brother.fileChunkTable.put(1, new ChunkInfo(1, "10.60.41.1", 8888, 232, 234));
//		test.child.brother.fileChunkTable.put(2, new ChunkInfo(2, "10.65.7.1", 8888, 786, 231));
//		test.child.child = new FileNode("c", false, test.child);
//		test.child.child.fileChunkTable.put(1, new ChunkInfo(1, "10.60.255.1", 8888, 45, 689));
//		try {
//			FileWriter fileWriter = new FileWriter(serializedJSONFileName);
//			fileWriter.write(test.toJSON().toString());
//			fileWriter.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

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
			
		}
	}

}
