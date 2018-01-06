package vfs.master;

import java.util.ArrayList;

import vfs.struct.FileNode;

public class MainChunk {

	private static int rentTimeLeftInitial = 1000;

	private int mainChunkId;

	private ArrayList<Integer> chunkIds;

	private int rentTimeLeft;
	
	private String filePath;

	public MainChunk(int mainChunkId, ArrayList<Integer> chunkIds, String filePath) {
		super();
		this.mainChunkId = mainChunkId;
		this.chunkIds = chunkIds;
		this.filePath = filePath;
		this.rentTimeLeft = rentTimeLeftInitial;
	}

	public boolean decreaseRentTime() {
		if (--rentTimeLeft == 0)
			return true;
		else
			return false;
	}

	public int changeMainChunck() {
		chunkIds.remove(mainChunkId);
		if (chunkIds.isEmpty())
			return -1;
		mainChunkId = chunkIds.get(0);
		return mainChunkId;
	}

	public int getMainChunkId() {
		return mainChunkId;
	}

	public ArrayList<Integer> getChunkIds() {
		return chunkIds;
	}

	public String getFilePath() {
		return filePath;
	}

}
