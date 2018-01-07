package vfs.master;

import java.util.ArrayList;

public class MainChunk {

	private static int leaseInitial = 1000;

	private int mainChunkId;

	private ArrayList<Integer> chunkIds;

	private int leaseLeft;

	private String filePath;

	public MainChunk(int mainChunkId, ArrayList<Integer> chunkIds, String filePath) {
		super();
		this.mainChunkId = mainChunkId;
		this.chunkIds = chunkIds;
		this.filePath = filePath;
		this.leaseLeft = leaseInitial;
	}

	public boolean decreaseLease() {
		if (--leaseLeft == 0)
			return true;
		else
			return false;
	}
	
	public void renewLease() {
		this.leaseLeft = leaseInitial;
	}

	public int changeMainChunck() {
		for (int i = 0; i < chunkIds.size(); ++i) {
			if (chunkIds.get(i) == mainChunkId)
				chunkIds.remove(i);
		}
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
