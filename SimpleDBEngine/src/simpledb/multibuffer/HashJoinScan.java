package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class HashJoinScan implements Scan {
	private Scan[] subscans;
	private Scan currentscan;
	private int currentscanidx;
	
	public HashJoinScan(Scan[] subscans) {
		this.subscans = subscans;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		// TODO Auto-generated method stub
		currentscanidx = 0;
		currentscan = subscans[currentscanidx];
		currentscan.beforeFirst();
	}

	@Override
	public boolean next() {
		while (!currentscan.next()) {
			currentscan.close();
			currentscanidx++;
			if (currentscanidx >= subscans.length)
				return false;
			currentscan = subscans[currentscanidx];
			currentscan.beforeFirst();
		}
		return true;
	}

	@Override
	public int getInt(String fldname) {
		// TODO Auto-generated method stub
		return currentscan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		// TODO Auto-generated method stub
		return currentscan.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		// TODO Auto-generated method stub
		return currentscan.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		// TODO Auto-generated method stub
		return currentscan.hasField(fldname);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		for (int i = currentscanidx; i < subscans.length; i++)
			subscans[i].close();
	}

}
