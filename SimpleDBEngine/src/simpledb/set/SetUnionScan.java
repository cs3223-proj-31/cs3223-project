package simpledb.set;

import java.util.HashSet;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class SetUnionScan implements Scan {
	private Scan lhsscan;
	private Scan rhsscan;
	private Schema lhssch;
	private Schema rhssch;
	private HashSet<Integer> set;
	
	public SetUnionScan(Scan lhsscan, Scan rhsscan, Schema lhssch, Schema rhssch) {
		this.lhsscan = lhsscan;
		this.rhsscan = rhsscan;
		this.lhssch = lhssch;
		this.rhssch = rhssch;
		set = new HashSet<>();
	}

	@Override
	public void beforeFirst() {
		// TODO Auto-generated method stub
		lhsscan.beforeFirst();
		rhsscan.beforeFirst();
		set.clear();
	}

	@Override
	public boolean next() {
		if (lhsscan.next()) {
			set.add(hash(lhsscan, lhssch));
			return true;
		}
		
		while (rhsscan.next()) {
			if (!set.contains(hash(rhsscan, rhssch)))
				return true;
		}
		
		return false;
	}

	@Override
	public int getInt(String fldname) {
		if (lhsscan.hasField(fldname))
			return lhsscan.getInt(fldname);
		return rhsscan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (lhsscan.hasField(fldname))
			return lhsscan.getString(fldname);
		return rhsscan.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (lhsscan.hasField(fldname))
			return lhsscan.getVal(fldname);
		return rhsscan.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return lhsscan.hasField(fldname) || rhsscan.hasField(fldname);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		rhsscan.close();
		lhsscan.close();
		set.clear();
	}
	
	private int hash(Scan s, Schema sch) {
		int hashKey = 0;
		for (String fldname : sch.fields()) {
			hashKey += s.getVal(fldname).hashCode();
		}
		return hashKey;
	}
}
