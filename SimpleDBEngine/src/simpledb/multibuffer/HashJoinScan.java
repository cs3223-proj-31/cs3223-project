package simpledb.multibuffer;

import java.util.HashMap;
import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinScan implements Scan {
	private Scan[] lhsscans, rhsscans;
	private String lhsjoinfld, rhsjoinfld;
	private Transaction tx;
	private Schema lhssch;
	
	private int scanpairidx;
	private Scan lhscurscan, rhscurscan, tempscan;
	private HashMap<Constant, UpdateScan> ht;
	
	public HashJoinScan(Scan[] lhsscans, Scan[] rhsscans, String lhsjoinfld, String rhsjoinfld, Transaction tx, Schema lhssch) {
		this.lhsscans = lhsscans;
		this.rhsscans = rhsscans;
		this.lhsjoinfld = lhsjoinfld;
		this.rhsjoinfld = rhsjoinfld;
		this.tx = tx;
		this.lhssch = lhssch;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		scanpairidx = 0;
		lhscurscan = lhsscans[scanpairidx];
		rhscurscan = rhsscans[scanpairidx];
		buildHashTableOnLhsPartScan();
	}
	
	public void buildHashTableOnLhsPartScan() {
		ht.clear();
		lhscurscan.beforeFirst();
		while (lhscurscan.next()) {
			Constant joinfieldval = lhscurscan.getVal(lhsjoinfld);
			ht.putIfAbsent(joinfieldval, new TempTable(tx, lhssch).open());
			UpdateScan mapscan = ht.get(joinfieldval);
			
			// Copy record of lhscurscan to tempscan.
			for (String fldname : lhssch.fields()) {
				mapscan.setVal(fldname, lhscurscan.getVal(fldname));
			}
			mapscan.insert();
		}
		
		// Reset pointers to point at the first record.
		for (Constant key : ht.keySet()) {
			ht.get(key).beforeFirst();
		}
	}
	
	@Override
	public boolean next() {
		while (tempscan == null || !tempscan.next()) {
			while (!rhscurscan.next()) {
				scanpairidx++;
				// Assume lhsscans.length == rhsscans.length
				if (scanpairidx >= lhsscans.length) {
					return false;
				}
				
				lhscurscan = lhsscans[scanpairidx];
				rhscurscan = rhsscans[scanpairidx];
				buildHashTableOnLhsPartScan();
				
				rhscurscan.beforeFirst();
			}
			
			// Get the tempscan from the hash table.
			Constant joinfieldval = rhscurscan.getVal(rhsjoinfld);
			tempscan = ht.get(joinfieldval);
			tempscan.beforeFirst();
		}
		
		return true;
	}

	@Override
	public int getInt(String fldname) {
		if (tempscan.hasField(fldname)) {
			return tempscan.getInt(fldname);
		}
		
		return rhscurscan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (tempscan.hasField(fldname)) {
			return tempscan.getString(fldname);
		}
		
		return rhscurscan.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (tempscan.hasField(fldname)) {
			return tempscan.getVal(fldname);
		}
		
		return rhscurscan.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return tempscan.hasField(fldname) || rhscurscan.hasField(fldname);
	}

	@Override
	public void close() {
		// Should cover tempscan.
		for (Constant key : ht.keySet()) {
			ht.get(key).close();
		}
		
		ht.clear();
		
		// Should cover lhscurscan and rhscurscan.
		for (int i = 0; i < lhsscans.length; i++) {
			lhsscans[i].close();
			rhsscans[i].close();
		}
	}

}
