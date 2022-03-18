package simpledb.multibuffer;

import java.util.HashMap;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinScan implements Scan {
	private TempTable[] lhstts, rhstts;
	private String lhsjoinfld, rhsjoinfld;
	private Transaction tx;
	private Schema lhssch;
	
	private int scanpairidx;
	private Scan lhscurscan, rhscurscan, tempscan;
	private HashMap<Constant, UpdateScan> ht;
	
	public HashJoinScan(TempTable[] lhstts, TempTable[] rhstts, String lhsjoinfld, String rhsjoinfld, Transaction tx, Schema lhssch) {
		this.lhstts = lhstts;
		this.rhstts = rhstts;
		this.lhsjoinfld = lhsjoinfld;
		this.rhsjoinfld = rhsjoinfld;
		this.tx = tx;
		this.lhssch = lhssch;
		ht = new HashMap<>();
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		if (lhscurscan != null) {
			lhscurscan.close();
		}
		
		if (rhscurscan != null) {
			rhscurscan.close();
		}
		
		scanpairidx = 0;
		lhscurscan = lhstts[scanpairidx].open();
		rhscurscan = rhstts[scanpairidx].open();
		buildHashTableOnLhsPartScan();
	}
	
	public void buildHashTableOnLhsPartScan() {
		for (Constant key : ht.keySet()) {
			ht.get(key).close();
		}
		
		ht.clear();
		lhscurscan.beforeFirst();
		while (lhscurscan.next()) {
			Constant joinfieldval = lhscurscan.getVal(lhsjoinfld);			
			ht.putIfAbsent(joinfieldval, new TempTable(tx, lhssch).open());
			UpdateScan mapscan = ht.get(joinfieldval);
			
			// Copy record of lhscurscan to tempscan.
			for (String fldname : lhssch.fields()) {
				mapscan.insert();
				mapscan.setVal(fldname, lhscurscan.getVal(fldname));
			}
		}
		
		// Reset pointers to point at the first record.
		for (Constant key : ht.keySet()) {
			ht.get(key).beforeFirst();
		}
		
		lhscurscan.close();
	}
	
	@Override
	public boolean next() {
		while (tempscan == null || !tempscan.next()) {			
			while (!rhscurscan.next()) {
				rhscurscan.close();
				
				scanpairidx++;
				// Assume lhsscans.length == rhsscans.length
				if (scanpairidx >= lhstts.length) {
					return false;
				}
				
				lhscurscan = lhstts[scanpairidx].open();
				rhscurscan = rhstts[scanpairidx].open();
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
	}

}
