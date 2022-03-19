package simpledb.multibuffer;

import java.util.HashMap;
import simpledb.materialize.TempTable;
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
	private Scan lhscurscan, rhscurscan, htcurscan;
	private HashMap<Constant, TempTable> ht;
	
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
		ht.clear();
		lhscurscan.beforeFirst();
		while (lhscurscan.next()) {
			Constant joinfieldval = lhscurscan.getVal(lhsjoinfld);			
			ht.putIfAbsent(joinfieldval, new TempTable(tx, lhssch));
			UpdateScan mapscan = ht.get(joinfieldval).open();
			
			// Move pointer to last record.
			while (mapscan.next());
			
			mapscan.insert();
			// Copy record of lhscurscan to tempscan.
			for (String fldname : lhssch.fields()) {
				mapscan.setVal(fldname, lhscurscan.getVal(fldname));
			}
			mapscan.close();
		}		
		lhscurscan.close();
	}
	
	@Override
	public boolean next() {
		while (htcurscan == null || !htcurscan.next()) {
			if (htcurscan != null)
				htcurscan.close();
			
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
			htcurscan = ht.containsKey(joinfieldval)
					       ? ht.get(joinfieldval).open()
					       : null;
			
			if (htcurscan != null)
				htcurscan.beforeFirst();
		}
		
		return true;
	}

	@Override
	public int getInt(String fldname) {
		if (htcurscan.hasField(fldname)) {
			return htcurscan.getInt(fldname);
		}
		
		return rhscurscan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (htcurscan.hasField(fldname)) {
			return htcurscan.getString(fldname);
		}
		
		return rhscurscan.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (htcurscan.hasField(fldname)) {
			return htcurscan.getVal(fldname);
		}
		
		return rhscurscan.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return htcurscan.hasField(fldname) || rhscurscan.hasField(fldname);
	}

	@Override
	public void close() {
		// Close the hash table scan if it is open.
		try {
			htcurscan.close();
		} catch (Exception e) {
			// Do nothing.
		}
		ht.clear();		
	}

}
