package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.record.*;

public class BlockNestedJoinScan implements Scan {
    private Transaction tx;
    private Scan lhsscan, rhsscan = null;
    private ProductScan prodscan;
    private String filename, fldname1, fldname2;
    private Layout layout;
    private int chunksize, nextblknum, filesize;

    /**
     * Creates the scan class for the block bested join of the LHS scan and a table (RHS).
     * Sets chunk size as factor of table size
     * 
     * @param lhsscan the LHS scan
     * @param layout  the metadata for the RHS table
     * @param tx      the current transaction
     * @param fldname1 the field of the LHS table used in the join
     * @param fldname2 the field of the RHS table used in the join
     */
    public BlockNestedJoinScan(Transaction tx, Scan lhsscan, String tblname, Layout layout, String fldname1,
            String fldname2) {
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.tx = tx;
        this.lhsscan = lhsscan;
        this.filename = tblname + ".tbl";
        this.layout = layout;
        filesize = tx.size(filename);
        int available = tx.availableBuffs();
        chunksize = BufferNeeds.bestFactor(available, filesize);
        beforeFirst();
    }
    

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan is positioned at its first record,
     * and the RHS scan is positioned before the first record of the first chunk.
     * 
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        nextblknum = 0;
        useNextChunk();
    }

    /**
     * Moves to the next matching pair of records in the current scan.
     * If there are no more records in the current chunk,
     * then move to the next LHS record and the beginning of that chunk.
     * If there are no more LHS records, then move to the next chunk
     * and begin again.
     * 
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        while (!prodscan.next())
            if (!useNextChunk())
                return false;
        if(prodscan.getVal(fldname1).equals(prodscan.s2getVal(fldname2)))
            return true;
        return next();
    }

    /**
     * Closes the current scans.
     * 
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        prodscan.close();
    }

    /**
     * Returns the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        return prodscan.getVal(fldname);
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        return prodscan.getInt(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        return prodscan.getString(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * 
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return prodscan.hasField(fldname);
    }

    private boolean useNextChunk() {
        if (nextblknum >= filesize)
            return false;
        if (rhsscan != null)
            rhsscan.close();
        int end = nextblknum + chunksize - 1;
        if (end >= filesize)
            end = filesize - 1;
        rhsscan = new ChunkScan(tx, filename, layout, nextblknum, end);
        lhsscan.beforeFirst();
        prodscan = new ProductScan(lhsscan, rhsscan);
        nextblknum = end + 1;
        return true;
    }
}
