


import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

  // a static method that loads the RocksDB C++ library.
public class TestRocksJava {
	static {
		  RocksDB.loadLibrary();
	}
	
	public static void main(String[] args) {
	  // the Options class contains a set of configurable DB options
	  // that determines the behavior of a database.
	  Options options = new Options().setCreateIfMissing(true);
	  RocksDB db = null;
	  try {
	    // a factory method that returns a RocksDB instance
	    System.out.println("Hello World");
		db = RocksDB.open(options, "/tmp/testdb");
	    // do something
	    
	    if (db != null) db.close();
	    options.dispose();
	    
	  } //catch (RocksDBException e) {
	  catch (Exception e) {
	    // do some error handling
	  }
	  
	}
  
}
  
