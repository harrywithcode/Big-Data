
package JavaHDFS.JavaHDFS;
//cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

/*
 Usage: 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/20417.txt.bz2 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/5000-8.txt.bz2 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/132.txt.bz2 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/1661-8.txt.bz2 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/972.txt.bz2 
hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadBzipAndUploadHDFS http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2  hdfs://cshadoop1/user/gxm151030/assignment01/19699.txt.bz2 
 */

//vv FileCopyWithProgress
public class DownloadBzipAndUploadHDFS {
	public static void main(String[] args) throws Exception {
	 String downloadSrc = args[0];	
	 String dst = args[1];
 
	 //InputStream is = new URL("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2").openStream();
	 InputStream is = new URL(downloadSrc).openStream();
	 //InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
	 
	 Configuration conf = new Configuration();
	 conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	 conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	 
	 FileSystem fs = FileSystem.get(URI.create(dst), conf);
	 OutputStream out = fs.create(new Path(dst), new Progressable() {
	   public void progress() {
	     System.out.print(".");
	   }
	 });
 
	 IOUtils.copyBytes(is, out, 4096, true);
	 
	 // Decompress file
	 String decompFile = dst; 
	 Path decompPath = new Path(decompFile);
	 CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	 CompressionCodec codec = factory.getCodec(decompPath);
	 if (codec == null) {
		 System.err.println("No codec found for " + decompFile);
		 System.exit(1);
	 }

	 String outputUri =
	     CompressionCodecFactory.removeSuffix(decompFile, codec.getDefaultExtension());

	 try {
		 is = codec.createInputStream(fs.open(decompPath));
		 out = fs.create(new Path(outputUri));
		 IOUtils.copyBytes(is, out, conf);
	 } finally {
		 IOUtils.closeStream(is);
		 IOUtils.closeStream(out);
	 }	 
	 fs.delete(decompPath, true);
	 	 
	}
}
//^^ FileCopyWithProgress
