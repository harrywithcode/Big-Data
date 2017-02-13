package JavaHDFS.JavaHDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class DownloadZipAndUploadHDFS{
	
	public static int FILES_PROCESSED = 0;
	
	/*
	 Usage:
	 hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.DownloadZipAndUploadHDFS 
	 	http://corpus.byu.edu/glowbetext/samples/text.zip  
	 	/home/011/g/gx/gxm151030/ass01/part2/text  
	 	hdfs://cshadoop1/user/gxm151030/assignment01/	 
	 */
	

    public static void main(String args[]) throws Exception{
    	DownloadZipAndUploadHDFS driver = new DownloadZipAndUploadHDFS();

    	String webURL = args[0];
    	String unzipFolder = args[1];
    	String hadoopFolder = args[2];
    	
        driver.unzip(webURL,unzipFolder);
    	driver.uploadFromLocal(unzipFolder,hadoopFolder );
    }
    
	public void uploadFromLocal(String localFileFolder, String destination) throws IOException{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

		FileSystem fs = FileSystem.get(conf);
		
		File[] files = new File(localFileFolder).listFiles();	
		for (File file : files) {
		    if (file.isFile()) {
		    	//System.out.println(file.getName());		    	
		    	fs.copyFromLocalFile(new Path(localFileFolder + '/' + file.getName()), new Path(destination + "/" + file.getName()));
		    }
		}
	}    
    
   // public void recursive_extract(ZipFile zipFile, ZipInputStream inStream, OutputStream outStream)
    public void unzip(String webURL, String unzipFolder){   	
  	
        byte[] buffer = new byte[1024];
        try{
        	/* create output directory if the directory is not exists */
	       	File folder = new File(unzipFolder);
	       	if(!folder.exists()){
	       		folder.mkdir();
       	}

		ZipInputStream zipIn = new ZipInputStream(new URL(webURL).openStream());
				
       	ZipEntry ze = zipIn.getNextEntry();

       	while(ze!=null){
       		String fileName = ze.getName();
            File newFile = new File(unzipFolder + File.separator + fileName);

            System.out.println("file unzip : "+ newFile.getAbsoluteFile());
            FILES_PROCESSED++;
            //create all non exists folders
            //else you will hit FileNotFoundException for compressed folder
            new File(newFile.getParent()).mkdirs();
            FileOutputStream fos = new FileOutputStream(newFile);

            int len;
            while ((len = zipIn.read(buffer)) > 0) {
            	fos.write(buffer, 0, len);
            }
            fos.close();
            ze = zipIn.getNextEntry();
       	}

       	zipIn.closeEntry();
       	zipIn.close();

       	System.out.println("Done");

       }catch(IOException ex){
          ex.printStackTrace();
       }
      }    
}