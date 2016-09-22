package com.ak.minicluster.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class HDFSMiniClusterDemo {
	
	//https://github.com/sakserv/hadoop-mini-clusters

	private static HdfsLocalCluster dfsCluster;

	public static void main(String[] args) throws Exception {

		dfsCluster = new HdfsLocalCluster.Builder()
				.setHdfsNamenodePort(Integer.valueOf(20112))
				.setHdfsTempDir("embedded_hdfs")
				.setHdfsNumDatanodes(Integer.valueOf(1))
				.setHdfsEnablePermissions(Boolean.valueOf(false))
				.setHdfsFormat(Boolean.valueOf(true))
				.setHdfsConfig(new Configuration()).build();
		dfsCluster.start();

		HDFSMiniClusterDemo m = new HDFSMiniClusterDemo();
		m.writeFileToHdfs("sample.txt", "these are all test contents");
		String content = m.readFileFromHdfs("sample.txt");
		System.out.println("CONTENT : " + content);
		dfsCluster.stop();

	}

	private void writeFileToHdfs(String fileName, String contents)
			throws Exception {
		// Write a file to HDFS containing the test string
		FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
		FSDataOutputStream writer = hdfsFsHandle.create(new Path(fileName));
		writer.writeUTF(contents);
		writer.close();
		hdfsFsHandle.close();
	}

	private String readFileFromHdfs(String filename) throws Exception {
		FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
		FSDataInputStream reader = hdfsFsHandle.open(new Path(filename));
		String output = reader.readUTF();
		reader.close();
		hdfsFsHandle.close();
		return output;
	}

	
}
