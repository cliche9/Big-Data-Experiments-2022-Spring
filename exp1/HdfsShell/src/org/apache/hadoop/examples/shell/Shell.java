package org.apache.hadoop.examples.shell;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

public class Shell {
	private FileSystem hdfs;
	private Scanner scanner;
	
	public Shell(FileSystem remote, Scanner scanner) {
		this.hdfs = remote;
		this.scanner = scanner;
	}

	public static void main(String[] args) throws Exception {
		String opt = args[1], src, dst, params;

		Configuration conf = new Configuration();
		Shell shell = new Shell(FileSystem.get(conf), new Scanner(System.in));
		// switch-case for different opts
		for (int i = 0; i < args.length; i++) {
			String str = String.format("%d: %s", i, args[i]);
			System.out.println(str);
		}

		switch (opt) {
			case "put":
				src = args[2];
				dst = args[3];
				shell.put(new Path(src), new Path(dst));
				break;
			case "get":
				src = args[2];
				dst = args[3];
				shell.get(new Path(src), new Path(dst));
				break;
			case "cat":
				src = args[2];
				shell.cat(new Path(src));
				break;
			case "ls":
				if (args[2].equals("-R")) {
					params = args[2];
					src = args[3];
				} else {
					params = "";
					src = args[2];
				}
				shell.ls(new Path(src), params);
				break;
			case "createAndRemoveFile":
				shell.createAndRemoveFile(new Path(args[2]), new Path(args[3]));
				break;
			case "createAndRemoveDir":
				shell.createAndRemoveDir(new Path(args[2]));
				break;
			case "appendToFile":
				src = args[2];
				dst = args[3];
				shell.appendToFile(new Path(src), new Path(dst));
				break;
			case "rm":
				shell.rm(new Path(args[2]));
				break;
			case "mv":
				src = args[2];
				dst = args[3];
				shell.mv(new Path(src), new Path(dst));
				break;
		}
	}
	
	public void put(Path src, Path dst) {
		try {
			if (hdfs.exists(dst)) {
				// dst exists in fileSystem
				System.out.println(dst + " exists.\nDo you want to overwrite(y) or append(n) the existed file? (y/n)");
				if (scanner.next().equals("y")) {
					// overwrite
					hdfs.copyFromLocalFile(false, true, src, dst);
				} else {
					// append
					FileInputStream inputStream = new FileInputStream(src.toString());
					FSDataOutputStream outputStream = hdfs.append(dst);
					byte[] bytes = new byte[1024];
					int read = -1;
					while ((read = inputStream.read(bytes)) > 0) {
						outputStream.write(bytes, 0, read);
					}
					inputStream.close();
					outputStream.close();
				}
			} else {
				// dst not exists
				hdfs.copyFromLocalFile(src, dst);
				System.out.println("Copy successfully.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void get(Path src, Path dst) {
		try {
			File f = new File(dst.toString());
			if (hdfs.exists(src) && !f.exists()) {
				// src exists and dst not exists
				hdfs.copyToLocalFile(src, dst);
			} else if (hdfs.exists(src)) {
				// both src and dst exist the same file --- rename the copy 
				String[] files = dst.toString().split("\\.");
				System.out.println(dst.toString() + " exists.");
				System.out.println("copy to " + files[0] + "_copy." + files[1]);
				hdfs.copyToLocalFile(src, new Path(files[0] + "_copy." + files[1]));
			} else {
				// src not exists --- error
				System.out.println(src + " not exists.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void cat(Path src) {
		try {
			if (!hdfs.exists(src)) {
				// src not exists
				System.out.println(src + " not exists.");
			} else {
				// src exists, cat its content
				FSDataInputStream in = hdfs.open(src);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					System.out.println(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ls(Path src, String params) {
		RemoteIterator<LocatedFileStatus> iterator;
		try {
			iterator = hdfs.listLocatedStatus(src);
			while (iterator.hasNext()) {
				FileStatus status = iterator.next();
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				System.out.println(
					status.getPermission().toString() + '\t' +
					status.getReplication() + '\t' + 
					status.getOwner() + '\t' +
					status.getGroup() + '\t' +
					status.getLen() + '\t' +
					format.format(status.getModificationTime()) + '\t' +
					status.getPath().getName()
				);
			}
		} catch (FileNotFoundException e) {
			System.out.println(src + " not exists.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createAndRemoveFile(Path dirPath, Path filePath) {
		filePath = new Path(dirPath.toString() + Path.SEPARATOR + filePath.toString());
		try {
			System.out.println("Do you want to create(y) or delete(n) the file? (y/n)");
			if (scanner.next().equals("y")){
				if (!hdfs.exists(dirPath)) {
					System.out.println(dirPath + " not exists.");
					hdfs.mkdirs(dirPath);
					System.out.println("mkdir " + dirPath + ", now it exists.");
				}
				hdfs.create(filePath);
				System.out.println("created " + filePath);
			} else {
				if (!hdfs.exists(filePath)) {
					System.out.println(filePath + " not exists.");
				} else {
					hdfs.delete(filePath, true);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createAndRemoveDir(Path dirPath) {
		// only remove file
		try {
			System.out.println("Do you want to create(y) or delete(n) the directory? (y/n)");
			if (scanner.next().equals("y")){
				if (hdfs.exists(dirPath)) {
					System.out.println(dirPath + " has existed.");
				} else {
					System.out.println(dirPath + " not exists. now create it.");
					hdfs.mkdirs(dirPath);
					System.out.println("created " + dirPath);
				}
			} else {
				if (!hdfs.exists(dirPath)) {
					System.out.println(dirPath + " not exists.");
				} else {
					FileStatus[] fileStatus = hdfs.listStatus(dirPath);
					if (fileStatus.length == 0) {
						hdfs.delete(dirPath, false);
						System.out.println(dirPath + " is empty and deleted");
					} else {
						System.out.println(dirPath + " is not empty and not deleted");
						for (FileStatus status : fileStatus) {
							System.out.println(status.getPath().getName());
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void appendToFile(Path src, Path dst) {
		try {
			File f = new File(src.toString());
			if (!hdfs.exists(dst) || !f.exists()) {
				System.out.println(src + " or " + dst + " not exists.");
			} else {
				System.out.println("Do you want to add content at the start(y) or end(n) of the file? (y/n)");
				if (scanner.next().equals("y")) {
					// inputString of two files
					FileInputStream srcin = new FileInputStream(src.toString());
					FSDataInputStream dstin = hdfs.open(dst);
					hdfs.delete(dst, false);
					// outputString of final file;
					FSDataOutputStream dstout = hdfs.create(dst);
					dstout.close();
					// appending mode
					dstout = hdfs.append(dst);
					byte[] data = new byte[1024];
					int read = -1;
					while ((read = srcin.read(data)) > 0){
						dstout.write(data, 0, read);
					}
					while ((read = dstin.read(data)) > 0){
						dstout.write(data, 0, read);
					}
					srcin.close();
					dstin.close();
					dstout.close();
					System.out.println("appendToFile of Head Success");
				} else {
					FileInputStream srcin = new FileInputStream(src.toString());
					FSDataOutputStream dstout = hdfs.append(dst);
					byte[] data = new byte[1024];
					int read = -1;
					while ((read = srcin.read(data)) > 0){
						dstout.write(data, 0, read);
					}
					srcin.close();
					dstout.close();
					System.out.println("appendToFile of Tail Success");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void rm(Path filePath) {
		try {
			if (!hdfs.exists(filePath)) {
				System.out.println("rm: \'" + filePath + "\': no such file or dirctory");
			} else {
				hdfs.delete(filePath, true);
				System.out.println("rm: \'" + filePath + "\': is deleted");
			}
		} catch (Exception e) {
			System.out.println("failed to delete " + filePath);
		}
	}

	public void mv(Path src, Path dst) {
		try {
			if (!hdfs.exists(src)) {
				System.out.println("mv: \'" + src + "\': no such file or dirctory");
			} else {
				hdfs.rename(src, dst);
				System.out.println("mv: \'" + src + "\' is moved to " + dst);
			}
		} catch (Exception e) {
			System.out.println("failed to move.");
		}
	}
}