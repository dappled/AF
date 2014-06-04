package exporter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import utils.GeneralImporterExporter;

/**
 * @author Zhenghong Dong
 */
public abstract class ExporterBase extends GeneralImporterExporter {
	/***********************************************************************
	 * Constructor
	 ***********************************************************************/
	public ExporterBase(String dbServer, String catalog) {
		super( dbServer, catalog );
	}

	/***********************************************************************
	 * Utilities
	 ***********************************************************************/
	/**
	 * Generate report and save as outFile for certain date
	 * @param outFile
	 * @param date
	 * @throws Exception
	 */
	public abstract void report(final String inFile, final String outFile, final String date, final String ftpAddress) throws Exception;

	/**
	 * Upload files to ftp
	 * @param outFile files to be uploaded, should be seperated by ; 
	 * @param ftpAddress
	 */
	public void uploadFtp(String outFile, String ftpAddress, String ftpUserName, String ftpPassword) {
		FTPClient client = new FTPClient();
		FileInputStream fis = null;

		int reply;
		boolean upload;
		try {
			client.connect( ftpAddress );
			client.login( ftpUserName, ftpPassword );
			reply = client.getReplyCode();

			if (!FTPReply.isPositiveCompletion( reply )) {
				client.disconnect();
				System.err.println( "FTP server refused connection." );
				return;
			} else {
				for (String file : outFile.split( ";" )) {
					if (file.isEmpty()) continue;
					fis = new FileInputStream( file );
					//System.out.println( file.substring( file.lastIndexOf( "\\" ) + 1 ) );
					upload = client.storeFile(file.substring( file.lastIndexOf( "\\" ) + 1 ), fis);
					if (upload == false) {
						System.err.println("Fail to upload " + file.substring( file.lastIndexOf( "\\" ) + 1 ));
					}
					System.out.println("Upload " + file.substring( file.lastIndexOf( "\\" ) + 1 ) + " successfully");
					fis.close();
				}
			}
			client.logout();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * download files from ftp to C:\Temp
	 * @param outFile files to be downloaded, should be seperated by ; 
	 * @param ftpAddress
	 * @return 
	 */
	public String downloadFtp(String inFile, String ftpAddress, String ftpUserName, String ftpPassword) {
		FTPClient client = new FTPClient();
		String re = null;
		
		int reply;
		try {
			client.connect( ftpAddress );
			client.login( ftpUserName, ftpPassword );
			reply = client.getReplyCode();

			if (!FTPReply.isPositiveCompletion( reply )) {
				client.disconnect();
				System.err.println( "FTP server refused connection." );
				return null;
			} else {
				for (String file : inFile.split( ";" )) {
					if (file.isEmpty()) continue;
					
					String localName = String.format("C:\\Temp\\%s", file);
					try (FileOutputStream fos = new FileOutputStream(localName)) {
			          client.retrieveFile(file, fos);
			          if (re == null) {
			        	  re = localName;
			          } else
			        	  re = re + ";" + localName; 
			          System.out.println("Download " + file + " successfully");
			        } catch (IOException e) {
			        	System.err.println("Fail to download " + file);
			        }
				}
			}
			client.logout();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return re;
	}
}
