package bdv.server;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.log.Log;

import bdv.server.BigDataServer.Parameters;

public class DirectoryFileWatcher extends Thread {

	private Parameters params;
	private String thumbnailsDirectoryName;
	private String baseURL;
	private String[] args;
	private HandlerCollection handlers;

	// minimum and maximum age of the file to be included in ms.
	private static long MIN_AGE = 60000;
	private static long MAX_AGE = 30l * 24l * 3600l * 1000l; // 30 days

	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger(BigDataServer.class);

	public DirectoryFileWatcher(String[] args, String baseURL, Parameters params, String thumbnailsDirectoryName,
			HandlerCollection handlers) {
		this.baseURL = baseURL;
		this.params = params;
		this.args = args;
		this.handlers = handlers;
		this.thumbnailsDirectoryName = thumbnailsDirectoryName;
	}

	public long getLastModificationDateOfFilesInDir(File dir) {
		List<File> files = (List<File>) FileUtils.listFiles(dir, null, true);
		long lastModificationTime = 0;
		for (File file : files) {
			if (file.lastModified() > lastModificationTime)
				lastModificationTime = file.lastModified();
		}
		return lastModificationTime;
	}

	public String getMD5ForFile(File file) throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException {
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		digest.reset();
		digest.update(file.getCanonicalPath().toString().replace("/scratch/temp/HaasFiji/", "").getBytes("utf8"));
		return String.format("%040x", new BigInteger(1, digest.digest()));
	}

	public void run() {

		Map<String, String> processedDatasets = new HashMap<String, String>(params.getDatasets());
		Map<String, String> newDatasets = new HashMap<String, String>();
		List<File> skippedOldFiles = new ArrayList<File>();
		final String[] extensions = new String[] { "xml" };

		while (true) {

			boolean newFilesFound = false;

			try {

				final Parameters params = BigDataServer.processOptions(args, BigDataServer.getDefaultParameters());

				LOG.debug("Starting listing directories");

				File[] subdir = new File(FileSystems.getDefault().getPath(params.getWatchDirectory()).toString())
						.listFiles(File::isDirectory);

				for (File dir : subdir) {
					if (skippedOldFiles.contains(dir)) {
						continue;
					}

					if (System.currentTimeMillis() - dir.lastModified() < MIN_AGE) {
						LOG.debug("Skipping directory " + dir.getCanonicalPath().toString()
								+ ", content modified less than " + MIN_AGE / 1000l + " s ago.");
						continue;
					}
					if (System.currentTimeMillis() - dir.lastModified() > MAX_AGE) {
						LOG.debug("Skipping directory " + dir.getCanonicalPath().toString()
								+ " forever, modification date is older than " + MAX_AGE / (24l * 3600l * 1000l)
								+ " days.");
						skippedOldFiles.add(dir);
						continue;
					}

					LOG.debug("Starting listing files of " + dir.getCanonicalPath().toString());
					List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
					LOG.debug("Processing list");

					for (File file : files) {
						if (skippedOldFiles.contains(file)) {
							continue;
						}

						if (System.currentTimeMillis() - file.lastModified() < MIN_AGE) {
							LOG.debug("Skipping file " + file.getCanonicalPath().toString()
									+ ", content modified less than " + MIN_AGE / 1000l + " s ago.");
							continue;
						}
						if (System.currentTimeMillis() - file.lastModified() > MAX_AGE) {
							LOG.debug("Skipping file " + file.getCanonicalPath().toString()
									+ " forever, modification date is older than " + MAX_AGE / (24l * 3600l * 1000l)
									+ " days.");
							skippedOldFiles.add(file);
							continue;
						}

						String sha1 = getMD5ForFile(file);

						if (!processedDatasets.containsKey(sha1)) {
							processedDatasets.put(sha1, file.getCanonicalPath().toString());
							newDatasets.put(sha1, file.getCanonicalPath().toString());
							newFilesFound = true;

							LOG.info("Reloading server with new configuration, added " + sha1 + " " + "("
									+ file.getCanonicalPath().toString().replace("/scratch/temp/HaasFiji/", "")
									+ " ) - " + file.getCanonicalPath().toString());
						}
					}

					if (newFilesFound) {
						final ContextHandlerCollection datasetHandlers = BigDataServer.createHandlers(baseURL,
								newDatasets, thumbnailsDirectoryName);
						handlers.addHandler(datasetHandlers);
						datasetHandlers.start();
						newDatasets.clear();
					}
				}
				LOG.debug("Listing directories finished");

			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// Java NIO WatchService does not work on SSHFS mounted volumes
	/*
	 * public void run() {
	 * 
	 * Map<String, String> generatedDatasets = new HashMap<String,
	 * String>(params.getDatasets());
	 * 
	 * final Path path =
	 * FileSystems.getDefault().getPath(params.getWatchDirectory());
	 * 
	 * try (final WatchService watchService =
	 * FileSystems.getDefault().newWatchService()) { path.register(watchService,
	 * StandardWatchEventKinds.ENTRY_CREATE); while (true) { try { final WatchKey wk
	 * = watchService.take(); for (WatchEvent<?> event : wk.pollEvents()) {
	 * 
	 * final Path changed = ((Path) wk.watchable()).resolve(((WatchEvent<Path>)
	 * event).context());
	 * 
	 * final Parameters params = BigDataServer.processOptions(args,
	 * BigDataServer.getDefaultParameters()); final HandlerCollection handlers = new
	 * HandlerCollection();
	 * 
	 * if (getFileExtension(changed).equals("xml")) {
	 * 
	 * MessageDigest digest = MessageDigest.getInstance("SHA-1"); digest.reset();
	 * digest.update(changed.toAbsolutePath().toString().getBytes("utf8")); String
	 * sha1 = String.format("%040x", new BigInteger(1, digest.digest()));
	 * 
	 * generatedDatasets.put(sha1, changed.toAbsolutePath().toString());
	 * 
	 * final ContextHandlerCollection datasetHandlers =
	 * BigDataServer.createHandlers(baseURL, generatedDatasets,
	 * thumbnailsDirectoryName); handlers.addHandler(datasetHandlers);
	 * handlers.addHandler(new JsonDatasetListHandler(server, datasetHandlers));
	 * 
	 * LOG.info("Reloading server with new configuration, added " + sha1 + " " +
	 * changed.toAbsolutePath().toString());
	 * 
	 * server.stop(); server.setHandler(handlers); server.start(); }
	 * 
	 * // recursive add new directory if (Files.isDirectory(changed))
	 * changed.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
	 * 
	 * } // reset the key boolean valid = wk.reset(); if (!valid) {
	 * LOG.warn("Key has been unregistered"); } } catch (Exception e) {
	 * e.printStackTrace(); LOG.warn(e.getMessage()); } } } catch (Exception e) {
	 * e.printStackTrace(); LOG.warn(e.getMessage()); } }
	 */
}
