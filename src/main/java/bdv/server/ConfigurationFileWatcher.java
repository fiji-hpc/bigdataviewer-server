package bdv.server;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.log.Log;

import bdv.server.BigDataServer.Parameters;

public class ConfigurationFileWatcher extends Thread {

	Server server;
	Parameters params;
	String thumbnailsDirectoryName;
	String baseURL;
	String[] args;
	HandlerCollection handlers;

	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger(BigDataServer.class);

	public ConfigurationFileWatcher(Server server, String[] args, String baseURL, Parameters params,
			String thumbnailsDirectoryName, HandlerCollection handlers) {
		this.server = server;
		this.baseURL = baseURL;
		this.params = params;
		this.args = args;
		this.handlers = handlers;
		this.thumbnailsDirectoryName = thumbnailsDirectoryName;
	}

	private String getFileExtension(Path path) {
		String name = path.toAbsolutePath().toString();
		try {
			return name.substring(name.lastIndexOf(".") + 1);
		} catch (Exception e) {
			return "";
		}
	}

	public long getLastModificationDateOfFilesInDir(File dir) {
		List<File> files = (List<File>) FileUtils.listFiles(dir, null, true);
		long lastModificationTime = 0;
		for (File file : files) {
			if (file.lastModified() > lastModificationTime) lastModificationTime = file.lastModified();
		}
		return lastModificationTime;
	}
	
	public void run() {

		Map<String, String> generatedDatasets = new HashMap<String, String>(params.getDatasets());
		Map<String, String> newDatasets = new HashMap<String, String>();
		final String[] extensions = new String[] { "xml" };

		while (true) {

			boolean newFilesFound = false;

			try {

				final Parameters params = BigDataServer.processOptions(args, BigDataServer.getDefaultParameters());
				final File dir = new File(FileSystems.getDefault().getPath(params.getWatchDirectory()).toString());

				List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
				for (File file : files) {
					
					long currentMillis = System.currentTimeMillis();
					if (currentMillis - getLastModificationDateOfFilesInDir(file.getParentFile()) <  60000) {
						LOG.warn("Skipping file " + file.getCanonicalPath().toString() + ", directory contents modified just 60s ago.");
						continue;
					}
					//System.out.println("file: " + file.getCanonicalPath());

					MessageDigest digest = MessageDigest.getInstance("SHA-1");
					digest.reset();
					digest.update(
							file.getCanonicalPath().toString().replace("/scratch/temp/HaasFiji/", "").getBytes("utf8"));
					String sha1 = String.format("%040x", new BigInteger(1, digest.digest()));

					if (!generatedDatasets.containsKey(sha1)) {
						generatedDatasets.put(sha1, file.getCanonicalPath().toString());
						newDatasets.put(sha1, file.getCanonicalPath().toString());
						newFilesFound = true;
						
						LOG.info("Reloading server with new configuration, added " + sha1 + " " + "("
								+ file.getCanonicalPath().toString().replace("/scratch/temp/HaasFiji/", "") + " ) - "
								+ file.getCanonicalPath().toString());
					}
				}

				if (newFilesFound) {
					final ContextHandlerCollection datasetHandlers = BigDataServer.createHandlers(baseURL,
							newDatasets, thumbnailsDirectoryName);
					handlers.addHandler(datasetHandlers);
					datasetHandlers.start();
					newDatasets.clear();
				}

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

	// Does not work with SSHFS 
	/*
	public void run() {

		Map<String, String> generatedDatasets = new HashMap<String, String>(params.getDatasets());

		final Path path = FileSystems.getDefault().getPath(params.getWatchDirectory());

		try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
			path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
			while (true) {
				try {
					final WatchKey wk = watchService.take();
					for (WatchEvent<?> event : wk.pollEvents()) {

						final Path changed = ((Path) wk.watchable()).resolve(((WatchEvent<Path>) event).context());

						final Parameters params = BigDataServer.processOptions(args,
								BigDataServer.getDefaultParameters());
						final HandlerCollection handlers = new HandlerCollection();

						if (getFileExtension(changed).equals("xml")) {

							MessageDigest digest = MessageDigest.getInstance("SHA-1");
							digest.reset();
							digest.update(changed.toAbsolutePath().toString().getBytes("utf8"));
							String sha1 = String.format("%040x", new BigInteger(1, digest.digest()));

							generatedDatasets.put(sha1, changed.toAbsolutePath().toString());

							final ContextHandlerCollection datasetHandlers = BigDataServer.createHandlers(baseURL,
									generatedDatasets, thumbnailsDirectoryName);
							handlers.addHandler(datasetHandlers);
							handlers.addHandler(new JsonDatasetListHandler(server, datasetHandlers));

							LOG.info("Reloading server with new configuration, added " + sha1 + " "
									+ changed.toAbsolutePath().toString());

							server.stop();
							server.setHandler(handlers);
							server.start();
						}

						// recursive add new directory if (Files.isDirectory(changed))
						changed.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

					}
					// reset the key
					boolean valid = wk.reset();
					if (!valid) {
						LOG.warn("Key has been unregistered");
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.warn(e.getMessage());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn(e.getMessage());
		}
	}
	*/
}
