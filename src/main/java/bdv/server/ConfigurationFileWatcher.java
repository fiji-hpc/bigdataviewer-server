package bdv.server;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

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
	
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( BigDataServer.class );
	
	public ConfigurationFileWatcher(Server server, String[] args, String baseURL, Parameters params, String thumbnailsDirectoryName) {
		this.server = server;
		this.baseURL = baseURL;
		this.params = params;
		this.args = args;
		this.thumbnailsDirectoryName = thumbnailsDirectoryName;
	}

	public void run() {
		
			final Path path = FileSystems.getDefault().getPath(System.getProperty("user.dir"));
					
			try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
			    final WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
			    while (true) {
			        final WatchKey wk = watchService.take();
			        for (WatchEvent<?> event : wk.pollEvents()) {
			            //we only register "ENTRY_MODIFY" so the context is always a Path.
			            final Path changed = (Path) event.context();
			              
			                final Parameters params = BigDataServer.processOptions( args, BigDataServer.getDefaultParameters() );    
			        		final HandlerCollection handlers = new HandlerCollection();

			        		final ContextHandlerCollection datasetHandlers = BigDataServer.createHandlers( baseURL, params.getDatasets(), thumbnailsDirectoryName );
			        		handlers.addHandler( datasetHandlers );
			        		handlers.addHandler( new JsonDatasetListHandler( server, datasetHandlers ) );
			        		
			        		LOG.info("Reloading server with new configuration, file changed " + changed);
			        		server.stop();
			        		server.setHandler(handlers);
			        		server.start();		                
			        }
			        // reset the key
			        boolean valid = wk.reset();
			        if (!valid) {
			        	LOG.warn("Key has been unregistered");
			        }
			    }
			} catch (Exception e) {
				e.printStackTrace();
				LOG.warn( e.getMessage() );
			}	
	}

}
