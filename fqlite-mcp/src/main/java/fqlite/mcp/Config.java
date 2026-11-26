package fqlite.mcp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Command-line settings: folders the server may read evidence from ({@code --evidence}, repeatable;
 * none = any readable file) and the one folder it writes to ({@code --output}).
 *
 * @uauthor pawalszc
 */
final class Config {

	final List<Path> evidenceRoots = new ArrayList<>();
	Path output = Path.of(System.getProperty("user.home"), "fqlite-mcp-output");

	/**
	 * The actual "main"-method.
	 * @param args
	 * @return
	 */
	static Config parse(String[] args) {

		Config c = new Config();
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "--evidence" -> c.evidenceRoots.add(real(Path.of(args[++i])));
				case "--output" -> c.output = Path.of(args[++i]).toAbsolutePath().normalize();
				case "--version" -> {
					System.err.println("fqlite-mcp " + FQLiteMcpServer.VERSION);
					System.exit(0);
				}
				default -> {
					System.err.println("""
							unknown argument: %s
							usage: java -jar fqlite-mcp.jar [--evidence <dir>]... [--output <dir>]
							  --evidence <dir>  folder evidence may be read from (repeatable; default: any file)
							  --output <dir>    folder for exports, BLOBs and the audit log
							                    (default: ~/fqlite-mcp-output)""".formatted(args[i]));
					System.exit(2);
				}
			}
		}
		return c;
	}

	private static Path real(Path p) {
		try {
			return p.toRealPath();
		} catch (IOException e) {
			return p.toAbsolutePath().normalize();
		}
	}

	/**
	 * resolves an evidence path and checks it lies in an allowed folder.
	 *
	 * @param path to database
	 */
	Path checkEvidence(String path) throws ToolException {
		if (path == null || path.isBlank())
			throw new ToolException("path is missing");
		Path p = Path.of(path);
		if (!p.isAbsolute())
			throw new ToolException("path must be absolute: " + path);
		if (!Files.exists(p))
			throw new ToolException("file not found: " + path);
		p = real(p);
		if (evidenceRoots.isEmpty())
			return p;
		for (Path root : evidenceRoots)
			if (p.startsWith(root))
				return p;
		throw new ToolException("access denied: " + p + " is outside the evidence folders " + evidenceRoots);
	}

	/**
	 * Creates the output folder of one analysis (created on demand).
	 *
	 * @param String the unique id for this anaylsis.
	 */
	Path outputDir(String analysisId) throws IOException {
		Path d = output.resolve(analysisId);
		Files.createDirectories(d);
		return d;
	}
}
