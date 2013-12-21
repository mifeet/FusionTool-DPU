package cz.cuni.mff.xrg.odcs.dpu.fusiontool.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;

/**
 * Reader and writer of files containing canonical URIs.
 * The implementation doesn't lock files but uses the move operation to ensure
 * files are not written to by different executions of a pipeline at the same time.
 * Note that this can lead to one pipeline overriding the results of another execution, however,
 * and therefore to loss of some canonical URIs.
 * @author Jan Michelfeit
 */
public class CanonicalUriFileReader {
    private static final Logger LOG = LoggerFactory.getLogger(CanonicalUriFileReader.class);

    private final File baseDirectory;

    /**
     * Creates a new instance.
     * @param baseDirectory directory with canonical URI files
     */
    public CanonicalUriFileReader(File baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    /**
     * Read canonical URIs from a file with the given name and add them to the given Set.
     * Do nothing if canonicalUrisFileName is empty or the file doesn't exist.
     * @param canonicalUrisFileName name of file with canonical URIs or null;
     *        this file will be looked up in the base directory given in constructor
     * @param canonicalUris set where to add loaded canonical URIs to
     * @throws IOException I/O error
     */
    public void readCanonicalUris(String canonicalUrisFileName, Set<String> canonicalUris) throws IOException {
        if (ODCSUtils.isNullOrEmpty(canonicalUrisFileName)) {
            return;
        }
        File canonicalUrisFile = new File(baseDirectory, canonicalUrisFileName);
        if (canonicalUrisFile.isFile() && canonicalUrisFile.canRead()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(canonicalUrisFile), "UTF-8"));
            try {
                long counter = 0;
                String line = reader.readLine();
                while (line != null) {
                    canonicalUris.add(line);
                    line = reader.readLine();
                    counter++;
                }
                LOG.info("Read {} canonical URIs from file '{}'", counter, canonicalUrisFileName);
            } finally {
                reader.close();
            }
        } else if (canonicalUrisFile.exists()) {
            LOG.error("Cannot read canonical URIs from '{}'", canonicalUrisFileName);
            // Intentionally do not throw an exception
        }
    }

    /**
     * Write given canonical URIs to a file with the given name.
     * Do nothing if canonicalUrisFileName is empty.
     * @param canonicalUrisFileName name of file with canonical URIs or null;
     *        this file will be looked up in the base directory given in constructor
     * @param canonicalUris canonical URIs to write
     * @throws IOException I/O error
     */
    public void writeCanonicalUris(String canonicalUrisFileName, Set<String> canonicalUris) throws IOException {
        if (ODCSUtils.isNullOrEmpty(canonicalUrisFileName)) {
            return;
        }
        File tmpFile = File.createTempFile("canon-" + canonicalUrisFileName, null, baseDirectory);
        try {
            FileOutputStream outputStream = new FileOutputStream(tmpFile);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            long counter = 0;
            try {
                for (String uri : canonicalUris) {
                    writer.println(uri);
                    counter++;
                }
            } finally {
                writer.close();
            }

            try {
                File canonicalUrisFile = new File(baseDirectory, canonicalUrisFileName);
                Files.move(tmpFile.toPath(), canonicalUrisFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                LOG.info(String.format("Written %,d canonical URIs to file '%s'", counter, canonicalUrisFileName));
            } catch (IOException e) {
                LOG.error("Cannot write canonical URIs file {}", canonicalUrisFileName);
            }
        } finally {
            tmpFile.delete();
        }
    }
}
