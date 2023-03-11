package com.lantromipis.orchestration.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.*;

@ApplicationScoped
public class PgFacadeIOUtils {

    @Inject
    ManagedExecutor managedExecutor;

    public InputStream createInputStreamWithTarFromInputStreamContainingFile(String fileName, InputStream inputStreamContainingSingleFile) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(byteArrayOutputStream);

        byte[] fileContent = inputStreamContainingSingleFile.readAllBytes();
        inputStreamContainingSingleFile.close();

        TarArchiveEntry tarEntry = new TarArchiveEntry(fileName);
        tarEntry.setSize(fileContent.length);
        tarArchiveOutputStream.putArchiveEntry(tarEntry);
        tarArchiveOutputStream.write(fileContent);
        tarArchiveOutputStream.closeArchiveEntry();
        tarArchiveOutputStream.finish();

        PipedInputStream in = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(in);

        managedExecutor.runAsync(() -> {
            try {
                byteArrayOutputStream.writeTo(out);
                out.flush();
                out.close();
                tarArchiveOutputStream.close();
                byteArrayOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return in;
    }
}
