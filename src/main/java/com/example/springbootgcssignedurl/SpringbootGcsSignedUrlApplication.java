package com.example.springbootgcssignedurl;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringbootGcsSignedUrlApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootGcsSignedUrlApplication.class, args);
    }

}

@RestController
@Slf4j
class StorageController {

    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    @Autowired
    private Storage storage;

    @Value("bucketname")
    String bucketName;
    @Value("subdirectory")
    String subdirectory;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<URL> uploadFile(@RequestPart("file") FilePart filePart) {
        //Convert the file to a byte array
        final byte[] byteArray = convertToByteArray(filePart);

        //Prepare the blobId
        //BlobId is a combination of bucketName + subdirectiory(optional) + fileName
        final BlobId blobId = constructBlobId(bucketName, subdirectory, filePart.filename());

        return Mono.just(blobId)
                //Create the blobInfo
                .map(bId -> BlobInfo.newBuilder(blobId)
                        .setContentType("text/plain")
                        .build())
                //Upload the blob to GCS
                .doOnNext(blobInfo -> getStorage().create(blobInfo, byteArray))
                //Create a Signed "Path Style" URL to access the newly created Blob
                //Set the URL expiry to 10 Minutes
                .map(blobInfo -> createSignedPathStyleUrl(blobInfo, 10, TimeUnit.MINUTES));
    }

    private URL createSignedPathStyleUrl(BlobInfo blobInfo,
                                         int duration, TimeUnit timeUnit) {
        return getStorage()
                .signUrl(blobInfo, duration, timeUnit, Storage.SignUrlOption.withPathStyle());
    }

    /**
     * Construct Blob ID
     *
     * @param bucketName
     * @param subdirectory optional
     * @param fileName
     * @return
     */
    private BlobId constructBlobId(String bucketName, @Nullable String subdirectory,
                                   String fileName) {
        return Optional.ofNullable(subdirectory)
                .map(s -> BlobId.of(bucketName, subdirectory + "/" + fileName))
                .orElse(BlobId.of(bucketName, fileName));
    }

    /**
     * Here, we convert the file to a byte array to be sent to GCS Libraries
     *
     * @param filePart File to be used
     * @return Byte Array with all the contents of the file
     */
    @SneakyThrows
    private byte[] convertToByteArray(FilePart filePart) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            filePart.content()
                    .subscribe(dataBuffer -> {
                        byte[] bytes = new byte[dataBuffer.readableByteCount()];
                        log.trace("readable byte count:" + dataBuffer.readableByteCount());
                        dataBuffer.read(bytes);
                        DataBufferUtils.release(dataBuffer);
                        try {
                            bos.write(bytes);
                        } catch (IOException e) {
                            log.error("read request body error...", e);
                        }
                    });

            return bos.toByteArray();
        }
    }

}

@Component
@Slf4j
class CloudStorageConfig {

    @Value("classpath:key.json")
    private Resource key;

    /**
     * Loads credential file into memory
     * Returns a fully formed GCS Credentials
     *
     * @return GoogleCredentials
     */
    private GoogleCredentials gcsCredentials() throws IOException {
        return GoogleCredentials.fromStream(key.getInputStream());
    }

    /**
     * Storage Bean to access GCS
     *
     * @return the Google Cloud Storage object
     */
    @Bean
    public Storage storage() {

        Storage storage = null;
        try {
            storage = StorageOptions.newBuilder()
                    .setCredentials(gcsCredentials())
                    .build()
                    .getService();
        } catch (FileNotFoundException fe) {
            log.error("Bucket key file not found. Failing silently", fe);
        } catch (StorageException | IOException e) {
            log.error("Exception", e);
        }
        return storage;
    }

}