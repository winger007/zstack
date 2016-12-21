package org.zstack.header.storage.backup;

import org.zstack.header.message.MessageReply;

/**
 */
public class BakeImageMetadataReply extends MessageReply {
    private String metadata;
    private String backupStorageMetaFileName;

    public String getBackupStorageMetaFileName() {
        return backupStorageMetaFileName;
    }

    public void setBackupStorageMetaFileName(String backupStorageMetaFileName) {
        this.backupStorageMetaFileName = backupStorageMetaFileName;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }
}
