package org.zstack.header.storage.backup;

import org.zstack.header.errorcode.ErrorCode;

/**
 * Created by Mei Lei <meilei007@gmail.com> on 11/5/16.
 */
public interface AddBackupStorageExtensionPoint {
    void preAddBackupStorage(BackupStorageInventory backupStorage);

    void beforeAddBackupStorage(BackupStorageInventory backupStorage);

    void afterAddBackupStorage(BackupStorageInventory backupStorage);

    void failedToAddBackupStorage(BackupStorageInventory backupStorage, ErrorCode err);
}
