package org.zstack.header.storage.backup;

/**
 * Created by Mei Lei <meilei007@gmail.com> on 12/9/16.
 */
public class AddBackupStorageStruct {
    private Boolean importImages=false;
    private BackupStorageInventory backupStorageInventory;

    public Boolean getImportImages() {
        return importImages;
    }

    public void setImportImages(Boolean importImages) {
        this.importImages = importImages;
    }

    public BackupStorageInventory getBackupStorageInventory() {
        return backupStorageInventory;
    }

    public void setBackupStorageInventory(BackupStorageInventory backupStorageInventory) {
        this.backupStorageInventory = backupStorageInventory;
    }
}
