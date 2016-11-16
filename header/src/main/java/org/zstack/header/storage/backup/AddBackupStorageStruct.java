package org.zstack.header.storage.backup;

/**
 * Created by Mei Lei <meilei007@gmail.com> on 12/9/16.
 */
public class AddBackupStorageStruct {
    private Boolean importImages=false;
    private BackupStorageVO vo;

    public Boolean getImportImages() {
        return importImages;
    }

    public void setImportImages(Boolean importImages) {
        this.importImages = importImages;
    }

    public BackupStorageVO getVo() {
        return vo;
    }

    public void setVo(BackupStorageVO vo) {
        this.vo = vo;
    }
}
