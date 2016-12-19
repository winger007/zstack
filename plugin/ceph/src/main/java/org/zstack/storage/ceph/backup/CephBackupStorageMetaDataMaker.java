package org.zstack.storage.ceph.backup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.zstack.core.db.DatabaseFacade;
import org.zstack.core.db.SimpleQuery;
import org.zstack.core.errorcode.ErrorFacade;
import org.zstack.core.workflow.FlowChainBuilder;
import org.zstack.core.workflow.ShareFlow;
import org.zstack.header.core.workflow.FlowChain;
import org.zstack.header.core.workflow.FlowDoneHandler;
import org.zstack.header.core.workflow.FlowTrigger;
import org.zstack.header.core.workflow.NoRollbackFlow;
import org.zstack.header.errorcode.ErrorCode;
import org.zstack.header.errorcode.SysErrors;
import org.zstack.header.image.*;
import org.zstack.header.rest.JsonAsyncRESTCallback;
import org.zstack.header.rest.RESTFacade;
import org.zstack.header.storage.backup.AddBackupStorageExtensionPoint;
import org.zstack.header.storage.backup.AddBackupStorageStruct;
import org.zstack.storage.ceph.CephConstants;
import org.zstack.utils.DebugUtils;
import org.zstack.utils.Utils;
import org.zstack.utils.gson.JSONObjectUtil;
import org.zstack.utils.logging.CLogger;

import javax.persistence.TypedQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Mei Lei <meilei007@gmail.com> on 11/3/16.
 */
public class CephBackupStorageMetaDataMaker implements AddImageExtensionPoint, AddBackupStorageExtensionPoint, ExpungeImageExtensionPoint {
    private static final CLogger logger = Utils.getLogger(CephBackupStorageMetaDataMaker.class);

    @Autowired
    protected RESTFacade restf;
    @Autowired
    private DatabaseFacade dbf;
    @Autowired
    private ErrorFacade errf;

    private String buildUrl( String hostName, Integer monPort,String subPath) {
        return String.format("http://%s:%s%s", hostName, monPort, subPath);
    }

    private String getAllImageInventories(ImageInventory img) {
        String allImageInventories = null;
        String sql = "select * from ImageVO img, ImageBackupStorageRefVO ref where ref.backupStorageUuid = :bsUuid";
        TypedQuery<ImageVO> q = dbf.getEntityManager().createQuery(sql, ImageVO.class);
        q.setParameter("bsUuid", getBackupStorageUuidFromImageInventory(img));
        List<ImageVO> allImageVO = q.getResultList();
        for (ImageVO imageVO : allImageVO) {
            if (allImageInventories != null) {
                allImageInventories = JSONObjectUtil.toJsonString(ImageInventory.valueOf(imageVO)) + "\n" + allImageInventories;
            } else {
                allImageInventories = JSONObjectUtil.toJsonString(ImageInventory.valueOf(imageVO));
            }
        }
        return allImageInventories;
    }


    private String getBackupStorageUuidFromImageInventory(ImageInventory img) {
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        q.select(ImageBackupStorageRefVO_.backupStorageUuid);
        q.add(ImageBackupStorageRefVO_.imageUuid, SimpleQuery.Op.EQ, img.getUuid());
        String backupStorageUuid = q.findValue();
        DebugUtils.Assert(backupStorageUuid != null, String.format("cannot find backup storage for image [uuid:%s]", img.getUuid()));
        return backupStorageUuid;
    }

    private void restoreImagesBackupStorageMetadataToDatabase(String imagesMetadata, String backupStorageUuid) {
        List<ImageVO>  imageVOs = new ArrayList<ImageVO>();
        List<ImageBackupStorageRefVO> backupStorageRefVOs = new ArrayList<ImageBackupStorageRefVO>();
        String[] metadatas =  imagesMetadata.split("\n");
        for ( String metadata : metadatas) {
            if (metadata.contains("backupStorageRefs")) {
                ImageInventory imageInventory = JSONObjectUtil.toObject(metadata, ImageInventory.class);
                for ( ImageBackupStorageRefInventory ref : imageInventory.getBackupStorageRefs()) {
                    ImageBackupStorageRefVO backupStorageRefVO = new ImageBackupStorageRefVO();
                    backupStorageRefVO.setStatus(ImageStatus.valueOf(ref.getStatus()));
                    backupStorageRefVO.setInstallPath(ref.getInstallPath());
                    backupStorageRefVO.setImageUuid(ref.getImageUuid());
                    backupStorageRefVO.setBackupStorageUuid(backupStorageUuid);
                    backupStorageRefVO.setCreateDate(ref.getCreateDate());
                    backupStorageRefVO.setLastOpDate(ref.getLastOpDate());
                    backupStorageRefVOs.add(backupStorageRefVO);
                }
                ImageVO imageVO = new ImageVO();
                imageVO.setActualSize(imageInventory.getActualSize());
                imageVO.setDescription(imageInventory.getDescription());
                imageVO.setStatus(ImageStatus.valueOf(imageInventory.getStatus()));
                imageVO.setExportUrl(imageInventory.getExportUrl());
                imageVO.setFormat(imageInventory.getFormat());
                imageVO.setGuestOsType(imageInventory.getGuestOsType());
                imageVO.setMd5Sum(imageInventory.getMd5Sum());
                imageVO.setMediaType(ImageConstant.ImageMediaType.valueOf(imageInventory.getMediaType()));
                imageVO.setName(imageInventory.getName());
                imageVO.setPlatform(ImagePlatform.valueOf(imageInventory.getPlatform()));
                imageVO.setSize(imageInventory.getSize());
                imageVO.setState(ImageState.valueOf(imageInventory.getState()));
                imageVO.setSystem(imageInventory.isSystem());
                imageVO.setType(imageInventory.getType());
                imageVO.setUrl(imageInventory.getUrl());
                imageVO.setUuid(imageInventory.getUuid());
                imageVO.setCreateDate(imageInventory.getCreateDate());
                imageVO.setLastOpDate(imageInventory.getLastOpDate());
                imageVOs.add(imageVO);
            }
        }
        dbf.persistCollection(imageVOs);
        dbf.persistCollection(backupStorageRefVOs);
    }

    private String getHostnameFromBackupStorage(CephBackupStorageInventory inv) {
        SimpleQuery<CephBackupStorageMonVO> q = dbf.createQuery(CephBackupStorageMonVO.class);
        q.select(CephBackupStorageMonVO_.hostname);
        q.add(CephBackupStorageMonVO_.backupStorageUuid, SimpleQuery.Op.EQ, inv.getUuid());
        CephBackupStorageMonVO cephMonVO = q.find();
        DebugUtils.Assert(cephMonVO != null, String.format("cannot find hostName for ceph backup storage [uuid:%s]", inv.getUuid()));
        return cephMonVO.getHostname();
    }

    private Integer getMonPortFromBackupStorage(CephBackupStorageInventory inv) {
        SimpleQuery<CephBackupStorageMonVO> q = dbf.createQuery(CephBackupStorageMonVO.class);
        q.select(CephBackupStorageMonVO_.monPort);
        q.add(CephBackupStorageMonVO_.backupStorageUuid, SimpleQuery.Op.EQ, inv.getUuid());
        CephBackupStorageMonVO cephMonVO = q.find();
        DebugUtils.Assert(cephMonVO != null, String.format("cannot find port for ceph backup storage [uuid:%s]", inv.getUuid()));
        return cephMonVO.getMonPort();
    }

    @Transactional
    private String getHostNameFromImageInventory(ImageInventory img) {
        String sql="select mon.hostname from CephBackupStorageMonVO mon, ImageBackupStorageRefVO ref where " +
                "ref.imageUuid= :uuid and ref.backupStorageUuid = mon.backupStorageUuid";
        TypedQuery<String> q = dbf.getEntityManager().createQuery(sql, String.class);
        q.setParameter("uuid", img.getUuid());
        return q.getSingleResult();
    }

    private Integer getMonPortFromImageInventory(ImageInventory img) {
        String sql="select mon.monPort from CephBackupStorageMonVO mon, ImageBackupStorageRefVO ref where " +
                "ref.imageUuid= :uuid and ref.backupStorageUuid = mon.backupStorageUuid";
        TypedQuery<Integer> q = dbf.getEntityManager().createQuery(sql, Integer.class);
        q.setParameter("uuid", img.getUuid());
        return q.getSingleResult();
    }

    @Transactional
    private String getBackupStorageTypeFromImageInventory(ImageInventory img) {
        String sql = "select bs.type from BackupStorageVO bs, ImageBackupStorageRefVO refVo where  " +
                "bs.uuid = refVo.backupStorageUuid and refVo.imageUuid = :uuid";
        TypedQuery<String> q = dbf.getEntityManager().createQuery(sql, String.class);
        q.setParameter("uuid", img.getUuid());
        String type = q.getSingleResult();
        logger.debug(String.format("meilei: %s", type));
        return type;
    }

    protected  void dumpImagesBackupStorageInfoToMetaDataFile(ImageInventory img, boolean allImagesInfo, String bsUrl, String hostName ) {
        logger.debug("dump all images info to meta data file");
        CephBackupStorageBase.DumpImageInfoToMetaDataFileCmd dumpCmd = new CephBackupStorageBase.DumpImageInfoToMetaDataFileCmd();
        String metaData;
        if (allImagesInfo) {
            metaData = getAllImageInventories(img);
        } else {
            metaData = JSONObjectUtil.toJsonString(img);
        }
        dumpCmd.setImageMetaData(metaData);
        dumpCmd.setDumpAllMetaData(allImagesInfo);
        if ( bsUrl != null) {
            dumpCmd.setBackupStoragePath(bsUrl);
        }
        if (hostName ==  null || hostName.isEmpty()) {
           hostName = getHostNameFromImageInventory(img);
        }
        Integer monPort = getMonPortFromImageInventory(img);
        restf.asyncJsonPost(buildUrl(hostName, monPort, CephBackupStorageBase.DUMP_IMAGE_METADATA_TO_FILE), dumpCmd,
                new JsonAsyncRESTCallback<CephBackupStorageBase.DumpImageInfoToMetaDataFileRsp >() {
                    @Override
                    public void fail(ErrorCode err) {
                        logger.error("Dump image metadata failed" + err.toString());
                    }

                    @Override
                    public void success(CephBackupStorageBase.DumpImageInfoToMetaDataFileRsp rsp) {
                        if (!rsp.isSuccess()) {
                            logger.error("Dump image metadata failed");
                        } else {
                            logger.info("Dump image metadata successfully");
                        }
                    }

                    @Override
                    public Class<CephBackupStorageBase.DumpImageInfoToMetaDataFileRsp> getReturnClass() {
                        return CephBackupStorageBase.DumpImageInfoToMetaDataFileRsp.class;
                    }
                });
    }

    @Override
    public void preAddImage(ImageInventory img) {

    }

    @Override
    public void beforeAddImage(ImageInventory img) {

    }

    @Override
    public void afterAddImage(ImageInventory img) {
        if (!getBackupStorageTypeFromImageInventory(img).equals(CephConstants.CEPH_BACKUP_STORAGE_TYPE)) {
            return;
        }
        String hostName = getHostNameFromImageInventory(img);
        Integer monPort = getMonPortFromImageInventory(img);

        FlowChain chain = FlowChainBuilder.newShareFlowChain();

        chain.setName("add-image-metadata-to-ceph-backupStorage-file");
        chain.then(new ShareFlow() {
            boolean metaDataExist = false;
            @Override
            public void setup() {
                flow(new NoRollbackFlow() {
                    String __name__ = "check-image-metadata-file-exist";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {
                        CephBackupStorageBase.CheckImageMetaDataFileExistCmd cmd = new CephBackupStorageBase.CheckImageMetaDataFileExistCmd();
                        cmd.setBackupStoragePath(CephBackupStorageMonBase.META_DATA_PATH);
                        restf.asyncJsonPost(buildUrl(hostName, monPort, CephBackupStorageBase.CHECK_IMAGE_METADATA_FILE_EXIST), cmd,
                                new JsonAsyncRESTCallback<CephBackupStorageBase.CheckImageMetaDataFileExistRsp>() {
                                    @Override
                                    public void fail(ErrorCode err) {
                                        logger.error("Check image metadata file exist failed" + err.toString());
                                        trigger.fail(err);
                                    }

                                    @Override
                                    public void success(CephBackupStorageBase.CheckImageMetaDataFileExistRsp rsp) {
                                        if (!rsp.isSuccess()) {
                                            logger.error(String.format("Check image metadata file: %s failed", rsp.getBackupStorageMetaFileName()));
                                            ErrorCode ec = errf.instantiateErrorCode(
                                                    SysErrors.OPERATION_ERROR,
                                                    String.format("Check image metadata file: %s failed", rsp.getBackupStorageMetaFileName())
                                            );
                                            trigger.fail(ec);
                                        } else {
                                            if (!rsp.getExist()) {
                                                logger.info(String.format("Image metadata file %s is not exist", rsp.getBackupStorageMetaFileName()));
                                                // call generate and dump all image info to yaml
                                                trigger.next();
                                            } else {
                                                logger.info(String.format("Image metadata file %s exist", rsp.getBackupStorageMetaFileName()));
                                                metaDataExist = true;
                                                trigger.next();
                                            }
                                        }
                                    }

                                    @Override
                                    public Class<CephBackupStorageBase.CheckImageMetaDataFileExistRsp> getReturnClass() {
                                        return CephBackupStorageBase.CheckImageMetaDataFileExistRsp.class;
                                    }
                                });
                    }
                });


                flow(new NoRollbackFlow() {
                    String __name__ = "create-image-metadata-file";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {

                        if (!metaDataExist) {
                            CephBackupStorageBase.GenerateImageMetaDataFileCmd generateCmd = new CephBackupStorageBase.GenerateImageMetaDataFileCmd();
                            restf.asyncJsonPost(buildUrl(hostName, monPort,CephBackupStorageBase.GENERATE_IMAGE_METADATA_FILE), generateCmd,
                                    new JsonAsyncRESTCallback<CephBackupStorageBase.GenerateImageMetaDataFileRsp>() {
                                        @Override
                                        public void fail(ErrorCode err) {
                                            logger.error("Create image metadata file failed" + err.toString());
                                        }

                                        @Override
                                        public void success(CephBackupStorageBase.GenerateImageMetaDataFileRsp rsp) {
                                            if (!rsp.isSuccess()) {
                                                ErrorCode ec = errf.instantiateErrorCode(
                                                        SysErrors.OPERATION_ERROR,
                                                        String.format("Create image metadata file : %s failed", rsp.getBackupStorageMetaFileName()));
                                                trigger.fail(ec);
                                            } else {
                                                logger.info("Create image metadata file successfully");
                                                dumpImagesBackupStorageInfoToMetaDataFile(img, true, null, null);
                                                trigger.next();
                                            }
                                        }

                                        @Override
                                        public Class<CephBackupStorageBase.GenerateImageMetaDataFileRsp> getReturnClass() {
                                            return CephBackupStorageBase.GenerateImageMetaDataFileRsp.class;
                                        }
                                    });

                        } else {
                            dumpImagesBackupStorageInfoToMetaDataFile(img, false, null, null);
                            trigger.next();
                        }


                    }
                });


                done(new FlowDoneHandler() {
                    @Override
                    public void handle(Map data) {

                    }
                });

            }

            }).start();
    }

    @Override
    public void failedToAddImage(ImageInventory img, ErrorCode err) {

    }

    @Override
    public void preAddBackupStorage(AddBackupStorageStruct backupStorage) {

    }
    @Override
    public void beforeAddBackupStorage(AddBackupStorageStruct backupStorage) {

    }
    @Override
    public void afterAddBackupStorage(AddBackupStorageStruct backupStorage) {
        if (!(backupStorage.getBackupStorgeType().equals(CephConstants.CEPH_BACKUP_STORAGE_TYPE) && backupStorage.getImportImages())) {
            return;
        }
        CephBackupStorageInventory inv = (CephBackupStorageInventory) backupStorage.getBackupStorageInventory();
        String hostName = getHostnameFromBackupStorage(inv);
        Integer monPort = getMonPortFromBackupStorage(inv);
        logger.debug("Starting to import ceph images metadata");
        CephBackupStorageBase.GetImagesMetaDataCommand cmd = new CephBackupStorageBase.GetImagesMetaDataCommand();
        cmd.setBackupStoragePath(CephBackupStorageMonBase.META_DATA_PATH);
        restf.asyncJsonPost(buildUrl(hostName, monPort, CephBackupStorageBase.GET_IMAGES_METADATA), cmd,
                new JsonAsyncRESTCallback<CephBackupStorageBase.GetImagesMetaDataRsp>() {
                    @Override
                    public void fail(ErrorCode err) {
                        logger.error("Check image metadata file exist failed" + err.toString());
                    }

                    @Override
                    public void success(CephBackupStorageBase.GetImagesMetaDataRsp rsp) {
                        if (!rsp.isSuccess()) {
                            logger.error(String.format("Get images metadata: %s failed", rsp.getImagesMetaData()));
                        } else {
                            logger.info(String.format("Get images metadata: %s success", rsp.getImagesMetaData()));
                            restoreImagesBackupStorageMetadataToDatabase(rsp.getImagesMetaData(), backupStorage.getBackupStorageInventory().getUuid());
                        }
                    }

                    @Override
                    public Class<CephBackupStorageBase.GetImagesMetaDataRsp> getReturnClass() {
                        return CephBackupStorageBase.GetImagesMetaDataRsp.class;
                    }
                });
    }
    public void failedToAddBackupStorage(AddBackupStorageStruct backupStorage, ErrorCode err) {

    }

    public void preExpungeImage(ImageInventory img) {

    }

    public void beforeExpungeImage(ImageInventory img) {

    }

    public void afterExpungeImage(ImageInventory img, String backupStorageUuid) {
        if (!getBackupStorageTypeFromImageInventory(img).equals(CephConstants.CEPH_BACKUP_STORAGE_TYPE)) {
            return;
        }
        FlowChain chain = FlowChainBuilder.newShareFlowChain();

        chain.setName("delete-image-info-from-ceph-metadata-file");
        String hostName = getHostNameFromImageInventory(img);
        Integer monPort = getMonPortFromImageInventory(img);
        String bsUrl = CephBackupStorageMonBase.META_DATA_PATH ;
        chain.then(new ShareFlow() {
            boolean metaDataExist = false;
            @Override
            public void setup() {
                flow(new NoRollbackFlow() {
                    String __name__ = "check-image-metadata-file-exist";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {
                        CephBackupStorageBase.CheckImageMetaDataFileExistCmd cmd = new CephBackupStorageBase.CheckImageMetaDataFileExistCmd();
                        cmd.setBackupStoragePath(bsUrl);
                        restf.asyncJsonPost(buildUrl(hostName, monPort, CephBackupStorageBase.CHECK_IMAGE_METADATA_FILE_EXIST), cmd,
                                new JsonAsyncRESTCallback<CephBackupStorageBase.CheckImageMetaDataFileExistRsp>() {
                                    @Override
                                    public void fail(ErrorCode err) {
                                        logger.error("Check image metadata file exist failed" + err.toString());
                                        trigger.fail(err);
                                    }

                                    @Override
                                    public void success(CephBackupStorageBase.CheckImageMetaDataFileExistRsp rsp) {
                                        if (!rsp.isSuccess()) {
                                            logger.error(String.format("Check image metadata file: %s failed", rsp.getBackupStorageMetaFileName()));
                                            ErrorCode ec = errf.instantiateErrorCode(
                                                    SysErrors.OPERATION_ERROR,
                                                    String.format("Check image metadata file: %s failed", rsp.getBackupStorageMetaFileName())
                                            );
                                            trigger.fail(ec);
                                        } else {
                                            if (!rsp.getExist()) {
                                                logger.info(String.format("Image metadata file %s is not exist", rsp.getBackupStorageMetaFileName()));
                                                ErrorCode ec = errf.instantiateErrorCode(
                                                        SysErrors.OPERATION_ERROR,
                                                        String.format("Image metadata file: %s is not exist", rsp.getBackupStorageMetaFileName())
                                                );
                                                trigger.fail(ec);
                                            } else {
                                                logger.info(String.format("Image metadata file %s exist", rsp.getBackupStorageMetaFileName()));
                                                trigger.next();
                                            }
                                        }
                                    }

                                    @Override
                                    public Class<CephBackupStorageBase.CheckImageMetaDataFileExistRsp> getReturnClass() {
                                        return CephBackupStorageBase.CheckImageMetaDataFileExistRsp.class;
                                    }
                                });
                    }
                });


                flow(new NoRollbackFlow() {
                    String __name__ = "delete-image-info";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {
                        CephBackupStorageBase.DeleteImageInfoFromMetaDataFileCmd deleteCmd = new CephBackupStorageBase.DeleteImageInfoFromMetaDataFileCmd();
                        deleteCmd.setImageUuid(img.getUuid());
                        deleteCmd.setImageBackupStorageUuid(backupStorageUuid);
                        deleteCmd.setBackupStoragePath(bsUrl);
                        restf.asyncJsonPost(buildUrl(hostName, monPort, CephBackupStorageBase.DELETE_IMAGES_METADATA), deleteCmd,
                                new JsonAsyncRESTCallback<CephBackupStorageBase.DeleteImageInfoFromMetaDataFileRsp>() {
                                    @Override
                                    public void fail(ErrorCode err) {
                                        logger.error("delete image metadata file failed" + err.toString());
                                    }

                                    @Override
                                    public void success(CephBackupStorageBase.DeleteImageInfoFromMetaDataFileRsp rsp) {
                                        if (!rsp.isSuccess()) {
                                            ErrorCode ec = errf.instantiateErrorCode(
                                                    SysErrors.OPERATION_ERROR,
                                                    String.format("delete image metadata file failed: %s", rsp.getError()));
                                            trigger.fail(ec);
                                        } else {
                                            if (rsp.getRet() != 0) {
                                                logger.info(String.format("delete image %s metadata failed : %s", img.getUuid(), rsp.getOut()));
                                                trigger.next();
                                            } else {
                                                logger.info(String.format("delete image %s metadata successfully", img.getUuid()));
                                                trigger.next();
                                            }
                                        }
                                    }

                                    @Override
                                    public Class<CephBackupStorageBase.DeleteImageInfoFromMetaDataFileRsp> getReturnClass() {
                                        return CephBackupStorageBase.DeleteImageInfoFromMetaDataFileRsp.class;
                                    }
                                });


                    }
                });

                done(new FlowDoneHandler() {
                    @Override
                    public void handle(Map data) {

                    }
                });

            }
        }).start();
    }

    public void failedToExpungeImage(ImageInventory img, ErrorCode err) {

    }

}
