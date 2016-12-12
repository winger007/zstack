package org.zstack.storage.backup.sftp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.util.UriComponentsBuilder;
import org.zstack.core.CoreGlobalProperty;
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
import org.zstack.utils.Utils;
import org.zstack.utils.gson.JSONObjectUtil;
import org.zstack.utils.logging.CLogger;

import java.util.List;
import java.util.Map;

/**
 * Created by Mei Lei <meilei007@gmail.com> on 11/3/16.
 */
public class SftpBackupStorageMetaDataMaker implements AddImageExtensionPoint, AddBackupStorageExtensionPoint {
    private static final CLogger logger = Utils.getLogger(SftpBackupStorage.class);
    @Autowired
    protected RESTFacade restf;
    @Autowired
    private DatabaseFacade dbf;
    @Autowired
    private ErrorFacade errf;

    private String buildUrl(String subPath, String hostName) {
        UriComponentsBuilder ub = UriComponentsBuilder.newInstance();
        ub.scheme(SftpBackupStorageGlobalProperty.AGENT_URL_SCHEME);
        if (CoreGlobalProperty.UNIT_TEST_ON) {
            ub.host("localhost");
        } else {
            ub.host(hostName);
        }

        ub.port(SftpBackupStorageGlobalProperty.AGENT_PORT);
        if (!"".equals(SftpBackupStorageGlobalProperty.AGENT_URL_ROOT_PATH)) {
            ub.path(SftpBackupStorageGlobalProperty.AGENT_URL_ROOT_PATH);
        }
        ub.path(subPath);
        return ub.build().toUriString();
    }

    private String getAllImageInventories() {
        String allImageInventories = null;
        SimpleQuery<ImageVO> q = dbf.createQuery(ImageVO.class);
        List<ImageVO> allImageVO = q.list();
        for ( ImageVO imageVO: allImageVO ) {
            if (allImageInventories != null) {
                allImageInventories = JSONObjectUtil.toJsonString(ImageInventory.valueOf(imageVO)) + "\n" + allImageInventories;
            } else {
                allImageInventories = JSONObjectUtil.toJsonString(ImageInventory.valueOf(imageVO));
            }
        }
        return allImageInventories;
    }

    private String getAllImageBackupStorageRefInventories() {
        String allImageBackupStorageRefInventories = null;
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        List<ImageBackupStorageRefVO> allImageBackupStorageRefVO = q.list();

        for ( ImageBackupStorageRefVO imageBackupStorageRefVO : allImageBackupStorageRefVO ) {
            if (allImageBackupStorageRefInventories != null) {
                allImageBackupStorageRefInventories = JSONObjectUtil.toJsonString(ImageBackupStorageRefInventory.valueOf(imageBackupStorageRefVO)) + "\n" + allImageBackupStorageRefInventories;
            } else {
                allImageBackupStorageRefInventories = JSONObjectUtil.toJsonString(ImageBackupStorageRefInventory.valueOf(imageBackupStorageRefVO));
            }
        }
        return allImageBackupStorageRefInventories;
    }

    private  String getImageInventory(ImageInventory image) {
        return JSONObjectUtil.toJsonString(image);
    }

    private  String getImageBackupStorageRefInventory(ImageInventory image) {
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        q.add(ImageBackupStorageRefVO_.imageUuid, SimpleQuery.Op.EQ, image.getUuid());
        ImageBackupStorageRefVO imageBackupStorageRefVO = q.find();
        return JSONObjectUtil.toJsonString(ImageBackupStorageRefInventory.valueOf(imageBackupStorageRefVO));
    }

    private void restoreImagesBackupStorageMetadataToDatabase(String imagesBackupStoragesInfo, String backupStorageUuid) {
        List<ImageInventory>  imageInventories = null;
        List<ImageBackupStorageRefInventory> backupStorageRefInventories = null;
        List<ImageBackupStorageRefInventory> tmpBackupStorageRefInventories = null;
        List<ImageBackupStorageRefInventory> newBackupStorageRefInventories = null;
        String[] metadatas =  imagesBackupStoragesInfo.split("\n");
        //List<ImageVO> imageVOs = new ArrayList<>();
        // check data type, imageVO or ImageBackupStorageRefVO
        // if imageVO, import
        // else if ImageBackupStorageRefVO, import new data with new backupStorageUuid, imageUuid, installPath. status
        for ( String metadata : metadatas) {
            logger.debug(String.format("meilei:%s", metadata));
            if (metadata.contains("backupStorageRefs")) {
                // this is imageInventory metaData
                ImageInventory imageInventory = JSONObjectUtil.toObject(metadata, ImageInventory.class);
                tmpBackupStorageRefInventories = imageInventory.getBackupStorageRefs();
                for ( ImageBackupStorageRefInventory tmpBackupStorageRefInventory : tmpBackupStorageRefInventories ) {
                    tmpBackupStorageRefInventory.setBackupStorageUuid(backupStorageUuid);
                    newBackupStorageRefInventories.add(tmpBackupStorageRefInventory);
                }
                imageInventory.setBackupStorageRefs(newBackupStorageRefInventories);
                imageInventories.add(imageInventory);
            } else {
                // this is ImageBackupStorageRefInventory metaData
                ImageBackupStorageRefInventory backupStorageRefInventory = JSONObjectUtil.toObject(metadata, ImageBackupStorageRefInventory.class);
                backupStorageRefInventory.setBackupStorageUuid(backupStorageUuid);
                backupStorageRefInventories.add(backupStorageRefInventory);
            }
        }
        if (imageInventories != null) {
            dbf.persistAndRefresh(imageInventories);
        } else {
            logger.debug("import imageInventories metadata is empty");
        }
        if (backupStorageRefInventories != null) {
            dbf.persistAndRefresh(backupStorageRefInventories);
        } else {
            logger.debug("import backupStorageRefInventoreies metadata is empty");
        }
    }


    private String getBsUrlFromImageInventory(ImageInventory img) {
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        q.select(ImageBackupStorageRefVO_.backupStorageUuid);
        q.add(ImageBackupStorageRefVO_.imageUuid, SimpleQuery.Op.EQ, img.getUuid());
        List<String> bsUuids = q.listValue();
        if (bsUuids.isEmpty()) {
            return null;
        }
        String bsUuid = bsUuids.get(0);

        SimpleQuery<SftpBackupStorageVO> q2 = dbf.createQuery(SftpBackupStorageVO.class);
        q.select(SftpBackupStorageVO_.url);
        q.add(SftpBackupStorageVO_.uuid, SimpleQuery.Op.EQ, bsUuid);
        List<SftpBackupStorageVO> urls= q2.listValue();
        if (urls.isEmpty())  {
            return null;
        }
        SftpBackupStorageVO bsVO = urls.get(0);
        return bsVO.getUrl();
    }

    private String getHostNameFromImageInventory(ImageInventory img) {
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        q.select(ImageBackupStorageRefVO_.backupStorageUuid);
        q.add(ImageBackupStorageRefVO_.imageUuid, SimpleQuery.Op.EQ, img.getUuid());
        List<String> bsUuids = q.listValue();
        if (bsUuids.isEmpty()) {
            return null;
        }
        String bsUuid = bsUuids.get(0);

        SimpleQuery<SftpBackupStorageVO> q2 = dbf.createQuery(SftpBackupStorageVO.class);
        q.select(SftpBackupStorageVO_.hostname);
        q.add(SftpBackupStorageVO_.uuid, SimpleQuery.Op.EQ, bsUuid);
        List<SftpBackupStorageVO> hostNames = q2.listValue();
        if (hostNames.isEmpty())  {
            return null;
        }
        SftpBackupStorageVO bsVO = hostNames.get(0);
        return bsVO.getHostname();
    }

    private  void dumpImageDataToMetaDataFile(ImageInventory img) {
        SftpBackupStorageCommands.DumpImageInfoToMetaDataFileCmd dumpCmd = new SftpBackupStorageCommands.DumpImageInfoToMetaDataFileCmd();
        String metaData = getImageInventory(img) + "\n" + getImageBackupStorageRefInventory(img);
        dumpCmd.setImageMetaData(metaData);
        dumpCmd.setBackupStoragePath(getBsUrlFromImageInventory(img));
        restf.asyncJsonPost(buildUrl(SftpBackupStorageConstant.DUMP_IMAGE_METADATA_TO_FILE, getHostNameFromImageInventory(img)), dumpCmd,
                new JsonAsyncRESTCallback<SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp >() {
                    @Override
                    public void fail(ErrorCode err) {
                        logger.error("Dump image metadata failed" + err.toString());
                    }

                    @Override
                    public void success(SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp rsp) {
                        if (!rsp.isSuccess()) {
                            logger.error("Dump image metadata failed");
                        } else {
                            logger.info("Dump image metadata successfully");
                        }
                    }

                    @Override
                    public Class<SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp> getReturnClass() {
                        return SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp.class;
                    }
                });
    }

    private  void dumpAllImageDataToMetaDataFile(ImageInventory img) {
        SftpBackupStorageCommands.DumpImageInfoToMetaDataFileCmd dumpCmd = new SftpBackupStorageCommands.DumpImageInfoToMetaDataFileCmd();
        String metaData = getAllImageInventories() + "\n" + getAllImageBackupStorageRefInventories();
        dumpCmd.setImageMetaData(metaData);
        dumpCmd.setBackupStoragePath(getBsUrlFromImageInventory(img));
        restf.asyncJsonPost(buildUrl(SftpBackupStorageConstant.DUMP_IMAGE_METADATA_TO_FILE, getHostNameFromImageInventory(img)), dumpCmd,
                new JsonAsyncRESTCallback<SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp >() {
                    @Override
                    public void fail(ErrorCode err) {
                        logger.error("Dump image metadata failed" + err.toString());
                    }

                    @Override
                    public void success(SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp rsp) {
                        if (!rsp.isSuccess()) {
                            logger.error("Dump image metadata to file failed");
                        } else {
                            logger.info("Dump image metadata to file successfully");
                        }
                    }

                    @Override
                    public Class<SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp> getReturnClass() {
                        return SftpBackupStorageCommands.DumpImageInfoToMetaDataFileRsp.class;
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

        FlowChain chain = FlowChainBuilder.newShareFlowChain();

        chain.setName("add-image-metadata-to-backupStorage-file");
        chain.then(new ShareFlow() {
            boolean metaDataExist = false;
            @Override
            public void setup() {
                flow(new NoRollbackFlow() {
                    String __name__ = "check-image-metadata-file-exist";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {
                        //todo: cmd should post bs uuid
                        SftpBackupStorageCommands.CheckImageMetaDataFileExistCmd cmd = new SftpBackupStorageCommands.CheckImageMetaDataFileExistCmd();
                        cmd.setBackupStoragePath(getBsUrlFromImageInventory(img));
                        restf.asyncJsonPost(buildUrl(SftpBackupStorageConstant.CHECK_IMAGE_METADATA_FILE_EXIST, getHostNameFromImageInventory(img)), cmd,
                                new JsonAsyncRESTCallback<SftpBackupStorageCommands.CheckImageMetaDataFileExistRsp>() {
                                    @Override
                                    public void fail(ErrorCode err) {
                                        logger.error("Check image metadata file exist failed" + err.toString());
                                        trigger.fail(err);
                                    }

                                    @Override
                                    public void success(SftpBackupStorageCommands.CheckImageMetaDataFileExistRsp rsp) {
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
                                    public Class<SftpBackupStorageCommands.CheckImageMetaDataFileExistRsp> getReturnClass() {
                                        return SftpBackupStorageCommands.CheckImageMetaDataFileExistRsp.class;
                                    }
                                });
                    }
                });


                flow(new NoRollbackFlow() {
                    String __name__ = "create-image-metadata-file";

                    @Override
                    public void run(FlowTrigger trigger, Map data) {

                        if (!metaDataExist) {
                            SftpBackupStorageCommands.GenerateImageMetaDataFileCmd generateCmd = new SftpBackupStorageCommands.GenerateImageMetaDataFileCmd();
                            generateCmd.setBackupStoragePath(getBsUrlFromImageInventory(img));
                            restf.asyncJsonPost(buildUrl(SftpBackupStorageConstant.GENERATE_IMAGE_METADATA_FILE, getHostNameFromImageInventory(img)), generateCmd,
                                    new JsonAsyncRESTCallback<SftpBackupStorageCommands.GenerateImageMetaDataFileRsp>() {
                                        @Override
                                        public void fail(ErrorCode err) {
                                            logger.error("Create image metadata file failed" + err.toString());
                                        }

                                        @Override
                                        public void success(SftpBackupStorageCommands.GenerateImageMetaDataFileRsp rsp) {
                                            if (!rsp.isSuccess()) {
                                                ErrorCode ec = errf.instantiateErrorCode(
                                                        SysErrors.OPERATION_ERROR,
                                                        String.format("Create image metadata file : %s failed", rsp.BackupStorageMetaFileName));
                                                trigger.fail(ec);
                                            } else {
                                                logger.info("Create image metadata file successfully");
                                                dumpAllImageDataToMetaDataFile(img);
                                                trigger.next();
                                            }
                                        }

                                        @Override
                                        public Class<SftpBackupStorageCommands.GenerateImageMetaDataFileRsp> getReturnClass() {
                                            return SftpBackupStorageCommands.GenerateImageMetaDataFileRsp.class;
                                        }
                                    });

                        } else {
                            dumpImageDataToMetaDataFile(img);
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
        logger.debug("meilei: entering to import images metadata");
        SftpBackupStorageVO inv = (SftpBackupStorageVO) backupStorage.getVo();
        if (backupStorage.getImportImages()) {
            logger.debug("meilei: Starting to import images metadata");
            SftpBackupStorageCommands.GetImagesMetaDataCmd cmd = new SftpBackupStorageCommands.GetImagesMetaDataCmd();
            cmd.setBackupStoragePath(inv.getUrl());
            restf.asyncJsonPost(buildUrl(SftpBackupStorageConstant.GET_IMAGES_METADATA, inv.getHostname()), cmd,
                    new JsonAsyncRESTCallback<SftpBackupStorageCommands.GetImagesMetaDataRsp>() {
                        @Override
                        public void fail(ErrorCode err) {
                            logger.error("Check image metadata file exist failed" + err.toString());
                        }

                        @Override
                        public void success(SftpBackupStorageCommands.GetImagesMetaDataRsp rsp) {
                            if (!rsp.isSuccess()) {
                                logger.error(String.format("Get images metadata: %s failed", rsp.getImagesMetaData()));
                            } else {
                                restoreImagesBackupStorageMetadataToDatabase(rsp.getImagesMetaData(), backupStorage.getVo().getUuid());
                                logger.info(String.format("Get images metadata: %s success", rsp.getImagesMetaData()));
                            }
                        }

                        @Override
                        public Class<SftpBackupStorageCommands.GetImagesMetaDataRsp> getReturnClass() {
                            return SftpBackupStorageCommands.GetImagesMetaDataRsp.class;
                        }
                    });
        }

    }
    public void failedToAddBackupStorage(AddBackupStorageStruct backupStorage, ErrorCode err) {

    }

}
