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

import javax.persistence.Query;
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

    private String getAllImageVOInfo() {
        String allImageVOInfo = null;
        String sql = "select image from ImageVO image";
        Query q = dbf.getEntityManager().createQuery(sql);
        List<ImageVO> allImageInfo = q.getResultList();
        for ( ImageVO imageInfo: allImageInfo ) {
            if (allImageVOInfo != null) {
                allImageVOInfo = JSONObjectUtil.toJsonString(imageInfo) + "\n" + allImageVOInfo;
            } else {
                allImageVOInfo = JSONObjectUtil.toJsonString(imageInfo);
            }
        }
        return allImageVOInfo;
    }

    private String getAllImageBackupStorageRefVOInfo() {
        String allRefVOInfo = null;
        String sql = "select ref from ImageBackupStorageRefVO ref";
        Query q = dbf.getEntityManager().createQuery(sql);
        List<ImageBackupStorageRefVO> allRefInfo = q.getResultList();
        for ( ImageBackupStorageRefVO refInfo : allRefInfo ) {
            if (allRefVOInfo != null) {
                allRefVOInfo = JSONObjectUtil.toJsonString(refInfo) + "\n" + allRefVOInfo;
            } else {
                allRefVOInfo = JSONObjectUtil.toJsonString(refInfo);
            }
        }
        return allRefVOInfo;
    }

    private  String getImageVOInfo(ImageInventory image) {
        SimpleQuery<ImageVO> q = dbf.createQuery(ImageVO.class);
        q.add(ImageVO_.uuid, SimpleQuery.Op.EQ, image.getUuid());
        ImageVO imageVO = q.find();
        return JSONObjectUtil.toJsonString(imageVO);
    }

    private  String getImageBackupStorageRefVOInfo(ImageInventory image) {
        SimpleQuery<ImageBackupStorageRefVO> q = dbf.createQuery(ImageBackupStorageRefVO.class);
        q.add(ImageBackupStorageRefVO_.imageUuid, SimpleQuery.Op.EQ, image.getUuid());
        ImageBackupStorageRefVO imageBackupStorageRefVO = q.find();
        return JSONObjectUtil.toJsonString(imageBackupStorageRefVO);
    }

    private void restoreImagesMetadataToDatabase(String imagesInfo) {
        String[] metadatas =  imagesInfo.split("\n");
        //List<ImageVO> imageVOs = new ArrayList<>();
        // check data type, imageVO or ImageBackupStorageRefVO
        // if imageVO, import
        // else if ImageBackupStorageRefVO, import new data with new backupStorageUuid, imageUuid, installPath. status
        for ( String metadata : metadatas) {
            logger.debug(String.format("meilei:%s", metadata));
            ImageVO imageVO = JSONObjectUtil.toObject(metadata, ImageVO.class);
            //ImageInventory inv = imageVO.
            //imageVOs.add(imageVO);
        }
        //dbf.persist(imageVOs);
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
        String metaData = getImageVOInfo(img) + "\n" + getImageBackupStorageRefVOInfo(img);
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
        String metaData = getAllImageVOInfo() + "\n" + getAllImageBackupStorageRefVOInfo();
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
        SftpBackupStorageVO inv = (SftpBackupStorageVO) backupStorage.getVo();
        if (backupStorage.getImportImages()) {
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
                                restoreImagesMetadataToDatabase(rsp.getImagesMetaData());
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
