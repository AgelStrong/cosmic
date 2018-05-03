package com.cloud.storage.image.datastore;

import com.cloud.engine.subsystem.api.storage.DataObject;
import com.cloud.engine.subsystem.api.storage.DataStore;
import com.cloud.engine.subsystem.api.storage.SnapshotInfo;
import com.cloud.engine.subsystem.api.storage.TemplateInfo;
import com.cloud.engine.subsystem.api.storage.VolumeInfo;
import com.cloud.legacymodel.storage.Upload;
import com.cloud.model.enumeration.ImageFormat;
import com.cloud.storage.ImageStore;

import java.util.Set;

public interface ImageStoreEntity extends DataStore, ImageStore {
    TemplateInfo getTemplate(long templateId);

    VolumeInfo getVolume(long volumeId);

    SnapshotInfo getSnapshot(long snapshotId);

    boolean exists(DataObject object);

    Set<TemplateInfo> listTemplates();

    String getMountPoint(); // get the mount point on ssvm.

    String createEntityExtractUrl(String installPath, ImageFormat format, DataObject dataObject);  // get the entity download URL

    void deleteExtractUrl(String installPath, String url, Upload.Type volume);
}
