package com.cloud.engine.subsystem.api.storage;

import com.cloud.framework.async.AsyncCompletionCallback;
import com.cloud.legacymodel.to.DataStoreTO;
import com.cloud.legacymodel.to.DataTO;
import com.cloud.storage.command.CommandResult;

import java.util.Map;

public interface DataStoreDriver {
    Map<String, String> getCapabilities();

    DataTO getTO(DataObject data);

    DataStoreTO getStoreTO(DataStore store);

    void createAsync(DataStore store, DataObject data, AsyncCompletionCallback<CreateCmdResult> callback);

    void deleteAsync(DataStore store, DataObject data, AsyncCompletionCallback<CommandResult> callback);

    void copyAsync(DataObject srcdata, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback);

    boolean canCopy(DataObject srcData, DataObject destData);

    void resize(DataObject data, AsyncCompletionCallback<CreateCmdResult> callback);
}
