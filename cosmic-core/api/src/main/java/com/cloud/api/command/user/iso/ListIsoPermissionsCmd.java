// Licensedname = "listIsoPermissions",  to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.cloud.api.command.user.iso;

import com.cloud.api.APICommand;
import com.cloud.api.APICommandGroup;
import com.cloud.api.BaseListTemplateOrIsoPermissionsCmd;
import com.cloud.api.ResponseObject.ResponseView;
import com.cloud.api.response.TemplatePermissionsResponse;
import com.cloud.legacymodel.storage.VirtualMachineTemplate;
import com.cloud.model.enumeration.ImageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@APICommand(name = "listIsoPermissions", group = APICommandGroup.ISOService, description = "List ISO visibility and all accounts that have permissions to view this ISO.", responseObject =
        TemplatePermissionsResponse.class, responseView = ResponseView.Restricted,
        requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = false)
public class ListIsoPermissionsCmd extends BaseListTemplateOrIsoPermissionsCmd {
    protected String getResponseName() {
        return "listisopermissionsresponse";
    }

    @Override
    protected Logger getLogger() {
        return LoggerFactory.getLogger(ListIsoPermissionsCmd.class.getName());
    }

    @Override
    protected boolean templateIsCorrectType(final VirtualMachineTemplate template) {
        return template.getFormat().equals(ImageFormat.ISO);
    }

    @Override
    public String getMediaType() {
        return "iso";
    }

    @Override
    public void execute() {
        executeWithView(ResponseView.Restricted);
    }
}
