package com.cloud.api.response;

import com.cloud.api.ApiConstants;
import com.cloud.api.BaseResponse;
import com.cloud.serializer.Param;

import com.google.gson.annotations.SerializedName;

public class CloudIdentifierResponse extends BaseResponse {

    @SerializedName(ApiConstants.USER_ID)
    @Param(description = "the user ID for the cloud identifier")
    private String userId;

    @SerializedName("cloudidentifier")
    @Param(description = "the cloud identifier")
    private String cloudIdentifier;

    @SerializedName("signature")
    @Param(description = "the signed response for the cloud identifier")
    private String signature;

    public String getUserId() {
        return userId;
    }

    public void setUserId(final String userId) {
        this.userId = userId;
    }

    public String getCloudIdentifier() {
        return cloudIdentifier;
    }

    public void setCloudIdentifier(final String cloudIdentifier) {
        this.cloudIdentifier = cloudIdentifier;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(final String signature) {
        this.signature = signature;
    }
}
