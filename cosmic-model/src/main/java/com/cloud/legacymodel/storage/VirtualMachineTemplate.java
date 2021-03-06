package com.cloud.legacymodel.storage;

import com.cloud.legacymodel.Identity;
import com.cloud.legacymodel.InternalIdentity;
import com.cloud.legacymodel.acl.ControlledEntity;
import com.cloud.legacymodel.statemachine.StateMachine2;
import com.cloud.legacymodel.statemachine.StateObject;
import com.cloud.legacymodel.statemachine.Transition;
import com.cloud.model.enumeration.HypervisorType;
import com.cloud.model.enumeration.ImageFormat;

import java.util.Date;
import java.util.Map;

public interface VirtualMachineTemplate extends ControlledEntity, Identity, InternalIdentity, StateObject<VirtualMachineTemplate.State> {
    @Override
    State getState();

    boolean isFeatured();

    boolean isPublicTemplate();

    boolean isExtractable();

    String getName();

    ImageFormat getFormat();

    String getDisplayText();

    boolean getEnablePassword();

    boolean getEnableSshKey();

    boolean isCrossZones();

    Date getCreated();

    long getGuestOSId();

    boolean isBootable();

    TemplateType getTemplateType();

    HypervisorType getHypervisorType();

    int getBits();

    String getUniqueName();

    String getUrl();

    String getChecksum();

    Long getSourceTemplateId();

    String getTemplateTag();

    Map getDetails();

    boolean isDynamicallyScalable();

    long getUpdatedCount();

    void incrUpdatedCount();

    Date getUpdated();

    enum State {
        Active,
        Inactive,
        NotUploaded,
        UploadInProgress,
        UploadError,
        UploadAbandoned;

        private final static StateMachine2<State, Event, VirtualMachineTemplate> s_fsm = new StateMachine2<>();

        static {
            s_fsm.addTransition(new Transition<>(NotUploaded, Event.OperationTimeout, UploadAbandoned, null));
            s_fsm.addTransition(new Transition<>(NotUploaded, Event.UploadRequested, UploadInProgress, null));
            s_fsm.addTransition(new Transition<>(NotUploaded, Event.OperationSucceeded, Active, null));
            s_fsm.addTransition(new Transition<>(NotUploaded, Event.OperationFailed, UploadError, null));
            s_fsm.addTransition(new Transition<>(UploadInProgress, Event.OperationSucceeded, Active, null));
            s_fsm.addTransition(new Transition<>(UploadInProgress, Event.OperationFailed, UploadError, null));
            s_fsm.addTransition(new Transition<>(UploadInProgress, Event.OperationTimeout, UploadError, null));
        }

        public static StateMachine2<State, Event, VirtualMachineTemplate> getStateMachine() {
            return s_fsm;
        }
    }

    enum Event {
        OperationFailed,
        OperationSucceeded,
        UploadRequested,
        OperationTimeout
    }

    enum TemplateFilter {
        featured, // returns templates that have been marked as featured and public
        self, // returns templates that have been registered or created by the calling user
        selfexecutable, // same as self, but only returns templates that are ready to be deployed with
        shared, // including templates that have been granted to the calling user by another user
        sharedexecutable, // ready templates that have been granted to the calling user by another user
        executable, // templates that are owned by the calling user, or public templates, that can be used to deploy a
        community, // returns templates that have been marked as public but not featured
        all // all templates (only usable by admins)
    }
}
