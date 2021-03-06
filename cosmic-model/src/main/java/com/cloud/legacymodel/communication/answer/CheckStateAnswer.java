package com.cloud.legacymodel.communication.answer;

import com.cloud.legacymodel.communication.command.CheckStateCommand;
import com.cloud.legacymodel.vm.VirtualMachine.State;

public class CheckStateAnswer extends Answer {
    State state;

    public CheckStateAnswer() {
    }

    public CheckStateAnswer(final CheckStateCommand cmd, final State state) {
        this(cmd, state, null);
    }

    public CheckStateAnswer(final CheckStateCommand cmd, final State state, final String details) {
        super(cmd, true, details);
        this.state = state;
    }

    public CheckStateAnswer(final CheckStateCommand cmd, final String details) {
        super(cmd, false, details);
        this.state = null;
    }

    public State getState() {
        return state;
    }
}
