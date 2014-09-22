package cz.cuni.mff.odcleanstore.fusiontool.util;

import eu.unifiedviews.dpu.DPUContext;

public class DPUContextIsCanceledCallback implements IsCanceledCallback {
    private final DPUContext context;

    public DPUContextIsCanceledCallback(DPUContext context) {
        this.context = context;
    }

    @Override
    public boolean isCanceled() {
        return context.canceled();
    }
}
