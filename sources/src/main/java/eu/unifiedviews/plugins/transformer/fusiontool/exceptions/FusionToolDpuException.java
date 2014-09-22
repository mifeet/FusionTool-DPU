package eu.unifiedviews.plugins.transformer.fusiontool.exceptions;


import eu.unifiedviews.dpu.DPUException;

/**
 * A general Fusion Tool DPU exception.
 */
public class FusionToolDpuException extends DPUException {
    private static final long serialVersionUID = 3420323334894817996L;

    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public FusionToolDpuException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public FusionToolDpuException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public FusionToolDpuException(String message) {
        super(message);
    }
}
