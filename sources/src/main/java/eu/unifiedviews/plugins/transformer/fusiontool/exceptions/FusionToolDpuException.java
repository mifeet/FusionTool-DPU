package eu.unifiedviews.plugins.transformer.fusiontool.exceptions;


import eu.unifiedviews.dpu.DPUException;

/**
 * A general Fusion Tool DPU exception.
 * @author Jan Michelfeit
 */
public class FusionToolDpuException extends DPUException {
    private static final long serialVersionUID = 3420323334894817996L;

    private final Integer errorCode;

    /**
     * Constructs a new exception with the given cause.
     * @param errorCode code of the error
     * @param cause the cause
     */
    public FusionToolDpuException(Integer errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param errorCode code of the error
     * @param message the detail message
     * @param cause the cause
     */
    public FusionToolDpuException(Integer errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the given message.
     * @param errorCode code of the error
     * @param message the detail message
     */
    public FusionToolDpuException(Integer errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Return the error code of the error.
     * @see eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuErrorCodes
     * @return error code or null
     */
    public Integer getErrorCode() {
        return errorCode;
    }
    
    @Override
    public String getMessage() {
        return "(" + getErrorCode() + ") " + super.getMessage(); 
    }
}
