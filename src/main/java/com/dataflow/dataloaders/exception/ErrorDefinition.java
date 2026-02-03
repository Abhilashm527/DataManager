package com.dataflow.dataloaders.exception;


import lombok.Generated;
import org.springframework.http.HttpStatus;

public class ErrorDefinition {
    private String label;
    private String code;
    private ErrorLevel level;
    private ErrorSeverity severity;
    private String message;
    private HttpStatus httpStatus;
    private String sourceApplication;

    @Generated
    public static ErrorDefinitionBuilder builder() {
        return new ErrorDefinitionBuilder();
    }

    @Generated
    public String getLabel() {
        return this.label;
    }

    @Generated
    public String getCode() {
        return this.code;
    }

    @Generated
    public ErrorLevel getLevel() {
        return this.level;
    }

    @Generated
    public ErrorSeverity getSeverity() {
        return this.severity;
    }

    @Generated
    public String getMessage() {
        return this.message;
    }

    @Generated
    public HttpStatus getHttpStatus() {
        return this.httpStatus;
    }

    @Generated
    public String getSourceApplication() {
        return this.sourceApplication;
    }

    @Generated
    public void setLabel(final String label) {
        this.label = label;
    }

    @Generated
    public void setCode(final String code) {
        this.code = code;
    }

    @Generated
    public void setLevel(final ErrorLevel level) {
        this.level = level;
    }

    @Generated
    public void setSeverity(final ErrorSeverity severity) {
        this.severity = severity;
    }

    @Generated
    public void setMessage(final String message) {
        this.message = message;
    }

    @Generated
    public void setHttpStatus(final HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Generated
    public void setSourceApplication(final String sourceApplication) {
        this.sourceApplication = sourceApplication;
    }

    @Generated
    public String toString() {
        String var10000 = this.getLabel();
        return "ErrorDefinition(label=" + var10000 + ", code=" + this.getCode() + ", level=" + String.valueOf(this.getLevel()) + ", severity=" + String.valueOf(this.getSeverity()) + ", message=" + this.getMessage() + ", httpStatus=" + String.valueOf(this.getHttpStatus()) + ", sourceApplication=" + this.getSourceApplication() + ")";
    }

    @Generated
    public ErrorDefinition() {
    }

    @Generated
    public ErrorDefinition(final String label, final String code, final ErrorLevel level, final ErrorSeverity severity, final String message, final HttpStatus httpStatus, final String sourceApplication) {
        this.label = label;
        this.code = code;
        this.level = level;
        this.severity = severity;
        this.message = message;
        this.httpStatus = httpStatus;
        this.sourceApplication = sourceApplication;
    }

    @Generated
    public static class ErrorDefinitionBuilder {
        @Generated
        private String label;
        @Generated
        private String code;
        @Generated
        private ErrorLevel level;
        @Generated
        private ErrorSeverity severity;
        @Generated
        private String message;
        @Generated
        private HttpStatus httpStatus;
        @Generated
        private String sourceApplication;

        @Generated
        ErrorDefinitionBuilder() {
        }

        @Generated
        public ErrorDefinitionBuilder label(final String label) {
            this.label = label;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder code(final String code) {
            this.code = code;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder level(final ErrorLevel level) {
            this.level = level;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder severity(final ErrorSeverity severity) {
            this.severity = severity;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder message(final String message) {
            this.message = message;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder httpStatus(final HttpStatus httpStatus) {
            this.httpStatus = httpStatus;
            return this;
        }

        @Generated
        public ErrorDefinitionBuilder sourceApplication(final String sourceApplication) {
            this.sourceApplication = sourceApplication;
            return this;
        }

        @Generated
        public ErrorDefinition build() {
            return new ErrorDefinition(this.label, this.code, this.level, this.severity, this.message, this.httpStatus, this.sourceApplication);
        }

        @Generated
        public String toString() {
            String var10000 = this.label;
            return "ErrorDefinition.ErrorDefinitionBuilder(label=" + var10000 + ", code=" + this.code + ", level=" + String.valueOf(this.level) + ", severity=" + String.valueOf(this.severity) + ", message=" + this.message + ", httpStatus=" + String.valueOf(this.httpStatus) + ", sourceApplication=" + this.sourceApplication + ")";
        }
    }
}
