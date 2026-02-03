package com.dataflow.dataloaders.util;


import com.dataflow.dataloaders.exception.ErrorDefinition;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Generated;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

@JsonInclude(Include.NON_NULL)
public class Response {
    private Integer code;
    private String message;
    private Object data;
    private List<ErrorDefinition> causedBy;

    public Response(Integer code, String message, String data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public Response(Integer code, String message, List<ErrorDefinition> causedBy) {
        this.code = code;
        this.message = message;
        this.causedBy = causedBy;
    }

    public static ResponseEntity<Response> createResponse(Object data) {
        return ResponseEntity.status(HttpStatus.CREATED).body(builder().code(201).message("Resource created successfully").data(data).build());
    }

    public static ResponseEntity<Response> updateResponse(Object data) {
        return ResponseEntity.status(HttpStatus.OK).body(builder().code(200).message("Resource updated successfully").data(data).build());
    }

    public static ResponseEntity<Response> getResponse(Object data) {
        return ResponseEntity.status(HttpStatus.OK).body(builder().code(200).message("Resource fetched successfully").data(data).build());
    }

    public static ResponseEntity<Response> listResponse(Object data) {
        return ResponseEntity.status(HttpStatus.OK).body(builder().code(200).message("Resources listed successfully").data(data).build());
    }

    public static ResponseEntity<Response> deleteResponse(Object data) {
        return ResponseEntity.status(HttpStatus.OK).body(builder().code(200).message("Resource deleted successfully").data(data).build());
    }

    public static ResponseEntity<Response> response(String message) {
        return ResponseEntity.status(HttpStatus.OK).body(builder().code(200).message(message).build());
    }

    public static ResponseEntity<Response> badRequestResponse(HttpStatus httpStatus, List<ErrorDefinition> causedBy) {
        return ResponseEntity.status(httpStatus).body(builder().code(httpStatus.value()).message(httpStatus.getReasonPhrase()).causedBy(causedBy).build());
    }

    public static ResponseEntity<Response> errorResponse(HttpStatus httpStatus, Response response) {
        return ResponseEntity.status(httpStatus).body(builder().code(response.getCode()).message(response.getMessage()).data(response.getData()).causedBy(response.getCausedBy()).build());
    }

    @Generated
    public static ResponseBuilder builder() {
        return new ResponseBuilder();
    }

    @Generated
    public Integer getCode() {
        return this.code;
    }

    @Generated
    public String getMessage() {
        return this.message;
    }

    @Generated
    public Object getData() {
        return this.data;
    }

    @Generated
    public List<ErrorDefinition> getCausedBy() {
        return this.causedBy;
    }

    @Generated
    public void setCode(final Integer code) {
        this.code = code;
    }

    @Generated
    public void setMessage(final String message) {
        this.message = message;
    }

    @Generated
    public void setData(final Object data) {
        this.data = data;
    }

    @Generated
    public void setCausedBy(final List<ErrorDefinition> causedBy) {
        this.causedBy = causedBy;
    }

    @Generated
    public String toString() {
        Integer var10000 = this.getCode();
        return "Response(code=" + var10000 + ", message=" + this.getMessage() + ", data=" + String.valueOf(this.getData()) + ", causedBy=" + String.valueOf(this.getCausedBy()) + ")";
    }

    @Generated
    public Response(final Integer code, final String message, final Object data, final List<ErrorDefinition> causedBy) {
        this.code = code;
        this.message = message;
        this.data = data;
        this.causedBy = causedBy;
    }

    @Generated
    public Response() {
    }

    @Generated
    public static class ResponseBuilder {
        @Generated
        private Integer code;
        @Generated
        private String message;
        @Generated
        private Object data;
        @Generated
        private List<ErrorDefinition> causedBy;

        @Generated
        ResponseBuilder() {
        }

        @Generated
        public ResponseBuilder code(final Integer code) {
            this.code = code;
            return this;
        }

        @Generated
        public ResponseBuilder message(final String message) {
            this.message = message;
            return this;
        }

        @Generated
        public ResponseBuilder data(final Object data) {
            this.data = data;
            return this;
        }

        @Generated
        public ResponseBuilder causedBy(final List<ErrorDefinition> causedBy) {
            this.causedBy = causedBy;
            return this;
        }

        @Generated
        public Response build() {
            return new Response(this.code, this.message, this.data, this.causedBy);
        }

        @Generated
        public String toString() {
            Integer var10000 = this.code;
            return "Response.ResponseBuilder(code=" + var10000 + ", message=" + this.message + ", data=" + String.valueOf(this.data) + ", causedBy=" + String.valueOf(this.causedBy) + ")";
        }
    }
}
