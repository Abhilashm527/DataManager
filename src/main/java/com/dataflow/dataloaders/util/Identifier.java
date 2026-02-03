package com.dataflow.dataloaders.util;

import com.dataflow.dataloaders.enums.EnumIdentifier;
import lombok.Generated;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;

import java.io.Serializable;
import java.util.UUID;

public class Identifier implements Serializable {
    private static final long serialVersionUID = 2442778896121136685L;
    private Long id;
    private Long foreignKey;
    private String word;
    private Long localeCd;
    private UUID uuid;
    private HttpHeaders headers;
    private Pageable pageable;
    private Identifier parentIdentifier;

    public Identifier(Long attribute, EnumIdentifier enumIdentifier) {
        switch (enumIdentifier) {
            case ID -> this.id = attribute;
            case FOREIGNKEY -> this.foreignKey = attribute;
            case LOCALECD -> this.localeCd = attribute;
        }

    }

    public Identifier(String word) {
        this.word = word;
    }

    public Identifier(UUID uuid) {
        this.uuid = uuid;
    }

    public Identifier(Long id, Long localeCd) {
        this.id = id;
        this.localeCd = localeCd;
    }

    public Identifier(Long id, Long foreignKey, Long localeCd) {
        this(id, localeCd);
        this.foreignKey = foreignKey;
    }

    public Identifier(Long id, String word) {
        this.id = id;
        this.word = word;
    }

    public Identifier(Long id, HttpHeaders headers) {
        this.id = id;
        this.headers = headers;
    }

    public Identifier(String word, Long localeCd) {
        this.word = word;
        this.localeCd = localeCd;
    }

    public Identifier(Long id, String word, Long localeCd) {
        this(id, localeCd);
        this.word = word;
    }

    public Identifier(Long id, Long foreignKey, String word, Long localeCd) {
        this(id, foreignKey, localeCd);
        this.word = word;
    }

    @Generated
    public static IdentifierBuilder builder() {
        return new IdentifierBuilder();
    }

    @Generated
    public Long getId() {
        return this.id;
    }

    @Generated
    public Long getForeignKey() {
        return this.foreignKey;
    }

    @Generated
    public String getWord() {
        return this.word;
    }

    @Generated
    public Long getLocaleCd() {
        return this.localeCd;
    }

    @Generated
    public UUID getUuid() {
        return this.uuid;
    }

    @Generated
    public HttpHeaders getHeaders() {
        return this.headers;
    }

    @Generated
    public Pageable getPageable() {
        return this.pageable;
    }

    @Generated
    public Identifier getParentIdentifier() {
        return this.parentIdentifier;
    }

    @Generated
    public void setId(final Long id) {
        this.id = id;
    }

    @Generated
    public void setForeignKey(final Long foreignKey) {
        this.foreignKey = foreignKey;
    }

    @Generated
    public void setWord(final String word) {
        this.word = word;
    }

    @Generated
    public void setLocaleCd(final Long localeCd) {
        this.localeCd = localeCd;
    }

    @Generated
    public void setUuid(final UUID uuid) {
        this.uuid = uuid;
    }

    @Generated
    public void setHeaders(final HttpHeaders headers) {
        this.headers = headers;
    }

    @Generated
    public void setPageable(final Pageable pageable) {
        this.pageable = pageable;
    }

    @Generated
    public void setParentIdentifier(final Identifier parentIdentifier) {
        this.parentIdentifier = parentIdentifier;
    }

    @Generated
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Identifier)) {
            return false;
        } else {
            Identifier other = (Identifier)o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$id = this.getId();
                Object other$id = other.getId();
                if (this$id == null) {
                    if (other$id != null) {
                        return false;
                    }
                } else if (!this$id.equals(other$id)) {
                    return false;
                }

                Object this$foreignKey = this.getForeignKey();
                Object other$foreignKey = other.getForeignKey();
                if (this$foreignKey == null) {
                    if (other$foreignKey != null) {
                        return false;
                    }
                } else if (!this$foreignKey.equals(other$foreignKey)) {
                    return false;
                }

                Object this$localeCd = this.getLocaleCd();
                Object other$localeCd = other.getLocaleCd();
                if (this$localeCd == null) {
                    if (other$localeCd != null) {
                        return false;
                    }
                } else if (!this$localeCd.equals(other$localeCd)) {
                    return false;
                }

                Object this$word = this.getWord();
                Object other$word = other.getWord();
                if (this$word == null) {
                    if (other$word != null) {
                        return false;
                    }
                } else if (!this$word.equals(other$word)) {
                    return false;
                }

                Object this$uuid = this.getUuid();
                Object other$uuid = other.getUuid();
                if (this$uuid == null) {
                    if (other$uuid != null) {
                        return false;
                    }
                } else if (!this$uuid.equals(other$uuid)) {
                    return false;
                }

                Object this$headers = this.getHeaders();
                Object other$headers = other.getHeaders();
                if (this$headers == null) {
                    if (other$headers != null) {
                        return false;
                    }
                } else if (!this$headers.equals(other$headers)) {
                    return false;
                }

                Object this$pageable = this.getPageable();
                Object other$pageable = other.getPageable();
                if (this$pageable == null) {
                    if (other$pageable != null) {
                        return false;
                    }
                } else if (!this$pageable.equals(other$pageable)) {
                    return false;
                }

                Object this$parentIdentifier = this.getParentIdentifier();
                Object other$parentIdentifier = other.getParentIdentifier();
                if (this$parentIdentifier == null) {
                    if (other$parentIdentifier != null) {
                        return false;
                    }
                } else if (!this$parentIdentifier.equals(other$parentIdentifier)) {
                    return false;
                }

                return true;
            }
        }
    }

    @Generated
    protected boolean canEqual(final Object other) {
        return other instanceof Identifier;
    }

    @Generated
    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $id = this.getId();
        result = result * 59 + ($id == null ? 43 : $id.hashCode());
        Object $foreignKey = this.getForeignKey();
        result = result * 59 + ($foreignKey == null ? 43 : $foreignKey.hashCode());
        Object $localeCd = this.getLocaleCd();
        result = result * 59 + ($localeCd == null ? 43 : $localeCd.hashCode());
        Object $word = this.getWord();
        result = result * 59 + ($word == null ? 43 : $word.hashCode());
        Object $uuid = this.getUuid();
        result = result * 59 + ($uuid == null ? 43 : $uuid.hashCode());
        Object $headers = this.getHeaders();
        result = result * 59 + ($headers == null ? 43 : $headers.hashCode());
        Object $pageable = this.getPageable();
        result = result * 59 + ($pageable == null ? 43 : $pageable.hashCode());
        Object $parentIdentifier = this.getParentIdentifier();
        result = result * 59 + ($parentIdentifier == null ? 43 : $parentIdentifier.hashCode());
        return result;
    }

    @Generated
    public String toString() {
        Long var10000 = this.getId();
        return "Identifier(id=" + var10000 + ", foreignKey=" + this.getForeignKey() + ", word=" + this.getWord() + ", localeCd=" + this.getLocaleCd() + ", uuid=" + String.valueOf(this.getUuid()) + ", headers=" + String.valueOf(this.getHeaders()) + ", pageable=" + String.valueOf(this.getPageable()) + ", parentIdentifier=" + String.valueOf(this.getParentIdentifier()) + ")";
    }

    @Generated
    public Identifier() {
    }

    @Generated
    public Identifier(final Long id, final Long foreignKey, final String word, final Long localeCd, final UUID uuid, final HttpHeaders headers, final Pageable pageable, final Identifier parentIdentifier) {
        this.id = id;
        this.foreignKey = foreignKey;
        this.word = word;
        this.localeCd = localeCd;
        this.uuid = uuid;
        this.headers = headers;
        this.pageable = pageable;
        this.parentIdentifier = parentIdentifier;
    }

    @Generated
    public static class IdentifierBuilder {
        @Generated
        private Long id;
        @Generated
        private Long foreignKey;
        @Generated
        private String word;
        @Generated
        private Long localeCd;
        @Generated
        private UUID uuid;
        @Generated
        private HttpHeaders headers;
        @Generated
        private Pageable pageable;
        @Generated
        private Identifier parentIdentifier;

        @Generated
        IdentifierBuilder() {
        }

        @Generated
        public IdentifierBuilder id(final Long id) {
            this.id = id;
            return this;
        }

        @Generated
        public IdentifierBuilder foreignKey(final Long foreignKey) {
            this.foreignKey = foreignKey;
            return this;
        }

        @Generated
        public IdentifierBuilder word(final String word) {
            this.word = word;
            return this;
        }

        @Generated
        public IdentifierBuilder localeCd(final Long localeCd) {
            this.localeCd = localeCd;
            return this;
        }

        @Generated
        public IdentifierBuilder uuid(final UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        @Generated
        public IdentifierBuilder headers(final HttpHeaders headers) {
            this.headers = headers;
            return this;
        }

        @Generated
        public IdentifierBuilder pageable(final Pageable pageable) {
            this.pageable = pageable;
            return this;
        }

        @Generated
        public IdentifierBuilder parentIdentifier(final Identifier parentIdentifier) {
            this.parentIdentifier = parentIdentifier;
            return this;
        }

        @Generated
        public Identifier build() {
            return new Identifier(this.id, this.foreignKey, this.word, this.localeCd, this.uuid, this.headers, this.pageable, this.parentIdentifier);
        }

        @Generated
        public String toString() {
            Long var10000 = this.id;
            return "Identifier.IdentifierBuilder(id=" + var10000 + ", foreignKey=" + this.foreignKey + ", word=" + this.word + ", localeCd=" + this.localeCd + ", uuid=" + String.valueOf(this.uuid) + ", headers=" + String.valueOf(this.headers) + ", pageable=" + String.valueOf(this.pageable) + ", parentIdentifier=" + String.valueOf(this.parentIdentifier) + ")";
        }
    }
}
