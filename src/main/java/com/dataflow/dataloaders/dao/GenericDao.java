package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.exception.DataloadersException;
import jakarta.validation.constraints.NotNull;
import lombok.Generated;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface GenericDao<T, I, S> {
    default T create(@NotNull T model, I identifier) {
        return (T)this.createV1(Objects.requireNonNull(model, "Model can't be null"), identifier).orElse((T) null);
    }

    Optional<T> createV1(T model, I identifier);

    Long insert(T model, I identifier);

    default T upsert(T model, I identifier) {
        return (T)this.getV1(identifier).orElseGet(() -> this.create(model, identifier));
    }

    default T upsert(T model, I identifier, S whereClause) {
        return (T)this.getV1(identifier, whereClause).orElseGet(() -> this.create(model, identifier));
    }

    default T get(@NotNull I identifier) {
        return (T)this.getV1(Objects.requireNonNull(identifier, "Identifier in the get method should not be null")).orElse((T)null);
    }

    Optional<T> getV1(I identifier);

    default T get(I identifier, S whereClause) {
        return (T)this.getV1(identifier, whereClause).orElse((T)null);
    }

    Optional<T> getV1(I identifier, S whereClause);

    List<T> list(I identifier);

    List<T> list(I identifier, S whereClause);

    int count(I identifier, S whereClause);

    default T update(T transientObject, I identifier) {
        return (T)this.updateV1(transientObject, identifier).orElse((T)null);
    }

    Optional<T> updateV1(T transientObject, I identifier);

    Optional<T> hotUpdate(T transientObject, I identifier, S whereClause);

    int delete(T persistentObject);

    default int deleteV1(Optional<T> persistentObject) {
        return this.delete(persistentObject.orElseThrow(() -> new DataloadersException("The requested object is not found in database")));
    }

    int delete(I identifier, S whereClause);

    default int findAndDelete(I identifier) {
        return this.deleteV1(this.getV1(identifier));
    }

    default <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues, String... delimitter) {
        return query.replace(replaceString, (CharSequence)inValues.stream().map(Object::toString).collect(Collectors.joining(delimitter.length > 0 ? delimitter[0] : ",")));
    }

    default String setInvalues(String query, String replaceString, Set<String> inValues) {
        if (inValues != null && !inValues.isEmpty()) {
            Stream var10002 = inValues.stream().map(Object::toString);
            return query.replace(replaceString, "'" + (String)var10002.collect(Collectors.joining("','")) + "'");
        } else {
            throw new DataloadersException(" List cannot be empty");
        }
    }

    public static enum postgreSqlConstant {
        INFINITY("infinity"),
        INTEGER("INTEGER"),
        TAGS("tags");

        private String constant;

        private postgreSqlConstant(final String constant) {
            this.constant = constant;
        }

        @Generated
        public String getConstant() {
            return this.constant;
        }

        @Generated
        public String toString() {
            String var10000 = this.name();
            return "GenericDao.postgreSqlConstant." + var10000 + "(constant=" + this.getConstant() + ")";
        }
    }
}
