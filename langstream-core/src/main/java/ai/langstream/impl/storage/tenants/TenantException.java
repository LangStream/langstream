package ai.langstream.impl.storage.tenants;

import lombok.Getter;

public class TenantException extends Exception {

    public enum Type {
        NotFound,
        AlreadyExists;
    }

    @Getter private final Type type;

    public TenantException(String message, Type type) {
        super(message);
        this.type = type;
    }
}
