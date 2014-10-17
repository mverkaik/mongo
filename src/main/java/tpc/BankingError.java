package tpc;

/**
 *
 */
public enum BankingError {

    DB_ERROR(0, "A database error occurred"),
    INSUFFICIENT_BALANCE(1, "Insufficient balance"),
    NON_EXISTING_ACCOUNT(2, "Account does not exist"),
    NON_EXISTING_TRANSACTION(3, "Transaction does not exist"),
    CLOSED_ACCOUNT(4, "Closed account");

    private final int code;
    private final String message;

    private BankingError(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code + ": " + message;
    }
}
