package tpc;

/**
 *
 */
public class BankingException extends Exception {

    private BankingError error;

    public BankingException(BankingError error) {
        this.error = error;
    }

    public int getCode() {
        return error.getCode();
    }

    @Override
    public String getMessage() {
        return error.getMessage();
    }
}
