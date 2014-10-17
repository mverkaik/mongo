package tpc;

import org.junit.Test;
import org.junit.Before;

import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

/**
 * Tests to make sure the bank works as expected
 */
public class BankUnitTest {

    private final static Logger LOG = Logger.getLogger(BankUnitTest.class.getName());

    private MongoBank mongoBank;

    /**
     * Create a new MongoBank instance to test on
     * @throws BankingException
     */
    public BankUnitTest() throws BankingException {
        mongoBank = MongoBank.getInstance();
    }

    /**
     * Drop all documents in the DB
     * @throws BankingException
     */
    @Before
    public void before() throws BankingException {
        mongoBank.reset();
    }

    /**
     * Test that account numbers are sequential and start with 1.
     * @throws BankingException
     */
    @Test
    public void accountCreationTest() throws BankingException {
        int accountNr = mongoBank.createAccount();
        assertEquals(1, accountNr);
        accountNr = mongoBank.createAccount();
        assertEquals(2, accountNr);
    }

    /**
     * Test account closing. Specifically that non-existing cannot be closed.
     * @throws BankingException
     */
    @Test
    public void accountCloseTest() throws BankingException {
        try {
            mongoBank.closeAccount(13); // This account does not exist
        } catch (BankingException e) {
            assertEquals(BankingError.NON_EXISTING_ACCOUNT.getCode(), e.getCode());
        }
        int acctNr = mongoBank.createAccount();
        boolean closed = mongoBank.isClosed(acctNr);
        assertEquals(false, closed);
        mongoBank.closeAccount(acctNr);
        closed = mongoBank.isClosed(acctNr);
        assertEquals(true, closed);
    }

    /**
     * Test that a deposit is reflected in the returned balance
     * @throws BankingException
     */
    @Test
    public void depositTest() throws BankingException {
        int accountNr = mongoBank.createAccount();
        assertEquals(0f, mongoBank.getBalance(accountNr), 0f);
        assertEquals(50.23f, mongoBank.deposit(accountNr, 50.23f), 0f);
    }

    /**
     * Test that a withdrawal is reflected in the returned balance
     * @throws BankingException
     */
    @Test
    public void withdrawTest() throws BankingException {
        int accountNr = mongoBank.createAccount();
        float balance = mongoBank.deposit(accountNr, 123.50f);
        balance = mongoBank.withdraw(accountNr, 23.50f);
        assertEquals(100f, balance, 0f);
    }

    /**
     * Test that a transfer succeeds and is reflected in the balances of source and destination accounts
     * @throws BankingException
     */
    @Test
    public void transferTest() throws BankingException {
        int acctNr1 = mongoBank.createAccount();
        mongoBank.deposit(acctNr1, 100f);
        int acctNr2 = mongoBank.createAccount();
        mongoBank.transfer(acctNr1, acctNr2, 45.34f);
        assertEquals(54.66f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(45.34f, mongoBank.getBalance(acctNr2), 0f);
    }

    /**
     * Test to ensure that a failing transaction in pending state can recover
     * @throws BankingException
     */
    @Test
    public void pendingTransferTransactionRecoveryTest() throws BankingException {
        int acctNr1 = mongoBank.createAccount();
        mongoBank.deposit(acctNr1, 100f);
        int acctNr2 = mongoBank.createAccount();
        mongoBank.setAgeOfTransactionsRequiringRecovery(1000);
        try {
            mongoBank.transfer(acctNr1, acctNr2, 50f, TxnState.PENDING); // Make it fail in the pending state
        } catch (BankingException e) { /* Ignore */ }
        try {
            Thread.sleep(1005);
        } catch (InterruptedException e) { /* Ingore */ }
        mongoBank.recoverPendingTransactions();
        assertEquals(50f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(50f, mongoBank.getBalance(acctNr2), 0f);
    }

    @Test
    public void appliedTransferTransactionRecoveryTest() throws BankingException {
        int acctNr1 = mongoBank.createAccount();
        mongoBank.deposit(acctNr1, 100f);
        int acctNr2 = mongoBank.createAccount();
        mongoBank.setAgeOfTransactionsRequiringRecovery(1000);
        try {
            mongoBank.transfer(acctNr1, acctNr2, 50f, TxnState.APPLIED); // Make it fail in the applied state
        } catch (BankingException e) { /* Ignore */ }
        sleep(mongoBank.getAgeOfTransactionsRequiringRecovery() + 5);
        mongoBank.recoverAppliedTransactions();
        assertEquals(50f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(50f, mongoBank.getBalance(acctNr2), 0f);
    }

    /**
     * After the “Update transaction state to applied.” step, you should not roll back the transaction.
     * Instead, complete that transaction and create a new transaction to reverse the transaction
     * by switching the values in the source and the destination fields.
     * @throws BankingException
     */
    @Test
    public void appliedTransferTransactionRollbackTest() throws BankingException {
        int acctNr1 = mongoBank.createAccount();
        int acctNr2 = mongoBank.createAccount();
        mongoBank.deposit(acctNr1, 100f);
        try {
            mongoBank.transfer(acctNr1, acctNr2, 50f, TxnState.APPLIED); // Fail in the 'applied' state
        } catch (BankingException e) {
            // ignore
        }
        sleep(mongoBank.getAgeOfTransactionsRequiringRecovery() + 5);
        mongoBank.recoverAppliedTransactions();
        assertEquals(50f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(50f, mongoBank.getBalance(acctNr2), 0f);
        // Do the transaction in reverse to recover
        mongoBank.transfer(acctNr2, acctNr1, 50f);
        assertEquals(100f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(0f, mongoBank.getBalance(acctNr2), 0f);
    }

    @Test
    public void pendingTransferRollbackTest() throws BankingException {
        int acctNr1 = mongoBank.createAccount();
        int acctNr2 = mongoBank.createAccount();
        mongoBank.deposit(acctNr1, 100f);
        try {
            mongoBank.transfer(acctNr1, acctNr2, 50f, TxnState.PENDING); // Fail in the 'pending' state
        } catch (BankingException e) {
            // ignore
        }
        sleep(mongoBank.getAgeOfTransactionsRequiringRecovery() + 5);
        mongoBank.cancelPendingTransactions();
        assertEquals(100f, mongoBank.getBalance(acctNr1), 0f);
        assertEquals(0f, mongoBank.getBalance(acctNr2), 0f);
    }

    /**
     * Take some sleep
     * @param timeInMs
     */
    private void sleep(long timeInMs) {
        try {
            LOG.info(String.format("Taking a nap for %s ms", timeInMs));
            Thread.sleep(timeInMs);
        } catch (InterruptedException e) { /* Ingore */ }
    }

}
