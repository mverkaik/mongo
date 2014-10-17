package tpc;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.logging.Logger;

/**
 * This class represents a consumer bank that is implemented with MongoDB as the backend store.
 *
 */
public class MongoBank {

    private final static Logger LOG = Logger.getLogger(MongoBank.class.getName());

    /**
     * BSON field names
     */
    private final static String CLOSED = "closed", BALANCE = "balance", PENDING_TXNS = "pendingTransactions",
    ID = "_id", LAST_MOD = "lastModified", SRC = "source", DEST = "destination", AMOUNT = "value", STATE = "state",
    DB = "mongobank", ACCOUNTS = "accounts", TXNS = "transactions";

    /**
     * The one and only mongo bank
     */
    private static MongoBank mongoBank;

    /**
     * Mongo collections
     */
    private DBCollection accounts, transactions;

    /**
     * Age of transactions that are considered incomplete
     */
    private long ageOfTransactionsRequiringRecovery = 5000; // in ms

    /**
     * Create a new MongoBank
     * @throws BankingException In the unlikely case that localhost is not recognized
     */
    private MongoBank() throws BankingException {
        try {
            MongoClient mongoClient = new MongoClient();
            DB db = mongoClient.getDB(DB);
            accounts = db.getCollection(ACCOUNTS);
            accounts.setWriteConcern(WriteConcern.JOURNALED);
            transactions = db.getCollection(TXNS);
            transactions.setWriteConcern(WriteConcern.JOURNALED);
            LOG.info("Bank open for business");
        } catch (UnknownHostException e) {
            String msg = String.format("Bank failed to open: %s", e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Get a singleton MongoBank
     * @return
     * @throws BankingException
     */
    public static synchronized MongoBank getInstance() throws BankingException {
        if (mongoBank == null) mongoBank = new MongoBank();
        return mongoBank;
    }

    /**
     * Get the age of transactions that are considered incomplete
     * @return in ms
     */
    public long getAgeOfTransactionsRequiringRecovery() {
        return ageOfTransactionsRequiringRecovery;
    }

    /**
     * Set the age of transactions that are considered incomplete
     * @param ageInMs The age in milliseconds
     */
    public void setAgeOfTransactionsRequiringRecovery(long ageInMs) {
        this.ageOfTransactionsRequiringRecovery = ageInMs;
    }

    /**
     * Reset the bank to initial state, without any data
     * @throws BankingException
     */
    public void reset() throws BankingException {
        try {
            accounts.remove(new BasicDBObject());
            transactions.remove(new BasicDBObject());
        } catch (MongoException e) {
            LOG.severe(String.format("Failed while resetting: %s", e.getMessage()));
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Create a new account
     * @return The account number of the created account
     * @throws BankingException When a database error occurs
     */
    public synchronized int createAccount() throws BankingException {
        final int acctNr = getNewAccountNumber();
        BasicDBObject accountA = new BasicDBObject(ID, acctNr)
                .append(CLOSED, false)
                .append(BALANCE, new Double(0.0))
                .append(PENDING_TXNS, new String[]{});
        try {
            accounts.insert(accountA);
        } catch (MongoException e) {
            String msg = String.format("Failed to create new account: %s", e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
        LOG.info(String.format("Created account %s", acctNr));
        return acctNr;
    }

    /**
     * Close an account
     * @param acctNr The account number to close
     * @throws BankingException When a database error occurs
     */
    public void closeAccount(int acctNr) throws BankingException {
        DBObject account = findAccount(acctNr);
        boolean closed = (Boolean) account.get(CLOSED);
        if (closed) {
            LOG.warning(String.format("Account %s was already closed.", acctNr));
            return;
        }
        try {
            accounts.update(new BasicDBObject(ID, acctNr), new BasicDBObject("$set", new BasicDBObject(CLOSED, true)));
            LOG.info(String.format("Closed account %s", acctNr));
        } catch (MongoException e) {
            LOG.severe(String.format("Failed to close account %s", acctNr));
        }
    }

    /**
     * Get the balance of an account
     * @param acctNr The account number for which to get the balance
     * @return The account number
     * @throws BankingException When the account does not exist or a database error occurs
     */
    public float getBalance(int acctNr) throws BankingException {
        DBObject account = findAccount(acctNr);
        return new Float((Double) account.get(BALANCE));
    }

    /**
     * Check if an account is closed
     * @param acctNr
     * @return
     * @throws BankingException
     */
    public boolean isClosed(int acctNr) throws BankingException {
        DBObject account = findAccount(acctNr);
        return (boolean) account.get(CLOSED);
    }

    public float deposit(int acctNr, float amount) throws BankingException {
        try {
            accounts.update(new BasicDBObject(ID, acctNr),
                    new BasicDBObject("$inc", new BasicDBObject(BALANCE, new Float(amount))));
            LOG.info(String.format("Deposited $%.2f into account %s", amount, acctNr));
            return getBalance(acctNr);
        } catch (MongoException e) {
            String msg = String.format("Failed to deposit $%.2f into account %s: %s", amount, acctNr, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Withdraw money from an account
     * @param acctNr The account number to withdraw from
     * @param amount The amount to withdraw
     * @return The balance after the withdraw has taken place
     * @throws BankingException
     */
    public float withdraw(int acctNr, float amount) throws BankingException {
        DBObject account = findAccount(acctNr);
        boolean closed = (boolean) account.get(CLOSED);
        if (closed) {
            LOG.severe(String.format("Cannot withdraw $%.2f from account %s because it is closed.",
                    amount, acctNr));
            throw new BankingException(BankingError.CLOSED_ACCOUNT);
        }
        double balance = (Double) account.get(BALANCE);
        if (amount > balance) {
            String msg = String.format("Can't withdraw $%.2f from account %s because of insufficient balance $%.2f",
                    amount, acctNr, balance);
            LOG.severe(msg);
            throw new BankingException(BankingError.INSUFFICIENT_BALANCE);
        } else {
            try {
                accounts.update(new BasicDBObject(ID, new Integer(acctNr)),
                        new BasicDBObject("$inc", new BasicDBObject(BALANCE, -new Float(amount))));
            } catch (MongoException e) {
                String msg = String.format("Failed to withdraw $%.2f from account %s: %s", amount, acctNr, e.getMessage());
                LOG.severe(msg);
                throw new BankingException(BankingError.DB_ERROR);
            }
        }
        LOG.info(String.format("$%.2f was withdrawn from account %s", amount, acctNr));
        return getBalance(acctNr);
    }

    /**
     * Transfer money from one account to another
     * @param srcAcctNr The source of the transfer
     * @param destAcctNr The destination of the transfer
     * @param amount The amount to transfer from source to destination
     * @throws BankingException
     */
    public void transfer(int srcAcctNr, int destAcctNr, float amount) throws BankingException {
        transfer(srcAcctNr, destAcctNr, amount, null);
    }

    public void transfer(int srcAcctNr, int destAcctNr, float amount, String failState)
            throws BankingException {

        // Check that the balance of the source account is sufficient
        DBObject srcAccount = findAccount(srcAcctNr);
        double balance = (Double) srcAccount.get(BALANCE);
        if (amount > balance) {
            String msg = String.format("Balance $%.2f in account %s is insufficient to transfer $%.2f to account %s",
                    balance, srcAccount, amount, destAcctNr);
            LOG.severe(msg);
            throw new BankingException(BankingError.INSUFFICIENT_BALANCE);
        }

        // Start a transaction
        DBObject transaction = createTransaction(srcAcctNr, destAcctNr, amount);
        int txnID = (Integer) transaction.get(ID);

        // Find the transaction
        findTransaction(srcAcctNr, destAcctNr, TxnState.INITIAL);

        // Set the transaction state to 'pending'
        updateTransactionState(txnID, TxnState.INITIAL, TxnState.PENDING);

        // Apply the transaction to the source account
        applyPendingTransactionToAccount(txnID, srcAcctNr, -amount);

        // Conditionally fail in the 'pending' state
        if (failState != null && failState.equals(TxnState.PENDING)) {
            String msg = String.format("The transfer transaction %s failed in the 'pending' state", txnID);
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }

        // Apply the transaction to the destination account
        applyPendingTransactionToAccount(txnID, destAcctNr, amount);

        // Update the transaction state to 'applied'
        updateTransactionState(txnID, TxnState.PENDING, TxnState.APPLIED);

        // Remove the applied transaction ID from the source account
        removeAppliedTransactionFromAccount(txnID, srcAcctNr);

        // Conditionally fail in the 'pending' state
        if (failState != null && failState.equals(TxnState.APPLIED)) {
            String msg = String.format("The transfer transaction %s failed in the 'applied' state", txnID);
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }

        // Remove the applied transaction ID from the target account
        removeAppliedTransactionFromAccount(txnID, destAcctNr);

        // Update the transaction state to 'done'
        updateTransactionState(txnID, TxnState.APPLIED, TxnState.DONE);

        LOG.info(String.format("Transferred $%.2f from account %s to account %s", amount, srcAcctNr, destAcctNr));

    }

    /**
     * Find an account
     * @param acctNr The account number
     * @return A Mongo object representing the account
     * @throws BankingException If the account does not exist
     */
    private DBObject findAccount(int acctNr) throws BankingException {
        DBObject account;
        try {
            account = accounts.findOne(new BasicDBObject(ID, acctNr));
        } catch (MongoException e) {
            LOG.severe(String.format("Failed to lookup account %s: $s", acctNr, e.getMessage()));
            throw new BankingException(BankingError.DB_ERROR);
        }
        if (account == null) {
            String msg = String.format("Account %s does not exist", acctNr);
            LOG.severe(msg);
            throw new BankingException(BankingError.NON_EXISTING_ACCOUNT);
        }
        else {
            return account;
        }
    }

    private int getNewAccountNumber() throws BankingException {
        try {
            DBCursor cursor = accounts.find(new BasicDBObject(), new BasicDBObject(ID, 1))
                    .sort(new BasicDBObject(ID, -1))
                    .limit(1);
            if (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                return ((Integer) dbObject.get(ID)).intValue() + 1;
            }
        } catch (MongoException e) {
            throw new BankingException(BankingError.DB_ERROR);
        }
        return 1;
    }

    private int getNewTransactionNumber() throws BankingException {
        try {
            DBCursor cursor = transactions.find(new BasicDBObject(), new BasicDBObject(ID, 1))
                    .sort(new BasicDBObject(ID, -1))
                    .limit(1);
            if (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                return ((Integer) dbObject.get(ID)).intValue() + 1;
            }
        } catch (MongoException e) {
            throw new BankingException(BankingError.DB_ERROR);
        }
        return 1;
    }

    /**
     * Create a new transaction in a thread-safe way, to ensure unique transaction numbers in the store
     * @param srcAcctNr The source account number
     * @param destAcctNr The destination account number
     * @param amount The amount to transfer
     * @return A Mongo object
     * @throws BankingException When a Mongo exception occurs
     */
    private synchronized DBObject createTransaction(int srcAcctNr, int destAcctNr, double amount)
            throws BankingException {
        int txnId = getNewTransactionNumber();
        BasicDBObject transaction = new BasicDBObject(ID, txnId)
                .append(SRC, srcAcctNr)
                .append(DEST, destAcctNr)
                .append(AMOUNT, amount)
                .append(STATE, TxnState.INITIAL)
                .append(LAST_MOD, new Date());
        try {
            transactions.insert(transaction);
        } catch (MongoException e) {
            String msg = String.format("Failed to create a transaction to transfer $%.2f from account %s to account %s: %s",
                    amount, srcAcctNr, destAcctNr, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
        LOG.info(String.format("Created transaction %s to transfer $%.2f from account %s to account %s",
                txnId, amount, srcAcctNr, destAcctNr));
        return transaction;
    }

    /**
     * Find a transaction
     * @param srcAcctNr The source account number the transaction applies to
     * @param destAcctNr The destination account number the transaction applies to
     * @param state The state of the transaction
     * @return A Mongo object representing the transaction
     * @throws BankingException When a Mongo exception occurs or if a transaction for the given ID does not exist.
     */
    private DBObject findTransaction(int srcAcctNr, int destAcctNr, String state) throws BankingException {
        DBObject transaction = null;
        try {
            transaction = transactions.findOne(new BasicDBObject(SRC, new Integer(srcAcctNr))
                    .append(DEST, new Integer(destAcctNr))
                    .append(STATE, state));
        } catch(MongoException e) {
            String msg =
                    String.format("Failed to find a transaction with state '%s' for source account %s and destination account %s: $s",
                    state, srcAcctNr, destAcctNr, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
        if (transaction == null) {
            String msg =
                    String.format("No transaction exists with state '%s' for source account %s and destination account %s",
                            state, srcAcctNr, destAcctNr);
            LOG.severe(msg);
            throw new BankingException(BankingError.NON_EXISTING_TRANSACTION);
        }
        int txnID = (Integer) transaction.get(ID);
        LOG.info(String.format("Found transaction %s with state '%s' for source account %s and destination account %s",
                txnID, state, srcAcctNr, destAcctNr));
        return transaction;
    }

    /**
     * Update the state of a transaction
     * @param txnID Thd ID of the transaction to update
     * @param currentState The current state of the txn
     * @param newState The new state of the txn
     * @throws BankingException When a db error occurs
     */
    private void updateTransactionState(int txnID, String currentState, String newState) throws BankingException {
        try {
            transactions.update(new BasicDBObject(ID, txnID)
                            .append(STATE, currentState),
                    new BasicDBObject("$set", new BasicDBObject(STATE, newState))
                            .append("$currentDate", new BasicDBObject(LAST_MOD, true)));
            LOG.info(String.format("Changed the state of transaction %s from '%s' to '%s'", txnID, currentState, newState));
        } catch (MongoException e) {
            String msg = String.format("Failed to change the state of transaction %s from '%s' to '%s': %s",
                    txnID, currentState, newState, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Apply a pending transaction to an account
     * @param txnID The ID of the transaction to apply
     * @param acctNr The number of the account to apply the txn to
     * @param amount The amount to add
     * @throws BankingException When a db error occurs.
     */
    private void applyPendingTransactionToAccount(int txnID, int acctNr, float amount) throws BankingException {
        try {
            WriteResult result = accounts.update(new BasicDBObject(ID, new Integer(acctNr))
                            .append(CLOSED, false)
                            .append(PENDING_TXNS, new BasicDBObject("$ne", txnID)),
                    new BasicDBObject("$inc", new BasicDBObject(BALANCE, amount))
                            .append("$push", new BasicDBObject(PENDING_TXNS, txnID)));
            switch(result.getN()) {
                case 1:
                    LOG.info(String.format("Applied transaction %s for amount $%.2f to account %s",
                            txnID, amount, acctNr));
                    break;
                case 0:
                    LOG.info(String.format(
                            "Did not apply transaction %s for amount $%.2f to account %s",
                            txnID, amount, acctNr));
            }
        } catch (MongoException e) {
            String msg = String.format("Failed to apply transaction %s for amount $%.2f to account %s: %s",
                    txnID, amount, acctNr, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Remove an applied transaction from an account
     * @param txnID The ID of the applied transaction
     * @param acctNr The number of the account to which the transaction was applied
     * @throws BankingException When a database error occurs
     */
    private void removeAppliedTransactionFromAccount(int txnID, int acctNr) throws BankingException {
        try {
            WriteResult result = accounts.update(new BasicDBObject(ID, new Integer(acctNr))
                            .append(PENDING_TXNS, txnID),
                    new BasicDBObject("$pull", new BasicDBObject(PENDING_TXNS, txnID)));
            switch(result.getN()) {
                case 1:
                    LOG.info(String.format("Removed applied transaction %s from account %s", txnID, acctNr));
                    break;
                case 0:
                    LOG.info(String.format(
                            "Did not remove applied transaction %s from account %s because it did not contain it",
                            txnID, acctNr));
            }

        } catch (MongoException e) {
            String msg = String.format("Failed to remove transaction %s from account %s: %s",
                    txnID, acctNr, e.getMessage());
            LOG.severe(msg);
            throw new
                    BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Recover transactions in the 'pending' state that are older than ageOfTransactionsRequiringRecovery.
     * It first applies the pending transaction to the source and destination accounts. Then it marks the txn
     * as 'applied'. Then it removes the applied txn ID from the source and destination accounts. And finally
     * it marks the txn as 'done'
     * @throws BankingException When a database error occurs
     */
    public void recoverPendingTransactions() throws BankingException {
        int txnID = -1, srcAcctNr, destAcctNr;
        float amount;
        Date dateThreshold = new Date();
        dateThreshold.setTime(dateThreshold.getTime() - ageOfTransactionsRequiringRecovery);
        try {
            DBCursor cursor = transactions.find(new BasicDBObject(STATE, TxnState.PENDING)
            .append(LAST_MOD, new BasicDBObject("$lt", dateThreshold)));
            while (cursor.hasNext()) {
                DBObject txn = cursor.next();
                txnID = (Integer) txn.get(ID);
                srcAcctNr = (Integer) txn.get(SRC);
                destAcctNr = (Integer) txn.get(DEST);
                amount = new Float((Double) txn.get(AMOUNT));
                LOG.info(String.format("About to recover pending transaction %s", txnID));
                applyPendingTransactionToAccount(txnID, srcAcctNr, -amount);
                applyPendingTransactionToAccount(txnID, destAcctNr, amount);
                updateTransactionState(txnID, TxnState.PENDING, TxnState.APPLIED);
                removeAppliedTransactionFromAccount(txnID, srcAcctNr);
                removeAppliedTransactionFromAccount(txnID, destAcctNr);
                updateTransactionState(txnID, TxnState.APPLIED, TxnState.DONE);
                LOG.info(String.format("Recovered pending transaction %s", txnID));
            }
        } catch (MongoException e) {
            String msg = (txnID == -1) ?
                    "Failed while recovering pending transactions" :
                    String.format("Failed to recover pending transaction %s: %s", txnID, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
    }

    /**
     * Cancel transactions in the 'pending' state.
     * @throws BankingException When a database error occurs.
     */
    public void cancelPendingTransactions() throws BankingException {
        LOG.info("Start cancelling pending transactions");
        Date dateThreshold = new Date();
        dateThreshold.setTime(dateThreshold.getTime() - ageOfTransactionsRequiringRecovery);
        try {
            // Set the txn state to 'cancelling'
            transactions.update(new BasicDBObject(STATE, TxnState.PENDING)
                    .append(LAST_MOD, new BasicDBObject("$lt", dateThreshold)),
                    new BasicDBObject("$set", new BasicDBObject(STATE, TxnState.CANCELING))
                    .append("$currentDate", new BasicDBObject(LAST_MOD, true)));
            // Undo the txn on both accounts
            DBCursor cursor = transactions.find(new BasicDBObject(STATE, TxnState.CANCELING));
            while (cursor.hasNext()) {
                // Update the destination account, subtracting from its balance the transaction value
                // and removing the transaction _id from the pendingTransactions array.
                DBObject txn = cursor.next();
                int txnID = (Integer) txn.get(ID);
                int srcAcctNr = (Integer) txn.get(SRC);
                int destAcctNr = (Integer) txn.get(DEST);
                double amount = (Double) txn.get(AMOUNT);
                WriteResult result = accounts.update(new BasicDBObject(ID, destAcctNr).append(PENDING_TXNS, txnID),
                        new BasicDBObject("$inc", new BasicDBObject(BALANCE, -amount))
                                .append("$pull", new BasicDBObject(PENDING_TXNS, txnID)));
                if (result.getN() == 1) {
                    LOG.info(String.format("Updated destination account %s by depositing -$%.2f and removing txn %s",
                            destAcctNr, amount, txnID));
                }
                // Update the source account, adding to its balance the transaction value and removing the
                // transaction _id from the pendingTransactions array.
                result = accounts.update(new BasicDBObject(ID, srcAcctNr).append(PENDING_TXNS, txnID),
                        new BasicDBObject("$inc", new BasicDBObject(BALANCE, amount))
                                .append("$pull", new BasicDBObject(PENDING_TXNS, txnID)));
                if (result.getN() == 1) {
                    LOG.info(String.format("Updated source account %s by depositing -$%.2f and removing txn %s",
                            srcAcctNr, amount, txnID));
                }
                // To finish the rollback, update the transaction state from canceling to cancelled.
                result = transactions.update(new BasicDBObject(ID, txnID).append(STATE, TxnState.CANCELING),
                        new BasicDBObject("$set", new BasicDBObject(STATE, TxnState.CANCELED))
                                .append("$currentDate", new BasicDBObject(LAST_MOD, true)));
                if (result.getN() == 1) {
                    LOG.info(String.format("Updated transaction %s to state 'cancelled'",
                            srcAcctNr, amount, txnID));
                }
            }
        } catch (MongoException e) {
            LOG.severe(String.format("Failed while cancelling pending transactions: %s", e.getMessage()));
            throw new BankingException(BankingError.DB_ERROR);
        } finally {
            LOG.info("Finish cancelling pending transactions");
        }
    }

    /**
     * Recover transactions in the 'applied' state that are older than ageOfTransactionsRequiringRecovery.
     * It first removes the txn ID from the accounts, then marks the txn as 'done'
     * @throws BankingException
     */
    public void recoverAppliedTransactions() throws BankingException {
        LOG.info("Start recovering applied transactions");
        int txnID = -1, srcAcctNr, destAcctNr;
        Date dateThreshold = new Date();
        dateThreshold.setTime(dateThreshold.getTime() - ageOfTransactionsRequiringRecovery);
        try {
            DBCursor cursor = transactions.find(new BasicDBObject(STATE, TxnState.APPLIED)
                    .append(LAST_MOD, new BasicDBObject("$lt", dateThreshold)));
            while (cursor.hasNext()) {
                DBObject txn = cursor.next();
                txnID = (Integer) txn.get(ID);
                srcAcctNr = (Integer) txn.get(SRC);
                destAcctNr = (Integer) txn.get(DEST);
                LOG.info(String.format("About to recover applied transaction %s", txnID));
                removeAppliedTransactionFromAccount(txnID, srcAcctNr);
                removeAppliedTransactionFromAccount(txnID, destAcctNr);
                updateTransactionState(txnID, TxnState.APPLIED, TxnState.DONE);
                LOG.info(String.format("Recovered applied transaction %s", txnID));
            }
        } catch (MongoException e) {
            String msg = (txnID == -1) ?
                    "Failed while recovering pending transactions" :
                    String.format("Failed to recover pending transaction %s: %s", txnID, e.getMessage());
            LOG.severe(msg);
            throw new BankingException(BankingError.DB_ERROR);
        }
        LOG.info("Finished recovering applied transactions");
    }

}
