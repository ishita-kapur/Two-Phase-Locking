import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TwoPhaseWoundWait {
	
	static Map<Integer, TransactionDetails> transactionMap = new HashMap<>(); //Hash Map keeping track of transactions - Transaction Table
	static Map<String, ItemDetails> lockTable = new HashMap<>(); //Hash Map keeping track of locks - Lock Table
	static int transactionTimestamp = 0;
	static Boolean finalTransaction = false;
	static Boolean finalTransactionLockSuccess = false;
	static String separate = ":";
	static String opt="";
	
	public static void main(String args[]) {
		//to execute another file change fileName here
		String fileName = "finalinput4.txt"; //reading the contents of a file to execute a schedule
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			String fileLine;
            String itemName = ""; //keeps track of the data item being processed
            String operation = ""; //keeps track of operation that has to be performed
            int tId = 0; //initializing the transaction Id
            while((fileLine = in.readLine()) != null) {     	
            	if(fileLine.isEmpty())
            		continue;            	
            	String operations[] = fileLine.split("[0-9;]+"); //filters the operation from the line
            	String ids[] = fileLine.split("[ecbwr()A-Z;]"); //filters the transaction id from the line
            	String itemNames[]= fileLine.split("[();]"); //separates the operation and transaction id from the data item
            	if (itemNames.length > 1)
                	itemName = itemNames[1].trim();
                else
                	itemName = "";          	
            	try{
                	operation = operations[0].trim();
                    tId = Integer.parseInt(ids[1].trim());
                }
            	catch(Exception e) {
                	System.out.println("EXCEPTION : Unable to read line :" + fileLine);
            	}
            	System.out.println("\n\tOperation : " + operation + ", tId : " + tId + ", itemName: " + itemName);
            	System.out.println("\t------------------------------------\n");
            	completeOperation(operation, tId, itemName); //call the completeOperation function to execute operations
            }
            commitFinally(); //after the schedule has been executed, commit finally
		}
		catch(FileNotFoundException ex) {
            System.out.println("EXCEPTION : Unable to open file '" + fileName + "'");
        }
        catch(Exception e){
            e.printStackTrace();
        }
	}

	private static void commitFinally() throws InterruptedException {
		
		Boolean completed = false;
		Boolean completedTransactions[] = new Boolean[transactionMap.keySet().size()]; //array to check for completeness of the transactions in the schedule
		finalTransaction = true;
		for(int i = 0; i < completedTransactions.length; i++)
			completedTransactions[i] = false;
		while(!completed) { //check if all transactions have been completed
			for(int tId : transactionMap.keySet()) {
				TransactionDetails transaction = transactionMap.get(tId);
				if(transaction.state.contains("Active") | transaction.state.contains("Blocked")) { //if not complete the operations that are in the locked items list
					for(int i = 0; i < transaction.lockedItems.size(); i++) {
						String operation = transaction.lockedItems.get(i);
						String opItem[] = operation.split(separate);
						if(!opItem[0].isEmpty()) {
							String itemName = "";
							if(opItem.length > 1)
								itemName = opItem[1];
							if(opItem[0] == "b")
								opt = "BEGIN";
							if(opItem[0] == "r")
								opt = "READ";
							if(opItem[0] == "w")
								opt = "WRITE";
							if(opItem[0] == "e")
								opt = "END";
							if(opItem[0] == "c")
								opt = "COMMIT";
							if(itemName.equals(""))
								System.out.println("Transaction T" + tId + " performs operation " + opt + ".");
							else
								System.out.println("Transaction T" + tId + " performs operation " + opt + " on " + itemName + ".");
							
    						finalTransactionLockSuccess = false;
    						completeOperation(opItem[0], tId, itemName);
    						if(!itemName.isEmpty() && finalTransactionLockSuccess) {
    							transaction.lockedItems.set(i, separate+itemName);
    							transactionMap.put(tId, transaction);
    						}
						}
					}
				}
				else
					completedTransactions[tId-1] = true;
			}
			for(int i = 0; i < completedTransactions.length; i++) {
				if(completedTransactions[i])
					completed = true;
				else
					completed = false;
			}
		}
	}

	private static void completeOperation(String operation, int tId, String itemName) throws InterruptedException {
		
		switch(operation) {
		
		case "b": //case for begin transaction operation
			//Begin
			System.out.println("Begin Operation");
			beginTransaction(tId); //call the beginTransaction function
			TimeUnit.SECONDS.sleep(1); //sleep to monitor the output while execution continues
			break;
			
		case "r": //case for read item operation
			//Read
			System.out.println("Read Operation");
			readItem(tId, itemName); //call the readItem function
			TimeUnit.SECONDS.sleep(1);
			break;
		
		case "w":
			//Write
			System.out.println("Write Operation");
			writeItem(tId, itemName); //call the writeItem function
			TimeUnit.SECONDS.sleep(1);
			break;
		
		case "e":
			//End
			System.out.println("End Operation"); //ending a transaction will lead to committing the transaction
		case "c":
			//Commit
			System.out.println("Commit Operation");
			commitTransaction(tId); //call the commitTransaction function to commit the transaction
			TimeUnit.SECONDS.sleep(1);
			break;
		
		default:
			String output = "'" + operation + "' is not a valid operation.";
    		System.out.println(output);
		}
		
	}

	private static void commitTransaction(int tId) throws InterruptedException {
		
		if(!transactionMap.containsKey(tId)) { //check if transaction is not already begun, cannot commit
			System.out.println("ERROR : Transaction T" + tId + " has not yet begun.");
			return;
		}
		TransactionDetails transaction = transactionMap.get(tId); //get the transaction details from the transaction table
		switch(transaction.state) {
		
		case "Active": //if the transaction is in Active state
			for(int i = 0; i < transaction.lockedItems.size(); i++) {
				String operation = transaction.lockedItems.get(i);
				String opItem[] = operation.split(separate);
				if(!opItem[0].isEmpty()) {
					if(opItem.length > 1) {
						Boolean flag = finalTransaction;
						Boolean transactionFlagLockSuccess = finalTransactionLockSuccess;
						finalTransactionLockSuccess = false;
						finalTransaction = true;
						completeOperation(opItem[0], tId, opItem[1]); //complete any operations that have been waiting in the transactions locked list
						if(finalTransactionLockSuccess) {
							transaction.lockedItems.set(i, separate + opItem[1]);
							transactionMap.put(tId, transaction);
						}	        			
						finalTransactionLockSuccess = transactionFlagLockSuccess;
						finalTransaction = flag;
					}
				}
			}
			if(transaction.state.contains("Blocked") | transaction.state.contains("Aborted")) { //if the transaction is in Blocked or Aborted state, nothing has to be done
				return;
			}
			for(String op : transaction.lockedItems) {
				String opItem[] = op.split(separate);
				if (opItem.length > 1)
        			releaseLock(tId, opItem[1]); //Locks need to be released prior to commit the transaction
			}
			transaction.state = "Commited";
			System.out.println("Transaction T" + tId + " committed.");
			break;
		
		case "Blocked": //if the transaction is in Blocked state, transaction cannot be committed
			System.out.println("Commit not possible. Transaction T" + tId + " is blocked");
			if(!finalTransaction)
				transaction.lockedItems.add("c" + separate);
			break;
		
		case "Aborted": //if transaction is in Aborted state, nothing has to be done
			System.out.println("Transaction T" + tId + " is aborted.");
			break;
		
		case "Committed": //if transaction is already in Committted state, nothing has to be done
			System.out.println("Transaction T" + tId + " is already committed.");
			break;
		}
	}

	private static void releaseLock(int tId, String itemName) {
		
		if(!lockTable.containsKey(itemName)) { //check if data item is present in the lock table, if not present nothing has to be done
			return;
		}
		ItemDetails itemDetails = lockTable.get(itemName); //otherwise, get itemDetails if data item is present in the lock table
		if(!itemDetails.txsHolding.contains(tId)) { //if transaction id is not present in the holding transactions list of the data item, nothing has to be done
			return;
		}
		switch(itemDetails.lockState) {
		case "Read Lock": //if the data item is read locked
			for(int i = 0; i < itemDetails.txsHolding.size(); i++) {
				if(itemDetails.txsHolding.get(i) == tId) { //if the transaction id is present in the holding transactions list
					itemDetails.txsHolding.remove(i); //delete the transaction from the holding transactions list
					System.out.println("Transaction T" + tId + " released 'Read Lock' on data item " + itemName);
					break;
				}
			}
			if(itemDetails.txsHolding.isEmpty()) {
				for(int i = 0; i < itemDetails.txsWaiting.size(); i++) {
					int waitTID = itemDetails.txsWaiting.get(i);
					itemDetails.txsWaiting.remove(i); //delete the transactions from the waiting transactions list
					String lockState = nextOperation(waitTID, itemName);
					if(!lockState.isEmpty()) {
						System.out.println("Transaction T" + waitTID + " " + lockState + "s data item " + itemName);
						itemDetails.txsHolding.add(waitTID);
						itemDetails.lockState = lockState;
						break;
					}
				}
			}
			break;
		case "Write Lock":
			itemDetails.txsHolding.clear();
			System.out.println("Transation T" + tId + " released 'Write Lock' on data item " + itemName);
			for(int i = 0; i < itemDetails.txsWaiting.size(); i++) {
				int waitTID = itemDetails.txsWaiting.get(i);
				itemDetails.txsWaiting.remove(i);
				String lockState = nextOperation(waitTID, itemName); //if waiting transaction is blocked, make it active after it is removed from the waiting transactions list
				if(!lockState.isEmpty()) {
					System.out.println("Transaction T" + waitTID + " " + lockState + "s data item " + itemName);
					itemDetails.txsHolding.add(waitTID);
					itemDetails.lockState = lockState;
					break;
				}
			}
			break;
		}
		if(itemDetails.txsHolding.isEmpty() && itemDetails.txsWaiting.isEmpty()) {
			lockTable.remove(itemName);
		}
		else {
			lockTable.put(itemName, itemDetails);
		}
	}

	private static String nextOperation(int tId, String itemName) {
		
		String lockState = "";
		TransactionDetails transaction = transactionMap.get(tId);
		switch(transaction.state) {
		case "Blocked": //changed state to active for a blocked transaction
			System.out.println("Transaction T" + tId + "'s state changed.");
			System.out.print("State changed from " + transaction.state + " to ");
			transaction.state = "Active";
			System.out.println(transaction.state);
			for(int i = 0; i < transaction.lockedItems.size(); i++) {
				String operation = transaction.lockedItems.get(i);
				String opItem[] = operation.split(separate);
				if(!opItem[0].isEmpty() && opItem[1].equals(itemName)) {
					if(opItem[0].equals("r")) {
						lockState = "Read Lock";
					}
					else {
						lockState = "Write Lock";
					}
					transaction.lockedItems.set(i, separate + itemName); //after transaction has again been resumed, add items to locked items list for waiting operations
					break;
				}
			}
			break;
		case "Aborted": //if transaction has been aborted, just remove the transaction id from waiting transactions list
			System.out.println("Transaction T" + tId + " is aborted.");
			System.out.println("Transaction T" + tId + " being removed from data item " + itemName + "'s waiting list");
			break;
		}
		transactionMap.put(tId, transaction);
		return lockState;
	}

	private static void writeItem(int tId, String itemName) {
		
		if(!transactionMap.containsKey(tId)) { //check if transaction has not yet begun
			System.out.println("ERROR : Transaction T" + tId + " has not yet begun.");
			return;
		}
		TransactionDetails transaction = transactionMap.get(tId); //get the transaction details from the transaction table
		String lockState = "Write Lock"; //since write operation has to be performed, lockstate needs to be write lock (which is an exclusive lock)
		
		switch(transaction.state) {
		
		case "Active": //if transaction is in Active state
			verifyLockTable(tId, itemName, lockState); //verify from the lock table
			break;
		
		case "Blocked": //if transaction is in Blocked state
			if(!finalTransaction) {
				System.out.println("Transaction T" + tId + " is blocked.");
				System.out.println("Operation w:" + itemName + " is added to the locked items list.");
				transaction.lockedItems.add("w" + separate + itemName); //insert the operation that needs to be perfomed in the locked items list of the transaction
				transactionMap.put(tId, transaction);
			}
			break;
		
		case "Aborted": //if transaction is in Aborted state, cannot write without begin
			System.out.println("Cannot perform Write without begining again. Transaction T" + tId + " is aborted.");
			break;
		
		case "Committed": //if transaction is in Committed state, cannot write without begin
			System.out.println("Cannot perform Write without begining again. Transaction T" + tId + " is already committed.");
			break;
		}
	}

	private static void readItem(int tId, String itemName) {
		
		if(!transactionMap.containsKey(tId)) { //check if transaction has not yet begun
			System.out.println("ERROR : Transaction T" + tId + " has not yet begun.");
			return;
		}
		TransactionDetails transaction = transactionMap.get(tId); //get the transaction details from the transaction table
		String lockState = "Read Lock"; //since read operation has to be performed, lockstate needs to be read lock (which is a shared lock)
		
		switch(transaction.state) {
		
		case "Active": //if transaction is in Active state
			verifyLockTable(tId, itemName, lockState); //verify from the lock table
			break;
		
		case "Blocked": //if transaction is in Blocked state
			if(!finalTransaction) {
				System.out.println("Transaction T" + tId + " is blocked.");
				System.out.println("Operation r:" + itemName + " is added to the locked items list.");
				transaction.lockedItems.add("r" + separate + itemName); //insert the operation that needs to be perfomed in the locked items list of the transaction
				transactionMap.put(tId, transaction);
			}
			break;
		
		case "Aborted": //if transaction is in Aborted state, cannot read without begin
			System.out.println("Cannot perform Read without begining again. Transaction T" + tId + " is aborted.");
			break;
		
		case "Committed": //if transaction is in Committed state, cannot read without begin
			System.out.println("Cannot perform Write without begining again. Transaction T" + tId + " is already committed.");
			break;
		}
		
	}

	private static void verifyLockTable(int tId, String itemName, String lockState) {
		
		ItemDetails itemDetails = new ItemDetails(itemName, lockState); //create an object of type itemDetails
		TransactionDetails transaction = transactionMap.get(tId); //get the transaction details from the transaction table
		if(!lockTable.containsKey(itemName)) { //if the data item is not resent in the lock table, not locked by any transaction
			itemDetails.txsHolding.add(tId); //insert the transaction id into the holding transactions list of the data item
			System.out.println("Transaction T" + tId + " is added to the Holding Transactions list for data item " + itemName);
			if(!finalTransaction) {
    			transaction.lockedItems.add(separate + itemName);
    			System.out.println("Operation " + separate + itemName + " is added to the locked items list for Transaction T" + tId);
    		}
    		else {
    			finalTransactionLockSuccess = true;
    		}
		}
		else {
			itemDetails = lockTable.get(itemName); //if data item is present in the lock table
			switch(lockState) {
			case "Read Lock": //check if read lock needs to be acquired on the data item
				if(itemDetails.lockState.equals("Read Lock")){ //check if read lock already exists on the data item
					itemDetails.txsHolding.add(tId); //insert the transaction id into the holding transactions list of the data item
					System.out.println("Transaction T" + tId + " is added to the Holding Transactions list for data item " + itemName);
					if(!finalTransaction) {
						transaction.lockedItems.add(separate + itemName);
						System.out.println("Operation " + separate + itemName + " is added to the locked items list for Transaction T" + tId);
					}
					else {
		    			finalTransactionLockSuccess = true;
		    		}
				}
				else{
					String requestedOperation = "r:" + itemName;
					if(woundWait(tId, requestedOperation, itemDetails.txsHolding)){ //check for two phase locking using wound-wait, holding transaction is wounded
						System.out.println("Lock Table updated for data item " + itemName);
						System.out.print("Lock Mode changed from " + itemDetails.lockState + " to ");
						itemDetails.lockState = "Read Lock"; //update the lock state on the data item
						System.out.println(itemDetails.lockState);
						itemDetails.txsHolding.clear(); //clear the holding transactions list as the lock state has been updated
						itemDetails.txsHolding.add(tId); //insert the transaction id into the holding transactions list of the data item
						System.out.println("Transaction T" + tId + " is added to the Holding Transactions list for data item " + itemName);
						if(!finalTransaction) {
							transaction.lockedItems.add(separate + itemName);
							System.out.println("Operation " + separate + itemName + " is added to the locked items list for Transaction T" + tId);
						}
						else
			    			finalTransactionLockSuccess = true;
					}
					else {
						itemDetails.txsWaiting.add(tId); //insert the transaction id into the waiting transactions list of the data item, younger transaction waits
						System.out.println("Transaction T" + tId + " is added to the Waiting Transactions list for data item " + itemName);
					}
				}
			break;
			case "Write Lock":
				String requestedOperation = "w:" + itemName;
				if(woundWait(tId, requestedOperation, itemDetails.txsHolding)){ //check for two phase locking using wound-wait, holding transaction is wounded
					System.out.println("Lock Table updated for data item " + itemName);
					System.out.print("Lock Mode changed from " + itemDetails.lockState + " to ");
					itemDetails.lockState = "Write Lock"; //update the lock state on the data item
					System.out.println(itemDetails.lockState);
					itemDetails.txsHolding.clear(); //clear the holding transactions list as the lock state has been updated
					itemDetails.txsHolding.add(tId); //insert the transaction id into the holding transactions list of the data item
					System.out.println("Transaction T" + tId + " is added to the Holding Transactions list for data item " + itemName);
					if(!finalTransaction) {
						transaction.lockedItems.add(separate + itemName);
						System.out.println("Operation " + separate + itemName + " is added to the locked items list for Transaction T" + tId);
					}
					else
		    			finalTransactionLockSuccess = true;
				}
				else {
					itemDetails.txsWaiting.add(tId); //insert the transaction id into the waiting transactions list of the data item, younger transaction waits
					System.out.println("Transaction T" + tId + " is added to the Waiting Transactions list for data item " + itemName);
				}
			}
		}
		lockTable.put(itemName, itemDetails); //update the details in the lock table
		transactionMap.put(tId, transaction); //update the details in the transaction table
	}

	private static boolean woundWait(int tId, String requestedOperation, List<Integer> txsHolding) {
		
		Boolean verify = true;
		TransactionDetails verifyTID = transactionMap.get(tId);
		for(Integer tx: txsHolding) {
			TransactionDetails holdTrans = transactionMap.get(tx);
			if(verifyTID.timestamp == holdTrans.timestamp) //since same transaction, lock state can be changed from read to write
				continue;
			if(verifyTID.timestamp < holdTrans.timestamp) { //requesting tx is older than holding tx, timestamp(req tx) < timestamp(hold tx), so holding tx is aborted(wounded)
				System.out.println("Transaction T" + tId + " (Requesting Tx), older than T" + holdTrans.tId + " (Holding Tx)");
				System.out.println("Transaction T" + tId + " wounds (aborts) T" + holdTrans.tId);
				System.out.println("Transaction T" + holdTrans.tId + "'s state changed.");
				System.out.print("State changed from " + holdTrans.state + " to ");
				holdTrans.state = "Aborted"; //wounded, hence transaction is aborted
				System.out.println(holdTrans.state);
				transactionMap.put(tId, holdTrans);
			}
			else { //requesting tx is younger than holding tx, timestamp(req tx) > timestamp(hold tx), so requesting tx waits
				if(verify) {
					verify = false;
					System.out.println("Transaction T" + verifyTID.tId + "'s state changed.");
					System.out.print("State changed from " + verifyTID.state + " to ");
					verifyTID.state = "Blocked"; //waiting, hence transaction is blocked
					System.out.println(verifyTID.state);
					if(!finalTransaction) {
						verifyTID.lockedItems.add(requestedOperation);
						System.out.println("Operation " + requestedOperation + " is added to the locked items list for Transaction T" + verifyTID.tId);
					}
					transactionMap.put(tId, verifyTID);
					System.out.println("Transaction T" + holdTrans.tId + " (Holding Tx), older than T" + tId + " (Requesting Tx)");
					System.out.println("Transaction T" + tId + " waits");
				}
			}
		}
		return verify;
	}

	private static void beginTransaction(int tId) {
		
		if(!transactionMap.containsKey(tId)) {//check if transaction is not already begun
			transactionTimestamp++; //increment timestamp for a new transaction
			TransactionDetails transaction = new TransactionDetails(tId, "Active", transactionTimestamp); //create an object of type TransactionDetails and start the transaction with status Active
			System.out.println("Transaction T" + tId + " started.");
			System.out.println("Transaction T" + tId + "'s state is " + transaction.state + " and timestamp is " + transaction.timestamp);
			transactionMap.put(tId, transaction);// insert the details into the transaction table
		}
		else {
			System.out.println("ERROR : Transaction T" + tId + " is already started.");
		}		
	}

}