import java.util.ArrayList;
import java.util.List;

public class TransactionDetails {
	
    int tId; //Transaction Id
    String state; //Transaction state : Active, Blocked, Aborted, or Committed
    int timestamp; //Timestamp of transactions starting from 1
    List<String> lockedItems; //List of locked or waiting operations in the transaction
    
    public TransactionDetails(int newTId, String newTransactionState, int timestamp){ //constructor for the class TransactionDetails
    	tId = newTId;
        state = newTransactionState;
        this.timestamp = timestamp;
        lockedItems = new ArrayList<>();
    }
}