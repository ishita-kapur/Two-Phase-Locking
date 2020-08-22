import java.util.ArrayList;
import java.util.List;

public class ItemDetails {
	
    String itemName; //Data item
    String lockState; //Lock state of the data item
    List<Integer> txsHolding; //List of transactions holding lock on the data item
    List<Integer> txsWaiting; //List of transaction waiting for the data item
    
    public ItemDetails(String newItemName, String newLockState) { //constructor for the class ItemDetails
        this.itemName = newItemName;
        this.lockState = newLockState;
        txsHolding = new ArrayList<>();
        txsWaiting = new ArrayList<>();
    }
}