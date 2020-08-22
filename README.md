## Two-Phase-Locking

### Description

Simulation of Two-Phase Locking (2PL) Protocol for concurrency control. Implementation includes the rigorous 2PL, with the wound-wait method for dealing with deadlock.

### Steps to Execute

1.  Download the files in one folder
2.  Launch Command Prompt and navigate to the directory containing the project files.
3.  Set path of the Java bin in that directory using the command: `SET PATH=<path-of-java-bin>`
4.  Compile the *‘TwoPhaseWoundWait.java’* file using the command: `javac TwoPhaseWoundWait.java`
5.  Execute the *TwoPhaseWoundWait* java class using the following command: <br />
        i.  To view the output on the command prompt window:
            `java TwoPhaseWoundWait` <br />
        ii. To store the output into a text file and view it from any text editor:
            `java TwoPhaseWoundWait > <output-file-name.txt>` <br />
      e.g.: java TwoPhaseWoundWait > output1.txt
6.  To execute another input file, the value of fileName variable needs to be changed in the *‘TwoPhaseWoundWait.java’* source code file. <br />
      e.g.: String fileName = "finalinput1.txt"; (Line 21)
    
    After changing the file name the code needs to be **compiled again** so that the changes are reflected in the compiled code.



#### Note

i.  Please make sure input files are in the same folder as the source code. <br />
ii. Please make sure the same version of java is being used to compile as well as execute the source codes. <br />
iii.  Output files are saved in the same folder as the source code and the input files if Step 5.ii is used to save the output to a text file. <br />
