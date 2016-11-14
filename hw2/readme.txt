Instructions for running code. 

I’m using CoreNLP 3.5.0 for lemmatization.

You can run my code in following 2 ways.

1) Using runscript.sh:

Just execute ./runscript.sh in terminal and it will compile and run the code in csgrads1 machine. 
I have already set the absolute path for Canfield folder(/people/cs/s/sanda/cs6322/Cranfield) and stopwrods file is already included in submission

In case you encounter error for permission just execute following command:
chmod -R 777 runscript.sh

**—> When you run code it will ask if you want to change location of output folder which you can change by giving absolute path for your desired location and if you don’t wish to change it then just press ’n’ and it will store all indexes in output folder that is already there.




2) Manually compile and run code on csgrads1:

Set classpath for Stanford's CoreNLP 3.5.0 which will load all jar files.
	source /usr/local/corenlp350/classpath.sh
Compile: javac *.java
Run: java IndexBuilding /people/cs/s/sanda/cs6322/Cranfield stopwords
here, args[0]: cranfield folder path,  args[1]: stopwords file path


output folder contains 4 indexes.
output.txt contains result statistics. 

Thank you!!