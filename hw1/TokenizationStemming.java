package hw1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TokenizationStemming {

	static TreeMap<String, Integer> tokenMap = new TreeMap<String, Integer>();
	static TreeMap<String, Integer> stemMap = new TreeMap<String, Integer>();
	static int totalTokens = 0;
	
	public static void main(String[] args) throws IOException {
		long timeStart = System.currentTimeMillis();
		File dir = new File(args[0]);
		File[] files = dir.listFiles();
		int fileCount = 0;
		
		// generate tokens and stems for each files in folder
		for (File f : files) {
			if (f.isFile()) {
				fileCount++;
				tokenizeFile(f);
			}
		}

		// Part1
		System.out.println("\n<======================== Tokenization output ========================>");
		System.out.println("\nTotal number of tokens = " + totalTokens);
		System.out.println("Total number of unique tokens = " + tokenMap.size());
		System.out.println("Total number of token that occur only once = " + countSingle(tokenMap));
		System.out.println("Average number of token per document = " + totalTokens / fileCount);
		System.out.println("30 most frequent tokens and it's frquency : ");
		printMostFrequent(tokenMap, 30);

		// Part2
		System.out.println("\n\n<======================== Stemming output ========================>");
		System.out.println("\nTotal number of unique stems = " + stemMap.size());
		System.out.println("Total number of stem that occur only once = " + countSingle(stemMap));
		System.out.println("Average number of stem per document = " + totalTokens / fileCount);
		System.out.println("30 most frequent stem and it's frquency : ");
		printMostFrequent(stemMap, 30);

		System.out.println("\nTime taken to acquire characteristics: " + (System.currentTimeMillis() - timeStart) + "ms");

	}
	
	// printing top 30 tokens/stems
	public static void printMostFrequent(TreeMap<String, Integer> tMap, int n) {
		TreeMap<String, Integer> sortedMap = sortByValueDecreasing(tMap);
		Iterator<Entry<String, Integer>> it = sortedMap.entrySet().iterator();
		for (int i = 0; i < n; i++) {
			Entry<String, Integer> me = it.next();
			System.out.println(me.getKey() + " " + me.getValue());
		}

	}

	// counting token/stem that occurred only once
	private static int countSingle(TreeMap<String, Integer> tMap) {
		int onceCount = 0;
		Iterator<Entry<String, Integer>> it = tMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> me = it.next();
			if ((int) me.getValue() == 1)
				onceCount++;

		}

		return onceCount;
	}

	private static void tokenizeFile(File f) throws IOException {
		BufferedReader inputStream = new BufferedReader(new FileReader(f));
		String line;
		String[] tokenizeLine;
		
		while ((line = inputStream.readLine()) != null) {
			// check line doesn't contains SGML tags
			if (!(line.length() >= 3 && line.charAt(0) == '<' && line.charAt(line.length() - 1) == '>')) {
				//generate tokens by splitting line by space
				tokenizeLine = line.split(" ");
				for (String token : tokenizeLine) {
					// convert all words to lower case
					token = token.toLowerCase();
					// remove all unnecessary punctuation like hyphens, dot, apostrophe 
					token = token.replaceAll("[^a-zA-Z]", "");
					if (!token.equals("") && !token.equals(" ")) {
						totalTokens++;
						if (tokenMap.containsKey(token))
							tokenMap.put(token, tokenMap.get(token) + 1);
						else
							tokenMap.put(token, 1);

						// generate stems
						Stemmer stemmer = new Stemmer();
						char stemChar[] = token.toCharArray();
						stemmer.add(stemChar, stemChar.length);
						stemmer.stem();

						if (stemMap.containsKey(stemmer.toString()))
							stemMap.put(stemmer.toString(), stemMap.get(stemmer.toString()) + 1);
						else
							stemMap.put(stemmer.toString(), 1);
					}

				}
			}
		}
	}

	// sort in decreasing order of value
	public static TreeMap<String, Integer> sortByValueDecreasing(final TreeMap<String, Integer> tokenMap) {
		Comparator<String> valueComparator = new Comparator<String>() {
			public int compare(String s1, String s2) {
				int compare = tokenMap.get(s2).compareTo(tokenMap.get(s1));
				if (compare == 0)
					return 1;
				else
					return compare;
			}
		};

		TreeMap<String, Integer> decreasingValuesMap = new TreeMap<String, Integer>(valueComparator);
		decreasingValuesMap.putAll(tokenMap);
		return decreasingValuesMap;
	}
}
