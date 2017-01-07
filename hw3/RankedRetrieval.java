package hw3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class RankedRetrieval {
	static TreeMap<String, TermNode> tokenMap = new TreeMap<String, TermNode>();
	static ArrayList<String> stopWordsList = new ArrayList<>();
	static StanfordCoreNLP pipeline;
	static int[] doclen;
	static int[] max_tf1;
	static String[] title;
	static int[] queryLen;

	public static void main(String[] args) throws Exception {
		File folder = new File(args[0]);
		String stopwordsFile = args[1];
		File queryFile = new File(args[2]);
		stopWordsList = extractStopWords(stopwordsFile);

		tokanizeDocuments(folder);
		int avgDocLength = findAvgDocLen();
		System.out.println("AVg. doc length: " + avgDocLength);

		List<String> queries = extractQueries(queryFile.getAbsolutePath());
		queryLen = new int[queries.size() + 1];

		for (int i = 0; i < queries.size(); i++) {
			evaluateRelevance(queries.get(i), stopWordsList, i + 1, avgDocLength, folder.listFiles().length);
		}
	}

	private static ArrayList<String> extractStopWords(String stopwordsLocation) {
		File stopwordsFile = new File(stopwordsLocation);
		String line = "";
		try {
			@SuppressWarnings("resource")
			BufferedReader bufferedReader = new BufferedReader(new FileReader(stopwordsFile));
			while ((line = bufferedReader.readLine()) != null) {
				stopWordsList.add(line.trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stopWordsList;

	}

	public static List<String> extractQueries(String filename) throws Exception {
		String data = new String(Files.readAllBytes(new File(filename).toPath()));
		String[] parts = Pattern.compile("[Q0-9:]+").split(data);
		List<String> queries = new ArrayList<>();
		for (String part : parts) {
			String query = part.trim().replaceAll("\\r\\n", " ");
			if (query.length() > 0) {
				queries.add(query);
			}
		}
		return queries;
	}

	public static void tokanizeDocuments(File folder) throws FileNotFoundException, IOException {
		File[] files = folder.listFiles();
		doclen = new int[files.length + 1];
		max_tf1 = new int[files.length + 1];
		title = new String[files.length + 1];

		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(props);
		Pattern pattern = Pattern.compile("<.?title>", Pattern.CASE_INSENSITIVE);

		for (File f : files) {
			if (f.isFile()) {
				tokenizeFile(f, pattern, pipeline);
			}
		}

	}

	private static void tokenizeFile(File f, Pattern pattern, StanfordCoreNLP pipeline) throws IOException {
		String line;
		int docID = 0;
		String[] name = f.getName().split("d");
		if (name[0] != null && name[0].contains("cran"))
			docID = Integer.parseInt(name[1]);
		else
			return;
		@SuppressWarnings("resource")
		BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
		int docLen = 0;
		TreeMap<String, Integer> maxTFIndex1 = new TreeMap<>();

		while ((line = bufferedReader.readLine()) != null) {

			if (!(line.length() >= 3 && line.charAt(0) == '<' && line.charAt(line.length() - 1) == '>')) {
				line = line.toLowerCase();
				line = line.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");

				String doc = line;

				Annotation document = new Annotation(doc);

				pipeline.annotate(document);

				List<CoreMap> sentences = document.get(SentencesAnnotation.class);
				String word = "";

				for (CoreMap sentence : sentences) {
					for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
						word = token.get(LemmaAnnotation.class);
						String tempWord = word.trim();
						if (!stopWordsList.contains(tempWord) && !word.trim().isEmpty()) {
							docLen++;
							if (maxTFIndex1.containsKey(tempWord))
								maxTFIndex1.put(tempWord, maxTFIndex1.get(tempWord) + 1);
							else
								maxTFIndex1.put(tempWord, 1);

							if (tokenMap.containsKey(tempWord)) {
								TermNode node = tokenMap.get(tempWord);
								node.termFrequency += 1;
								if (node.postingFiles.containsKey(docID)) {
									int val = node.postingFiles.get(docID);
									node.postingFiles.put(docID, val + 1);
								} else {
									node.postingFiles.put(docID, 1);
									node.docFrequency += 1;
								}
							} else {
								TermNode node = new TermNode();
								node.termFrequency = 1;
								node.postingFiles = new TreeMap<Integer, Integer>();
								if (node.postingFiles.containsKey(String.valueOf(docID))) {
									int val = node.postingFiles.get(String.valueOf(docID));
									node.postingFiles.put(docID, val + 1);
								} else {
									node.postingFiles.put(docID, 1);
									node.docFrequency += 1;
								}
								tokenMap.put(tempWord, node);
							}
						}
					}
				}
			}
		}
		doclen[docID] = docLen;
		max_tf1[docID] = getMostFrequentValue(maxTFIndex1);
		title[docID] = getTitle(f, pattern);
		// documentDetails.put(docID, new DocDetails(docID, title,
		// getMostFrequentValue(maxTFIndex1), docLen));

	}

	private static String getTitle(File f, Pattern pattern) {

		try {
			String data = new String(Files.readAllBytes(f.toPath()));
			String[] parts = pattern.split(data);
			if (parts.length > 1) {
				return parts[1].replace("\n", " ");
			} else
				System.out.println("...." + f.getPath());

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static int findAvgDocLen() {
		int length = 0;
		for (Integer len : doclen) {
			length += len;
		}
		length = length / (doclen.length - 1);
		return length;
	}

	public static int getMostFrequentValue(final TreeMap<String, Integer> tokenMap) {
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
		return decreasingValuesMap.firstEntry().getValue();
	}

	public static TreeMap<String, Integer> lemmatizeQuery(String queryText, List<String> stopWordsList, int queryID) {
		int length = 0;
		TreeMap<String, Integer> tokenWordMap = new TreeMap<String, Integer>();
		String text = queryText.replaceAll("[-,/]", "");

		Annotation document = new Annotation(text);

		RankedRetrieval.pipeline.annotate(document);

		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		String word = "";
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				word = token.get(LemmaAnnotation.class);
				if (!stopWordsList.contains(word)) {
					length++;
					if (!word.isEmpty()) {
						if (tokenWordMap.containsKey(word)) {
							tokenWordMap.put(word, tokenWordMap.get(word) + 1);
						} else {
							tokenWordMap.put(word, 1);
						}
					}
				}

			}
		}
		queryLen[queryID] = length;
		return tokenWordMap;
	}

	public static void evaluateRelevance(String query, List<String> stopWordsList, int queryID, int avgDocLength,
			int collectionSize) throws Exception {
		TreeMap<String, Integer> queryTermFreqMap = lemmatizeQuery(query, stopWordsList, queryID);

		SortedSet<Entry<String, Integer>> querryTermFreqSorted = entriesSortedByFrequency(queryTermFreqMap);
		Integer maxQueryTermFreq = querryTermFreqSorted.first().getValue();

		Map<Integer, Double> w1Map = new HashMap<>();
		Map<Integer, Double> w2Map = new HashMap<>();

		Map<String, Double> w1QueryMap = new HashMap<>();
		Map<String, Double> w2QueryMap = new HashMap<>();
		TreeMap<String, Double> w1DocMap = new TreeMap<>();
		TreeMap<String, Double> w2DocMap = new TreeMap<>();
		int queryLength = queryLen[queryID];
		for (String queryTerm : queryTermFreqMap.keySet()) {
			TermNode node = tokenMap.get(queryTerm);
			if (node != null) {
				int queryTermFreq = queryTermFreqMap.get(queryTerm);
				int docFreq = node.docFrequency;
				double docQ1 = calcW1(queryTermFreq, maxQueryTermFreq, docFreq, collectionSize);
				double docQ2 = calcW2(queryTermFreq, queryLength, avgDocLength, docFreq, collectionSize);
				w1QueryMap.put(queryTerm, docQ1 * docQ1);
				w2QueryMap.put(queryTerm, docQ2 * docQ2);
				for (Integer docId : node.postingFiles.keySet()) {
					int termFreq = node.postingFiles.get(docId);
					int maxTermFreq = max_tf1[docId];
					int docLength = doclen[docId];
					double docW1 = calcW1(termFreq, maxTermFreq, docFreq, collectionSize);
					double docW2 = calcW2(termFreq, docLength, avgDocLength, docFreq, collectionSize);

					w1DocMap.put(queryTerm + ":" + docId, docW1 * docW1);
					w2DocMap.put(queryTerm + ":" + docId, docW2 * docW2);

					addWtoMap(w1Map, docId, docW1 * docQ1);
					addWtoMap(w2Map, docId, docW2 * docQ2);
				}
			}
		}

		System.out.println("\n\t************************************   Query " + queryID
				+ "   ************************************\n");
		System.out.println("\tQuery: " + query);
		System.out.print("\tVector Representation [term, TF]: ");
		printVectorRepresentation(queryTermFreqMap);

		
		System.out.println("\n\tFor W1 top 5 documents=>");
		System.out.print("\tVector Representation [term, W1]: ");
		printWeightVectorRepresentation(w1QueryMap);
		printTop5Result(w1Map, w1QueryMap, w1DocMap);
		System.out.println("\n\tFor W2 top 5 documents=>");
		System.out.print("\tVector Representation [term, W2]: ");
		printWeightVectorRepresentation(w2QueryMap);
		

		printTop5Result(w2Map, w2QueryMap, w2DocMap);
	}

	private static void printWeightVectorRepresentation(Map<String, Double> wQueryMap) {
		System.out.print("\t[");
		for (String entry : wQueryMap.keySet()) {
			System.out.print(entry + ":" + Math.sqrt(wQueryMap.get(entry)) + ", ");
		}
		System.out.print("]");
		System.out.println();
	}

	private static void printVectorRepresentation(TreeMap<String, Integer> queryTermFreqMap) {
		System.out.print("\t[");
		for (String entry : queryTermFreqMap.keySet()) {
			System.out.print(entry + ":" + queryTermFreqMap.get(entry) + ", ");
		}
		System.out.print("]");
		System.out.println();
	}

	static class ValueComparator implements Comparator<Entry<Integer, Double>> {
		@Override
		public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
			if (o1.getValue() < o2.getValue()) {
				return 1;
			}
			return -1;
		}
	}

	public static SortedSet<Map.Entry<String, Integer>> entriesSortedByFrequency(Map<String, Integer> map) {
		SortedSet<Map.Entry<String, Integer>> sortedEntries = new TreeSet<Map.Entry<String, Integer>>(
				new Comparator<Map.Entry<String, Integer>>() {
					@Override
					public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
						int res = e1.getValue().compareTo(e2.getValue());
						if (res == -1) {
							res = 1;
						} else if (res == 1) {
							res = -1;
						}
						if (e1.getKey().equals(e2.getKey())) {
							return res;
						} else {
							return res != 0 ? res : 1;
						}
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	public static void addWtoMap(Map<Integer, Double> wMap, int docID, double w) {
		if (wMap.get(docID) == null)
			wMap.put(docID, w);
		else
			wMap.put(docID, w + wMap.get(docID));
	}

	public static double calcW1(int termFreq, int maxTermFreq, int docFreq, int collectionSize) {
		double temp = 0;
		try {
			temp = (0.4 + 0.6 * Math.log(termFreq + 0.5) / Math.log(maxTermFreq + 1.0))
					* (Math.log(collectionSize / docFreq) / Math.log(collectionSize));
		} catch (Exception e) {
			temp = 0;
		}
		return temp;
	}

	public static double calcW2(int termFreq, int docLen, double avgDocLen, int docFreq, int collectionSize) {
		double temp = 0;
		try {
			temp = (0.4 + 0.6 * (termFreq / (termFreq + 0.5 + 1.5 * (docLen / avgDocLen)))
					* Math.log(collectionSize / docFreq) / Math.log(collectionSize));
		} catch (Exception e) {
			temp = 0;
		}
		return temp;
	}

	public static void printTop5Result(Map<Integer, Double> wMap, Map<String, Double> wQueryMap,
			Map<String, Double> wDocMap) {

		double queryValue = 0;
		double docValue = 0;

		for (String key : wQueryMap.keySet()) {
			queryValue += wQueryMap.get(key);
		}
		HashMap<String, Double> map = new HashMap<String, Double>();

		for (String key : wDocMap.keySet()) {
			String docId = key.split(":")[1];
			if (map.containsKey(docId)) {
				map.put(docId, map.get(docId) + wDocMap.get(key));
			} else {
				map.put(docId, wDocMap.get(key));
			}
		}

		for (String key : map.keySet()) {
			docValue += map.get(key);
		}

		docValue = Math.sqrt(docValue);
		queryValue = Math.sqrt(queryValue);

		for (Integer key : wMap.keySet()) {
			wMap.put(key, wMap.get(key) / (docValue * queryValue));
		}

		TreeSet<Entry<Integer, Double>> sortedSet = new TreeSet<Entry<Integer, Double>>(new ValueComparator());
		sortedSet.addAll(wMap.entrySet());

		System.out.println("\t\tRank : " + "\t Weight   " + "    : " + " DocID" + " : " + " Title");
		Iterator<Entry<Integer, Double>> iterator = sortedSet.iterator();
		for (int i = 1; i <= 5 && iterator.hasNext(); i++) {
			Entry<Integer, Double> entry = iterator.next();
			System.out.println(
					"\t\t " + (i) + " : " + entry.getValue() + " : " + entry.getKey() + " : " + title[entry.getKey()]);
		}
	}

	public static class TermNode {
		int termFrequency;
		int docFrequency;
		TreeMap<Integer, Integer> postingFiles = new TreeMap<Integer, Integer>();
	}

}
