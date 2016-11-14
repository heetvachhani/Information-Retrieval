package hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.TreeMap;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class IndexBuilding {
	static PrintWriter out;
	static Stemmer stem = new Stemmer();
	static ArrayList<String> stopWordsList = new ArrayList<>();
	static TreeMap<String, TermNode> tokenMap = new TreeMap<String, TermNode>();
	static TreeMap<String, TermNode> stemMap = new TreeMap<String, TermNode>();
	static int[] doclen;
	static int[] max_tf1;
	static int[] max_tf2;

	static ArrayList<byte[]> compressGammaDoclen;
	static ArrayList<byte[]> compressDeltaDoclen;

	static ArrayList<byte[]> compressMaxTf1;
	static ArrayList<byte[]> compressMaxTf2;

	static double totalTime;

	public static void main(String[] args) {
		String path = "/Users/Heet/Downloads/HW2";
		System.out.println("Do you wish to change output folder path? y/n");
		@SuppressWarnings("resource")
		Scanner sc = new Scanner(System.in);
		String in = sc.next();
		if (in.equals("y")) {
			System.out.print("Please enter output folder path: ");
			path = sc.next();
		}
		
		File tokenUncompressed = new File(path + "/index1-uncompressed");
		File tokenCompressed = new File(path + "/index1-compressed");
		File stemUncompressed = new File(path + "/index2-uncompressed");
		File stemCompressed = new File(path + "/index2-compressed");

		stopWordsList = extractStopWords(args[1]);
		try {
			File cranFieldFolder = new File(args[0]);
			int totalFiles = cranFieldFolder.listFiles().length;
			doclen = new int[totalFiles + 1];
			max_tf1 = new int[totalFiles + 1];
			max_tf2 = new int[totalFiles + 1];
			Arrays.fill(doclen, 0);
			Arrays.fill(max_tf1, 0);
			Arrays.fill(max_tf2, 0);

			compressGammaDoclen = new ArrayList<>(Collections.nCopies(totalFiles + 1, null));
			compressDeltaDoclen = new ArrayList<>(Collections.nCopies(totalFiles + 1, null));
			compressMaxTf1 = new ArrayList<>(Collections.nCopies(totalFiles + 1, null));
			compressMaxTf2 = new ArrayList<>(Collections.nCopies(totalFiles + 1, null));

			tokanizeDocuments(cranFieldFolder);

			// compress index
			ArrayList<CompressedTermIndex> compressedTokenIndex = compressIndex(tokenMap, 8, true);
			ArrayList<CompressedTermIndex> compressedStemIndex = compressIndex(stemMap, 8, false);

			writeUnCompressIndex(tokenMap, tokenUncompressed.getAbsolutePath(), max_tf1);
			writeUnCompressIndex(stemMap, stemUncompressed.getAbsolutePath(), max_tf2);
			writeCompressIndex(compressedTokenIndex, tokenCompressed.getAbsolutePath());
			writeCompressIndex(compressedStemIndex, stemCompressed.getAbsolutePath());

			System.out.println("\t=========Statistics=========");
			System.out.println("	1. Time taken to build all index = " + totalTime + " ms");
			System.out.println(
					"	2. The size of the index Version 1 uncompressed (in bytes) = " + tokenUncompressed.length());
			System.out.println(
					"	3. The size of the index Version 2 uncompressed (in bytes) = " + stemUncompressed.length());
			System.out.println(
					"	4. The size of the index Version 1 compressed (in bytes)   = " + tokenCompressed.length());
			System.out.println(
					"	5. The size of the index Version 2 compressed (in bytes)   = " + stemCompressed.length());
			System.out.println();
			System.out.println("	6. The number of inverted lists in each version of the index");
			System.out.println("	\ta. Version 1 uncompressed = " + tokenMap.size());
			System.out.println("	\tb. Version 2 uncompressed = " + stemMap.size());
			System.out.println("	\tc. Version 1 compressed = " + compressedTokenIndex.size());
			System.out.println("	\td. Version 2 compressed = " + compressedStemIndex.size());
			System.out.println();
			System.out.println("	7. The df, tf, and inverted list length (in bytes) for the terms in stem index");
			String[] terms = { "Reynolds", "NASA", "Prandtl", "flow", "pressure", "boundary", "shock" };
			for (int i = 0; i < terms.length; i++) {
				String testWord = terms[i].toLowerCase();
				stem.add(testWord.toCharArray(), testWord.length());
				stem.stem();
				String stemmedWord = stem.toString();
				TermNode node = stemMap.get(stemmedWord);
				System.out.println("	\tTerm  = " + testWord);
				System.out.println("    \t\tDocument Frequency(df) 	= " + node.docFrequency);
				System.out.println("    \t\tTerm Frequency(tf) 	= " + node.termFrequency);
				System.out.println(
						"	\tSize of Posting file (in bytes) = " + node.postingFiles.size() * 2 * Integer.BYTES);
				System.out.println();
			}

			System.out.println("	8. For term NASA: df, tf, doclen, max_tf in first 3 entries :");
			printDfTfMaxTF("nasa");
			System.out.println();
			System.out.println("	9. For Index1: Term with max & min document frequency:");
			termMaxMinDF(tokenMap);
			System.out.println();
			System.out.println("	10. For Index2: Term with max & min document frequency:");
			termMaxMinDF(stemMap);
			System.out.print("   \n\t11. Documents with largest max_tf= ");
			findMaxValue(max_tf1);
			System.out.print("	 \n\t12. Documents with largest doclen= ");
			findMaxValue(doclen);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("File not found Exception!!");
			
		}

	}

	private static void printDfTfMaxTF(String inputTerm) {
		TermNode node = tokenMap.get(inputTerm);
		Iterator<Integer> it12 = node.postingFiles.keySet().iterator();

		for (int i = 0; i < 3; i++) {
			Integer docId = (Integer) it12.next();
			System.out.println("    \t\tDoc ID: " + (docId) + "   DF: " + node.docFrequency + "   TF: "
					+ node.postingFiles.get(docId) + "   doclen:" + doclen[docId] + "   maxtf: " + max_tf1[docId]);
		}
	}

	private static void findMaxValue(int[] maxtf) {
		int max = Integer.MIN_VALUE;
		ArrayList<Integer> maxDocId = new ArrayList<>();
		for (int i = 0; i < maxtf.length; i++) {
			if (max < maxtf[i]) {
				maxDocId.clear();
				max = maxtf[i];
				maxDocId.add(i);
			} else if (max == maxtf[i]) {
				maxDocId.add(i);
			}
		}
		System.out.println(maxDocId.toString());

	}

	private static void termMaxMinDF(TreeMap<String, TermNode> tokenMap) {
		int maxDF = Integer.MIN_VALUE;
		int minDF = Integer.MAX_VALUE;
		ArrayList<String> maxDFTerm = new ArrayList<>();
		ArrayList<String> minDFTerm = new ArrayList<>();

		Iterator<String> it = tokenMap.keySet().iterator();
		while (it.hasNext()) {
			String term = it.next();
			TermNode node = tokenMap.get(term);

			if (maxDF < node.docFrequency) {
				maxDFTerm.clear();
				maxDF = node.docFrequency;
				maxDFTerm.add(term);
			} else if (maxDF == node.docFrequency) {
				maxDFTerm.add(term);
			}

			if (minDF > node.docFrequency) {
				minDFTerm.clear();
				minDF = node.docFrequency;
				minDFTerm.add(term);
			} else if (minDF == node.docFrequency) {
				minDFTerm.add(term);
			}
		}
		System.out.println("    \t\tMax DF Terms: " + maxDFTerm.toString());
		System.out.println("    \t\tMin DF Terms:" + minDFTerm.toString());

	}

	private static ArrayList<String> extractStopWords(String stopWordsLoc) {
		File stopwordsFile = new File(stopWordsLoc);
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

	// writing uncompressed index
	private static void writeUnCompressIndex(TreeMap<String, TermNode> tokenMap, String filePath, int[] maxtf)
			throws Exception {
		RandomAccessFile index = new RandomAccessFile(filePath, "rw");
		Iterator<String> it = tokenMap.keySet().iterator();

		while (it.hasNext()) {
			String term = it.next();
			TermNode node = tokenMap.get(term);
			index.writeBytes(term);
			index.writeBytes("\t");
			index.write(node.docFrequency);
			index.write(node.termFrequency);
			Iterator<Integer> it1 = node.postingFiles.keySet().iterator();
			while (it1.hasNext()) {
				Integer docId = (Integer) it1.next();
				Integer termFreq = node.postingFiles.get(docId);
				index.write(docId);
				index.writeBytes("\t");
				index.write(termFreq);
				index.writeBytes("\t");
				index.write(doclen[docId]);
				index.writeBytes("\t");
				index.write(maxtf[docId]);
			}
			index.writeBytes("\n");
		}
		index.close();

	}

	// writing compress index
	private static void writeCompressIndex(ArrayList<CompressedTermIndex> compressedTermIndex1, String filePath)
			throws Exception {
		RandomAccessFile index = new RandomAccessFile(filePath, "rw");
		index.writeBytes(CompressedTermIndex.termFrontCoding);
		index.writeBytes("\n");
		for (int i = 0; i < compressedTermIndex1.size(); i++) {
			CompressedTermIndex term = compressedTermIndex1.get(i);
			index.write(term.docFrequency);
			index.writeBytes("\t");
			index.write(term.termFrequency);
			index.writeBytes("\t");
			index.write(term.termPointer);
			index.writeBytes("\t");
			for (int j = 0; j < term.postingFile.size(); j++) {
				index.write(term.postingFile.get(j).docId);
				index.writeBytes("\t");
				index.write(term.postingFile.get(j).termFrequency);
			}
			index.writeBytes("\n");
		}
		index.close();
	}

	public static ArrayList<CompressedTermIndex> compressIndex(TreeMap<String, TermNode> map, int blockingFactor,
			boolean isLemma) {
		Iterator<String> it = map.keySet().iterator();
		ArrayList<CompressedTermIndex> compressedTermIndexList = new ArrayList<>();
		int count = -1;
		int termPointer = 0;
		String[] terms = new String[blockingFactor];
		StringBuffer result = new StringBuffer();
		while (it.hasNext()) {
			String term = it.next();
			CompressedTermIndex termNode;
			if (isLemma)
				termNode = compressPostingFile1(map, term);
			else
				termNode = compressPostingFile2(map, term);

			if (count == -1) {
				termNode.termPointer = termPointer;
				count++;
				terms[count] = term;
				count++;
			} else if (count <= blockingFactor - 1) {
				terms[count] = term;
				count++;
			} else {
				String codeTerm = "";
				if (isLemma)
					codeTerm = performBlockCoding(terms);
				else
					codeTerm = performFrontCoding(terms);
				result.append(codeTerm);
				termPointer += codeTerm.length();
				count = -1;
			}
			compressedTermIndexList.add(termNode);
		}
		CompressedTermIndex.termFrontCoding = result.toString();
		return compressedTermIndexList;
	}

	public static CompressedTermIndex compressPostingFile1(TreeMap<String, TermNode> map, String term) {
		TermNode node = map.get(term);
		CompressedTermIndex termNode = new CompressedTermIndex();
		termNode.docFrequency = ExtractGammaCode(node.docFrequency);
		termNode.termFrequency = ExtractGammaCode(node.termFrequency);
		termNode.postingFile = new ArrayList<>();
		termNode.postingFile.addAll(compressPostingFileUsingGammaCode(node.postingFiles));
		return termNode;

	}

	public static CompressedTermIndex compressPostingFile2(TreeMap<String, TermNode> map, String term) {
		TermNode node = map.get(term);
		CompressedTermIndex termNode = new CompressedTermIndex();
		termNode.docFrequency = deltaCode(node.docFrequency);
		termNode.termFrequency = deltaCode(node.termFrequency);
		termNode.postingFile = new ArrayList<>();
		termNode.postingFile.addAll(compressPostingFileUsingDeltaCode(node.postingFiles));
		return termNode;
	}

	public static ArrayList<PostingFileNode> compressPostingFileUsingGammaCode(TreeMap<Integer, Integer> postingFiles) {

		ArrayList<PostingFileNode> compressedList = new ArrayList<>();
		Iterator<Integer> it = postingFiles.keySet().iterator();
		Integer firstDocIdEntry = it.next();
		Integer termFreq = postingFiles.get(firstDocIdEntry);
		PostingFileNode node = new PostingFileNode();
		node.docId = ExtractGammaCode(firstDocIdEntry);
		node.termFrequency = ExtractGammaCode(termFreq);
		compressGammaDoclen.set(firstDocIdEntry.intValue(), ExtractGammaCode(doclen[firstDocIdEntry.intValue()]));
		compressMaxTf1.set(firstDocIdEntry.intValue(), ExtractGammaCode(doclen[firstDocIdEntry.intValue()]));
		compressedList.add(node);

		while (it.hasNext()) {
			Integer docId = it.next();
			termFreq = postingFiles.get(docId);

			node = new PostingFileNode();
			node.docId = ExtractGammaCode(docId - firstDocIdEntry);
			node.termFrequency = ExtractGammaCode(termFreq);
			compressGammaDoclen.set(docId, ExtractGammaCode(doclen[docId]));
			compressMaxTf1.set(docId, ExtractGammaCode(doclen[docId]));

			compressedList.add(node);
			firstDocIdEntry = docId;
		}
		return compressedList;
	}

	public static ArrayList<PostingFileNode> compressPostingFileUsingDeltaCode(TreeMap<Integer, Integer> postingFiles) {
		ArrayList<PostingFileNode> compressedList = new ArrayList<>();
		Iterator<Integer> it = postingFiles.keySet().iterator();
		Integer firstDocIdEntry = it.next();
		Integer termFreq = postingFiles.get(firstDocIdEntry);
		PostingFileNode node = new PostingFileNode();
		node.docId = deltaCode(firstDocIdEntry);
		node.termFrequency = deltaCode(termFreq);
		compressDeltaDoclen.set(firstDocIdEntry.intValue(), deltaCode(doclen[firstDocIdEntry.intValue()]));
		compressMaxTf2.set(firstDocIdEntry.intValue(), deltaCode(doclen[firstDocIdEntry.intValue()]));
		compressedList.add(node);

		while (it.hasNext()) {
			Integer docId = it.next();
			termFreq = postingFiles.get(docId);

			node = new PostingFileNode();
			node.docId = deltaCode(docId - firstDocIdEntry);
			node.termFrequency = deltaCode(termFreq);
			compressDeltaDoclen.set(docId, deltaCode(doclen[docId]));
			compressMaxTf1.set(docId, deltaCode(doclen[docId]));

			compressedList.add(node);
			firstDocIdEntry = docId;
		}
		return compressedList;
	}

	public static String performBlockCoding(String[] terms) {
		String encode = "";
		for (int i = 0; i < terms.length; i++) {
			String temp = terms[i];

			encode += temp.length() + temp;
		}
		return encode;
	}

	public static String performFrontCoding(String[] terms) {
		String temp = terms[0];
		String encode = "";
		encode += temp.length() + temp + "*";
		String x = "";
		for (int i = 0; i < terms.length; i++) {
			x = terms[i];
			x = x.replace(temp, "");
			encode += x.length() + x + "*";
		}
		return encode;
	}

	public static byte[] deltaCode(Integer termFreq) {
		String binary = Integer.toBinaryString(termFreq);
		String gammaCode = getGammaCode(binary.length());
		binary = binary.substring(1);
		String deltaCode = gammaCode + binary;
		BitSet bits = new BitSet();
		bits = fromString(deltaCode);
		return bits.toByteArray();
	}

	public static byte[] ExtractGammaCode(Integer docId) {
		BitSet bits = new BitSet();
		bits = fromString(getGammaCode(docId));
		return bits.toByteArray();
	}

	public static String getGammaCode(Integer docId) {
		String binary = Integer.toBinaryString(docId);
		binary = binary.substring(1);
		String unaryCodeForLength = "";
		for (int i = 0; i < binary.length(); i++) {
			unaryCodeForLength += "1";
		}
		unaryCodeForLength += "0";
		String gamma = unaryCodeForLength + binary;
		return gamma;
	}

	private static BitSet fromString(final String s) {
		return BitSet.valueOf(new long[] { Long.parseLong(s, 2) });
	}

	public static void tokanizeDocuments(File folder) throws FileNotFoundException, IOException {
		File[] files = folder.listFiles();
		long startTime = System.currentTimeMillis();

		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

		for (File f : files) {
			if (f.isFile()) {
				tokenizeFile(f, pipeline);
			}
		}

		totalTime = System.currentTimeMillis() - startTime;
	}

	private static void tokenizeFile(File f, StanfordCoreNLP pipeline) throws IOException {
		String line;
		int docID = 0;
		String[] name = f.getName().split("d");
		if (name[0] != null && name[0].contains("cran"))
			docID = Integer.parseInt(name[1]);

		@SuppressWarnings("resource")
		BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
		int docLen = 0;
		TreeMap<String, Integer> maxTFIndex1 = new TreeMap<>();
		TreeMap<String, Integer> maxTFIndex2 = new TreeMap<>();

		while ((line = bufferedReader.readLine()) != null) {

			if (!(line.length() >= 3 && line.charAt(0) == '<' && line.charAt(line.length() - 1) == '>')) {
				line = line.toLowerCase();
				line = line.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");

				String doc = line;

				// create an empty Annotation just with the given text
				Annotation document = new Annotation(doc);

				// run all Annotators on this text
				pipeline.annotate(document);

				// these are all the sentences in this document
				// a CoreMap is essentially a Map that uses class objects as
				// keys and has values with custom types
				List<CoreMap> sentences = document.get(SentencesAnnotation.class);
				String word = "";

				for (CoreMap sentence : sentences) {
					// traversing the words in the current sentence
					// a CoreLabel is a CoreMap with additional
					// token-specific methods
					for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
						// this is the text of the token
						word = token.get(LemmaAnnotation.class);
						docLen++;
						String tempWord = word.trim();
						if (!stopWordsList.contains(tempWord) && !word.trim().isEmpty()) {
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

				String[] words = line.split(" ");
				for (int j = 0; j < words.length; j++) {
					if (!stopWordsList.contains(words[j])) {
						String tempWord = words[j].trim();

						stem.add(tempWord.toCharArray(), tempWord.length());
						stem.stem();
						String stemmedWord = stem.toString();

						if (!stemmedWord.isEmpty()) {
							if (maxTFIndex2.containsKey(stemmedWord))
								maxTFIndex2.put(stemmedWord, maxTFIndex2.get(stemmedWord) + 1);
							else
								maxTFIndex2.put(stemmedWord, 1);

							if (stemMap.containsKey(stemmedWord)) {
								TermNode node = stemMap.get(stemmedWord);
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
								if (node.postingFiles.containsKey(docID)) {
									int val = node.postingFiles.get(docID);
									node.postingFiles.put(docID, val + 1);
								} else {
									node.postingFiles.put(docID, 1);
									node.docFrequency += 1;
								}
								stemMap.put(stemmedWord, node);
							}
						}
					}
				}
			}
		}
		doclen[docID] = docLen;
		max_tf1[docID] = getMostFrequentValue(maxTFIndex1);
		max_tf2[docID] = getMostFrequentValue(maxTFIndex2);

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

	public static class TermNode {
		int termFrequency;
		int docFrequency;
		TreeMap<Integer, Integer> postingFiles = new TreeMap<Integer, Integer>();
	}

	public static class CompressedTermIndex {
		static String termFrontCoding;
		byte[] termFrequency;
		byte[] docFrequency;
		int termPointer;
		ArrayList<PostingFileNode> postingFile = new ArrayList<PostingFileNode>();
	}

	public static class PostingFileNode {
		byte[] docId;
		byte[] termFrequency;
	}

}
