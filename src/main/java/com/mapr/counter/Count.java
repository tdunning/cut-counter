package com.mapr.counter;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.io.Files;
import org.kohsuke.args4j.*;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.Setter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Counts lines. Every so often, we spit out a count of lines seen and unique lines so far.
 * <p>
 * Command line options include:
 * <p>
 * -interval n How often to summarize counts
 * <p>
 * -skip m How many header lines to skip in each input file
 * <p>
 * files The files to read.
 */
public class Count {
    /**
     * Reads lines that have user,item pairs encoded as integers with few missing values. These values
     * are counted in a variety of ways including the number of unique user,item pairs, the number of
     * unique users and the number of unique items. In addition, these same values are counted after
     * taking into account an item frequency cut and a user interaction cut.
     *
     * Every so often, these unique counts are printed to standard out. At the end of the run,
     * distributions of the number of non-zeros per row (user) and per column (item) in the history
     * matrix are printed to user.dist.csv and item.dist.csv respectively.
     *
     * All command line arguments except for the names of the files to process are optional. The following
     * optional arguments can be used:
     *
     * -interval n  Controls how often to report counts. Default is 100000
     *
     * -skip m  Controls how many lines to skip at the beginning of each file. Default is 1.
     *
     * -fCut f  Sets the frequency cut. Default is 100.
     *
     * -iCut i  Sets the interaction cut. Default is 100.
     *
     * @param args
     * @throws CmdLineException
     * @throws IOException
     */
    public static void main(String[] args) throws CmdLineException, IOException {
        final Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println("Usage: " +
                    "[ -interval n ] " +
                    "[ -skip m ] " +
                    "[ -fCut f ] " +
                    "[ -iCut i ] " +
                    "files ...");
            throw e;
        }

        // counts user interactions with cut
        IntCounter aCount = new IntCounter();

        // counts item interactions with cuts
        IntCounter bCount = new IntCounter();

        // counts user interactions without cut
        IntCounter rawRowCount = new IntCounter();

        // counts item interactions without cut
        IntCounter rawItemCount = new IntCounter();

        // records all interactions without cut
        Multiset<Pair> straight = HashMultiset.create();

        // records all interactions with cut
        Multiset<Pair> cut = HashMultiset.create();

        // records all interactions after cut
        Map<Integer, List<Integer>> userHistory = Maps.newHashMap();

        // for reservoir sampling
        Random gen = new Random(1);

        int totalLines = 0;

        System.out.printf("n,interactions,items,users,interactions.cut,items.cut,users.cut\n");
        for (String file : opts.files) {
            try (BufferedReader in = Files.newReader(new File(file), Charsets.UTF_8)) {
                for (int i = 0; i < opts.skip; i++) {
                    in.readLine();
                }
                int line = 0;

                try {
                    int[] values = parseCsv(in, ++line);
                    while (values != null) {
                        totalLines++;

                        Pair ab = new Pair(values);
                        rawRowCount.add(values[0]);
                        rawItemCount.add(values[1]);
                        straight.add(ab);

                        if (line % opts.interval == 0) {
                            System.out.printf("%d,%d,%d,%d,%d,%d,%d\n", totalLines,
                                    straight.elementSet().size(), rawItemCount.uniques(), rawRowCount.uniques(),
                                    cut.elementSet().size(), aCount.uniques(), bCount.uniques());
                        }

                        // we assume that users are in random order which makes the frequency cut trivial
                        if (bCount.get(values[1]) < opts.frequencyCut) {
                            List<Integer> row = userHistory.get(values[0]);
                            if (row == null) {
                                row = Lists.newArrayList();
                                userHistory.put(values[0], row);
                            }
                            if (row.size() < opts.interactionCut) {
                                // unconditionally accept this interaction
                                cut.add(ab);
                                aCount.add(values[0]);
                                row.add(values[1]);
                                bCount.add(values[1]);
                            } else {
                                // maxed out this user. We may have to push out another interaction
                                int sample = gen.nextInt(rawRowCount.get(values[0]));
                                if (sample < row.size() && row.get(sample) != values[1]) {
                                    int old = row.get(sample);
                                    cut.add(ab);
                                    cut.remove(new Pair(values[0], old));
                                    row.set(sample, values[1]);
                                    bCount.add(values[1]);
                                    bCount.add(old, -1);
                                }
                            }
                        }
                        values = parseCsv(in, ++line);
                    }
                } catch (Throwable e) {
                    throw new IOException(String.format("Error at %s:%d", file, line), e);
                }
            }
        }

        printDistribution(aCount, "user.dist.csv");
        printDistribution(bCount, "item.dist.csv");
    }

    /**
     * Prints the distribution of a bunch of counts.
     *
     * @param counterTable The table to count
     * @param pathname     Where to put the output
     * @throws FileNotFoundException
     */
    private static void printDistribution(IntCounter counterTable, String pathname) throws FileNotFoundException {
        try (PrintWriter out = new PrintWriter(new File(pathname))) {
            Multiset<Integer> count = HashMultiset.create();
            for (int i = 0; i < counterTable.fill; i++) {
                if (counterTable.get(i) > 0) {
                    count.add(counterTable.get(i));
                }
            }
            out.printf("interactions,count\n");
            for (Integer i : count.elementSet()) {
                out.printf("%d,%d\n", i, count.count(i));
            }
        }
    }

    /**
     * Character level CSV decoding (supposed to be fast)
     *
     * @param in   Our input
     * @param line Current line number for error messages
     * @return An array of the integers found on this line, null if EOF
     * @throws IOException In the event of error reading data
     */
    private static int[] parseCsv(BufferedReader in, int line) throws IOException {
        int ch = in.read();
        if (ch == -1) {
            return null;
        }
        int[] r = new int[2];
        while (ch != -1 && Character.isSpaceChar(ch)) {
            ch = in.read();
        }
        while (ch != -1 && Character.isDigit(ch)) {
            r[0] = r[0] * 10 + (ch - '0');
            ch = in.read();
        }
        while (ch != -1 && Character.isSpaceChar(ch)) {
            ch = in.read();
        }

        if (ch != ',') {
            throw new IOException(String.format("Ill-formed input on line %d, expected comma, got %c", line, ch));
        } else {
            ch = in.read();
        }
        while (ch != -1 && Character.isSpaceChar(ch)) {
            ch = in.read();
        }
        while (ch != -1 && Character.isDigit(ch)) {
            r[1] = r[1] * 10 + (ch - '0');
            ch = in.read();
        }
        while (ch != -1 && ch != '\n') {
            ch = in.read();
        }
        return r;
    }

    /**
     * Primitive array based counter for integers
     */
    private static class IntCounter {
        int[] counts = new int[1000];
        int fill = 0;
        int uniques = 0;

        public void add(int value) {
            resize(value);
            if (counts[value] == 0) {
                uniques++;
            }
            counts[value]++;
            fill = Math.max(fill, value);
        }

        private void resize(int value) {
            if (value >= counts.length) {
                int[] newArray = new int[Math.min(value + 100, 2 * counts.length)];
                System.arraycopy(counts, 0, newArray, 0, fill + 1);
                counts = newArray;
            }
        }

        public int get(int value) {
            if (value >= counts.length) {
                return 0;
            }
            return counts[value];
        }

        public int size() {
            return fill;
        }

        public void add(int value, int delta) {
            resize(value);
            if (counts[value] + delta < 0) {
                throw new IllegalArgumentException("Cannot decrement below zero");
            } else if (counts[value] == 0 && delta > 0) {
                uniques++;
            } else if (counts[value] == -delta) {
                uniques--;
            }

            counts[value] += delta;
            fill = Math.max(fill, value);
        }

        public int uniques() {
            return uniques;
        }
    }

    /**
     * For counting interactions
     */
    private static class Pair {
        int a;
        int b;

        public Pair(int[] values) {
            a = values[0];
            b = values[1];
        }

        public Pair(int a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return Objects.equal(a, pair.a) &&
                    Objects.equal(b, pair.b);
        }

        @Override
        public int hashCode() {
            return a * 31147 + b * 65479;
        }
    }

    /**
     * For command line argument parsing
     */
    private static class Options {
        @Option(name = "-interval")
        int interval = 100000;

        @Option(name = "-skip")
        int skip = 1;

        @Option(name = "-fCut")
        int frequencyCut = 100;

        @Option(name = "-iCut")
        int interactionCut = 100;

        @Argument
        private List<String> files = new ArrayList<String>();

        public static class SizeParser extends IntOptionHandler {
            public SizeParser(CmdLineParser parser, OptionDef option, Setter<? super Integer> setter) {
                super(parser, option, setter);
            }

            @Override
            protected Integer parse(String argument) throws NumberFormatException {
                int n = Integer.parseInt(argument.replaceAll("[kKMG]?$", ""));

                switch (argument.charAt(argument.length() - 1)) {
                    case 'G':
                        n *= 1e9;
                        break;
                    case 'M':
                        n *= 1e6;
                        break;
                    case 'K':
                    case 'k':
                        n *= 1e3;
                        break;
                    default:
                        // no suffix leads here
                        break;
                }
                return n;
            }
        }
    }
}
