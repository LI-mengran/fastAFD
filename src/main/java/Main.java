//import FastAFD.AEI.REIwithTopK;
import FastRFD.AEI.REIwithTopK;
import FastRFD.AEI.RelaxedEvidenceInversion;
import FastRFD.AEI.TopKSet;
import FastRFD.RFD.RFDSet;
import FastRFD.TANE.LatticeTraverse;
import FastRFD.Utils;
import FastRFD.evidence.EvidenceSetBuilder;
import FastRFD.input.ColumnStats;
import FastRFD.pli.PliBuilder;
import FastRFD.input.RelationalInput;
import FastRFD.input.Input;
import FastRFD.predicates.PredicatesBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "FastRFD", version = "1.0", mixinStandardHelpOptions = true)
public class Main implements Runnable{

    @Option(names = {"-f"}, description = "input file")
    String fp = "./dataset/tax.csv";

    @Option(names = {"-r"}, description = "rowLimit")
    int rowLimit = 1000;

    @Option(names = {"-d"}, description = "pliShard")
    int shardLength = 10000;

    @Option(names = {"-i"}, description = "output the index")
    boolean indexOutput = false;
    @Option(names = {"-o"}, description = "output the result")
    boolean output = false;
    @Option(names = {"-g"}, description = "without g1 error")
    boolean nog1Error = true;
    @Option(names = {"-t"}, description = "threshold")
    double threshold = 0.01;
//21:55 22:22
    @Option(names = {"-s"}, description = "column thresholds")
    String columnThreshold = "./threshold/tax.txt";
//    String columnThreshold = "";

    @Option(names = {"-e"}, description = "evidences file")
//    String evidencesIndexesFile = "./evidenceSet/evidencesIndexpcm.csv";
    String evidencesIndexesFile = "";

    @Option(names = {"-m"}, description = "mode")
    Integer mode = 1;

    @Option(names = {"-k"}, description = "for topK")
    Integer topK = 50;
    @Override
    public void run(){
        double maxThreshold = 0.5;
        int maxStage = 3;
        switch (mode) {
            case 1 -> System.out.println("[mode] : pipeline + AEI");
            case 2 -> System.out.println("[mode] : pipeline + lattice traverse");
            case 3 -> System.out.println("[mode] : lattice traverse");
            case 4 -> System.out.println("[mode] : RFDD-");
            case 5 -> System.out.println("[mode] : topK--" + topK + " for each attribute");
            case 6 -> System.out.println("[mode] : Topk");
            case 7 -> System.out.println("[mode]: cache with lattice");
            case 8 -> System.out.println("[mode]: rowRFD ");
        }
        System.out.println("[File name] : " + fp);
        System.out.println("[Threshold] : " + threshold);
        System.out.println("[Index] : " + columnThreshold);
        Utils.editDistanceBuffer = new int[50 + 1][50 + 1];
//        for (int i = 0; i < 50; i++) {
//            Utils.editDistanceBuffer[0][i] = i;
//            Utils.editDistanceBuffer[i][0] = i;
//        }
//        System.out.println(calculateEditDistanceWithThreshold(new SubstringableString("vhigh"),0,5,new SubstringableString("med"),0,3, 4, Utils.editDistanceBuffer));

        ArrayList<ColumnStats> columnStats = new ArrayList<>();

        Instant start = Instant.now();
        Input input = new Input(new RelationalInput(fp), rowLimit);
        Duration duration = Duration.between(start, Instant.now());
        System.out.println("[Reading Time] : " + duration.toMillis() + "ms");


        start = Instant.now();
        PliBuilder pliBuilder = new PliBuilder(input.getParsedColumns());
        pliBuilder.buildPlis(columnStats,input.getParsedColumns());
        duration = Duration.between(start, Instant.now());
        System.out.println("[Pli Building Time] : " + duration.toMillis() + "ms");

        start = Instant.now();
        PredicatesBuilder predicatesBuilder = new PredicatesBuilder(maxStage,maxThreshold,columnThreshold);
        try {
            predicatesBuilder.generatePredicates(columnStats);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        duration = Duration.between(start, Instant.now());
        System.out.println("[Predicates Building Time] : " + duration.toMillis() + "ms");

        start = Instant.now();
        EvidenceSetBuilder evidenceSetBuilder = new EvidenceSetBuilder(predicatesBuilder, pliBuilder, input, shardLength);
        if(mode != 3 && mode != 7){
            if( Objects.equals(evidencesIndexesFile, "")) {
                if(mode != 8)
                    evidenceSetBuilder.buildEvidenceSet();
                else
                    evidenceSetBuilder.buildDiffSetBySepPli();
//                    evidenceSetBuilder.buildDiffSetByPli();
                System.out.println("[Evidence Number]: " + evidenceSetBuilder.getEvidenceSet().getEvidenceSet().size());
//        evidenceSetBuilder.testPassjoin();
//        evidenceSetBuilder.outPut();
                if(indexOutput)
                    evidenceSetBuilder.indexOutput(fp.split("/")[fp.split("/").length - 1]);
                duration = Duration.between(start, Instant.now());
                System.out.println("[EvidenceSet Building Time] : " + duration.toMillis() + "ms");
            }
            else{
                try {
                    evidenceSetBuilder.readIndex(evidencesIndexesFile);
                    System.out.println(evidenceSetBuilder.getEvidenceSet().getEvidenceSet().size());
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }


        if(mode == 1 || mode == 4 || mode == 8) {
            start = Instant.now();
            RelaxedEvidenceInversion relaxedEvidenceInversion = new RelaxedEvidenceInversion(predicatesBuilder, input.getRowCount(), evidenceSetBuilder.getEvidenceSet(), !nog1Error);

            RFDSet afdset;
            if(mode == 1)
                afdset = relaxedEvidenceInversion.buildRFD(threshold);
            else
                afdset = relaxedEvidenceInversion.buildRFDSep(threshold);

            duration = Duration.between(start, Instant.now());
            System.out.println("[EvidenceSet Inversion Time] : " + duration.toMillis() + "ms");
            if(output)
                predicatesBuilder.printRFD(afdset, input.getParsedColumns());
        }

        if(mode == 2){
            start = Instant.now();
            LatticeTraverse latticeTraverse = new LatticeTraverse(predicatesBuilder,input.getRowCount(),evidenceSetBuilder.getEvidenceSet(),threshold);

            latticeTraverse.findRFD();
            duration = Duration.between(start, Instant.now());
            System.out.println("[Tranverse Time] : " + duration.toMillis() + "ms");
            if(output)
                predicatesBuilder.printRFD(latticeTraverse.getRFDSet(), input.getParsedColumns());
        }

        if(mode == 3){
            start = Instant.now();
            LatticeTraverse latticeTraverse = new LatticeTraverse(predicatesBuilder,input.getRowCount(),pliBuilder,threshold, input, false);

            latticeTraverse.findRFD();
            duration = Duration.between(start, Instant.now());
            System.out.println("[Tranverse Time] : " + duration.toMillis() + "ms");
        }
        if(mode == 5){
            start = Instant.now();
            REIwithTopK relaxedEvidenceInversion = new REIwithTopK(predicatesBuilder, input.getRowCount(),evidenceSetBuilder.getEvidenceSet(),!nog1Error,topK);

            List<TopKSet> afdSet = relaxedEvidenceInversion.buildTopK(threshold);
            if(output)
//                predicatesBuilder.printTopK(afdSet,input.getParsedColumns());
                predicatesBuilder.printTopKTotal(afdSet,input.getParsedColumns(),topK);


            duration = Duration.between(start, Instant.now());
            System.out.println("[EvidenceSet Inversion Time] : " + duration.toMillis() + "ms");
        }
        if(mode == 6){
            start = Instant.now();
            RelaxedEvidenceInversion relaxedEvidenceInversion = new RelaxedEvidenceInversion(predicatesBuilder, input.getRowCount(), evidenceSetBuilder.getEvidenceSet(), !nog1Error);

            List<TopKSet> afdSet = relaxedEvidenceInversion.buildTopK(threshold,topK);
            if(output)
                predicatesBuilder.printTopK(afdSet,input.getParsedColumns());

            duration = Duration.between(start, Instant.now());
            System.out.println("[EvidenceSet Inversion Time] : " + duration.toMillis() + "ms");
        }
        if(mode == 7){
            start = Instant.now();
            LatticeTraverse latticeTraverse = new LatticeTraverse(predicatesBuilder,input.getRowCount(),pliBuilder,threshold, input, true);

            latticeTraverse.findRFD();
            duration = Duration.between(start, Instant.now());
            System.out.println("[Tranverse Time] : " + duration.toMillis() + "ms");
        }

    }
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
