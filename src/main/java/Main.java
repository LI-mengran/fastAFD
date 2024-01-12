import FastAFD.AEI.RelaxedEvidenceInversion;
import FastAFD.AFD.AFDSet;
import FastAFD.Utils;
import FastAFD.evidence.EvidenceSetBuilder;
import FastAFD.input.ColumnStats;
import FastAFD.passjoin.PassJoin;
import FastAFD.passjoin.SubstringableString;
import FastAFD.pli.Pli;
import FastAFD.pli.PliBuilder;
import FastAFD.input.RelationalInput;
import FastAFD.input.Input;
import FastAFD.predicates.PredicatesBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "FastAFD", version = "1.0", mixinStandardHelpOptions = true)
public class Main implements Runnable{

    @Option(names = {"-f"}, description = "input file")
    String fp = "./dataset/tax.csv";

    @Option(names = {"-r"}, description = "rowLimit")
    int rowLimit = 1000;

    @Option(names = {"-o"}, description = "output the index")
    boolean indexOutput = false;

    @Option(names = {"-g"}, description = "output the index")
    boolean nog1Error = false;
    @Option(names = {"-t"}, description = "threshold")
    double threshold = 0.01;
//21:55 22:22
    @Option(names = {"-s"}, description = "column thresholds")
    String columnThreshold = "./threshold/tax.txt";
//    String columnThreshold = "";

    @Option(names = {"-e"}, description = "evidences file")
    String evidencesIndexesFile = "evidencesIndexpcm.txt";
//    String evidencesIndexesFile = "";
    @Override
    public void run(){
        double maxThreshold = 0.5;
        int maxStage = 3;
        System.out.println("[File name] : " + fp);
        System.out.println("[Threshold] : " + threshold);
        System.out.println("[Index] : " + columnThreshold);

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
        EvidenceSetBuilder evidenceSetBuilder = new EvidenceSetBuilder(predicatesBuilder, pliBuilder, input);
        if(Objects.equals(evidencesIndexesFile, "")) {
            evidenceSetBuilder.buildEvidenceSet();
            System.out.println(evidenceSetBuilder.getEvidenceSet().getEvidenceSet().size());
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

        start = Instant.now();
        RelaxedEvidenceInversion relaxedEvidenceInversion = new RelaxedEvidenceInversion(predicatesBuilder, input.getRowCount(),evidenceSetBuilder.getEvidenceSet(),nog1Error);
        AFDSet afdSet = relaxedEvidenceInversion.buildAFD(threshold);
        afdSet.show();
        duration = Duration.between(start, Instant.now());
        System.out.println("[EvidenceSet Inversion Time] : " + duration.toMillis() + "ms");

    }
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
