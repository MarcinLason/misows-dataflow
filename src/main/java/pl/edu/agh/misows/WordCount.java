package pl.edu.agh.misows;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WordCount {

    static class ExtractVulgarWordsFn extends DoFn<String, String> {
        public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractVulgarWordsFn.class, "lineLenDistro");
        private static List<String> vulgarWords;

        public ExtractVulgarWordsFn(List vulgarWords) {
            this.vulgarWords = vulgarWords;
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {

            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            for (String word : words) {
                if (!word.isEmpty() && vulgarWords.contains(word)) {
                    receiver.output(word);
                }
            }
        }
    }

    static class ExtractWordsFn extends DoFn<String, String> {
        public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        private List<String> vulgarWords;

        public CountWords(List vulgarWords) {
            this.vulgarWords = vulgarWords;
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            PCollection<String> words = lines.apply(ParDo.of(new ExtractVulgarWordsFn(vulgarWords)));
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
            return wordCounts;
        }
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);
        runWordCount(options);
    }

    static void runWordCount(WordCountOptions options) {
        List<String> vulgarWords = new ArrayList<>();
        try {
            vulgarWords = Files.lines(Paths.get(options.getVulgarWordsPath()))
                    .map(String::trim)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new CountWords(vulgarWords))
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://misows-dataflow-demo/data/formspring10000.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

        @Description("Path to the file with vulgar words")
        @Default.String("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/src/main/resources/bad-words-list.txt")
        String getVulgarWordsPath();

        void setVulgarWordsPath(String value);
    }
}
