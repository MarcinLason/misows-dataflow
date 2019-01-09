package pl.edu.agh.misows;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class WordCount {
    public static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            PCollection<Long> wordCounts = words.apply(Count.globally());
            PCollection<String> stringWordCounts = wordCounts.apply(ParDo.of(new LongToString()));
            return stringWordCounts;
        }
    }

    public static class CountVulgarWords extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            PCollection<String> filteredWords = words.apply(ParDo.of(new VulgarWordsFilter()));
            PCollection<Long> wordCounts = filteredWords.apply(Count.globally());
            PCollection<String> stringWordCounts = wordCounts.apply(ParDo.of(new LongToString()));
            return stringWordCounts;
        }
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);

        String[] inputFiles = options.getInputFile().split("##");
        String[] outputFiles = options.getOutput().split("##");

        for (int i = 0; i < inputFiles.length; i++) {
            options.setInputFile(inputFiles[i]);
            options.setOutput(outputFiles[i]);
            System.out.println("RUNNING: " + inputFiles[0] + " -> " + outputFiles[0]);
            runWordCount(options, new CountWords());
        }

        for (int i = 0; i < inputFiles.length; i++) {
            options.setInputFile(inputFiles[i]);
            options.setOutput(outputFiles[i] + "vulgar");
            System.out.println("RUNNING: " + inputFiles[0] + " -> " + outputFiles[0]);
            runWordCount(options, new CountVulgarWords());
        }
    }

    static void runWordCount(WordCountOptions options, PTransform<PCollection<String>, PCollection<String>> transform) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(transform)
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }

    public interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://misows-dataflow-demo/data/formspring10000.txt##" +
                "gs://misows-dataflow-demo/data/beerPosts2549.txt##" +
                "gs://misows-dataflow-demo/data/politicsPosts22915.txt##" +
                "gs://misows-dataflow-demo/data/redditSubmissions50000.txt##" +
                "gs://misows-dataflow-demo/data/ubuntuPosts37833.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }
}
