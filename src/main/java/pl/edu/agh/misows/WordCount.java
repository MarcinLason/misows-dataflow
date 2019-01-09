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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class WordCount {
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

    public static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            PCollection<Long> wordCounts = words.apply(Count.globally());
            PCollection<String> stringWordCounts = wordCounts.apply(ParDo.of(new DoFn<Long, String>() {
                @ProcessElement
                public void processElement(@Element Long element, OutputReceiver<String> receiver) {
                    if (Objects.nonNull(element)) {
                        receiver.output(element.toString());
                    }
                }
            }));
            return stringWordCounts;
        }
    }

    public static class CountVulgarWords extends PTransform<PCollection<String>, PCollection<String>> {
        private static final List<String> vulgarWords = Arrays.asList("2g1c","2 girls 1 cup","acrotomophilia","alabama hot pocket","alaskan pipeline","anal","anilingus","anus","apeshit","arsehole","ass","asshole","assmunch","auto erotic","autoerotic","babeland","baby batter","baby juice","ball gag","ball gravy","ball kicking","ball licking","ball sack","ball sucking","bangbros","bareback","barely legal","barenaked","bastard","bastardo","bastinado","bbw","bdsm","beaner","beaners","beaver cleaver","beaver lips","bestiality","big black","big breasts","big knockers","big tits","bimbos","birdlock","bitch","bitches","black cock","blonde action","blonde on blonde action","blowjob","blow job","blow your load","blue waffle","blumpkin","bollocks","bondage","boner","boob","boobs","booty call","brown showers","brunette action","bukkake","bulldyke","bullet vibe","bullshit","bung hole","bunghole","busty","butt","buttcheeks","butthole","camel toe","camgirl","camslut","camwhore","carpet muncher","carpetmuncher","chocolate rosebuds","circlejerk","cleveland steamer","clit","clitoris","clover clamps","clusterfuck","cock","cocks","coprolagnia","coprophilia","cornhole","coon","coons","creampie","cum","cumming","cunnilingus","cunt","darkie","date rape","daterape","deep throat","deepthroat","dendrophilia","dick","dildo","dingleberry","dingleberries","dirty pillows","dirty sanchez","doggie style","doggiestyle","doggy style","doggystyle","dog style","dolcett","domination","dominatrix","dommes","donkey punch","double dong","double penetration","dp action","dry hump","dvda","eat my ass","ecchi","ejaculation","erotic","erotism","escort","eunuch","faggot","fecal","felch","fellatio","feltch","female squirting","femdom","figging","fingerbang","fingering","fisting","foot fetish","footjob","frotting","fuck","fuck buttons","fuckin","fucking","fucktards","fudge packer","fudgepacker","futanari","gang bang","gay sex","genitals","giant cock","girl on","girl on top","girls gone wild","goatcx","goatse","god damn","gokkun","golden shower","goodpoop","goo girl","goregasm","grope","group sex","g-spot","guro","hand job","handjob","hard core","hardcore","hentai","homoerotic","honkey","hooker","hot carl","hot chick","how to kill","how to murder","huge fat","humping","incest","intercourse","jack off","jail bait","jailbait","jelly donut","jerk off","jigaboo","jiggaboo","jiggerboo","jizz","juggs","kike","kinbaku","kinkster","kinky","knobbing","leather restraint","leather straight jacket","lemon party","lolita","lovemaking","make me come","male squirting","masturbate","menage a trois","milf","missionary position","motherfucker","mound of venus","mr hands","muff diver","muffdiving","nambla","nawashi","negro","neonazi","nigga","nigger","nig nog","nimphomania","nipple","nipples","nsfw images","nude","nudity","nympho","nymphomania","octopussy","omorashi","one cup two girls","one guy one jar","orgasm","orgy","paedophile","paki","panties","panty","pedobear","pedophile","pegging","penis","phone sex","piece of shit","pissing","piss pig","pisspig","playboy","pleasure chest","pole smoker","ponyplay","poof","poon","poontang","punany","poop chute","poopchute","porn","porno","pornography","prince albert piercing","pthc","pubes","pussy","queaf","queef","quim","raghead","raging boner","rape","raping","rapist","rectum","reverse cowgirl","rimjob","rimming","rosy palm","rosy palm and her 5 sisters","rusty trombone","sadism","santorum","scat","schlong","scissoring","semen","sex","sexo","sexy","shaved beaver","shaved pussy","shemale","shibari","shit","shitblimp","shitty","shota","shrimping","skeet","slanteye","slut","s&m","smut","snatch","snowballing","sodomize","sodomy","spic","splooge","splooge moose","spooge","spread legs","spunk","strap on","strapon","strappado","strip club","style doggy","suck","sucks","suicide girls","sultry women","swastika","swinger","tainted love","taste my","tea bagging","threesome","throating","tied up","tight white","tit","tits","titties","titty","tongue in a","topless","tosser","towelhead","tranny","tribadism","tub girl","tubgirl","tushy","twat","twink","twinkie","two girls one cup","undressing","upskirt","urethra play","urophilia","vagina","venus mound","vibrator","violet wand","vorarephilia","voyeur","vulva","wank","wetback","wet dream","white power","wrapping men","wrinkled starfish","xx","xxx","yaoi","yellow showers","yiffy","zoophilia");
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
            PCollection<String> filteredWords = words.apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(@Element String element, OutputReceiver<String> receiver) {

                    if (Objects.nonNull(element) && vulgarWords.contains(element))
                        receiver.output(element);
                }
            }));
            PCollection<Long> wordCounts = filteredWords.apply(Count.globally());
            PCollection<String> stringWordCounts = wordCounts.apply(ParDo.of(new DoFn<Long, String>() {
                @ProcessElement
                public void processElement(@Element Long element, OutputReceiver<String> receiver) {
                    if (Objects.nonNull(element)) {
                        receiver.output(element.toString());
                    }
                }
            }));
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
//                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
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
