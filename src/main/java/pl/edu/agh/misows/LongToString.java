package pl.edu.agh.misows;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class LongToString extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<String> receiver) {
        if (Objects.nonNull(element)) {
            receiver.output(element.toString());
        }
    }
}
