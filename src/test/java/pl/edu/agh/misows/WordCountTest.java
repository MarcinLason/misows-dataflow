/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.edu.agh.misows;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@RunWith(JUnit4.class)
public class WordCountTest {

    /**
     * Example test that tests a specific {@link DoFn}.
     */
    @Test
    public void testExtractWordsFn() throws Exception {
        DoFnTester<String, String> extractWordsFn = DoFnTester.of(new ExtractWordsFn());

        Assert.assertThat(
                extractWordsFn.processBundle(" some  input  words "),
                CoreMatchers.hasItems("some", "input", "words"));
        Assert.assertThat(extractWordsFn.processBundle(" "), CoreMatchers.hasItems());
        Assert.assertThat(
                extractWordsFn.processBundle(" some ", " input", " words"),
                CoreMatchers.hasItems("some", "input", "words"));
    }

    @Test
    public void countSum() throws IOException {
        int allWords = 0;
        allWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspring-00000-of-00005"));
        allWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspring-00001-of-00005"));
        allWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspring-00002-of-00005"));
        allWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspring-00003-of-00005"));
        allWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspring-00004-of-00005"));

        int vulgarWords = 0;
        vulgarWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspringvulgar-00000-of-00004"));
        vulgarWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspringvulgar-00001-of-00004"));
        vulgarWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspringvulgar-00002-of-00004"));
        vulgarWords += (sum("/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/output/formspringvulgar-00003-of-00004"));

        System.out.println("Formspring vulgar: " + vulgarWords + " all: " + allWords);
    }

    public int sum(String path) throws IOException {
        return Files.lines(Paths.get(path))
                .map(singleString -> singleString.split(":")[1])
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .sum();
    }

    @Test
    public void prepareStringsWithVulgarWords() throws IOException {
        String path = "/home/cinek/Documents/Studia/01_MiSOWS/GoogleDataflow/word-count-beam/src/main/resources/bad-words-list.txt";
        Files.lines(Paths.get(path))
                .map(simpleString -> "\"" + simpleString + "\"" + ",")
                .forEach(simpleString -> System.out.print(simpleString));

    }
}
