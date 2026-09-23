package com.github.lisama569.idempotenttaskqueuejava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Spring 上下文加载冒烟测试。
 */
@SpringBootTest
public class ApplicationTests {

    @BeforeAll
    static void cleanData() throws IOException {
        Path dir = Path.of("target/test-boot-data");
        if (Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    @Test
    public void contentLoad() {
    }
}
