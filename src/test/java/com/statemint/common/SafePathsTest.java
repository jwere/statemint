package com.statemint.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SafePathsTest {

    private final Path root = Path.of("out").toAbsolutePath();

    @Test
    void buildsPathFromTokensWithPadding() {
        Path p = SafePaths.build(root, "{client}/{yyyy}/{mm}/{client}_part{part:3}.pdf",
                Map.of("client", "ACME", "yyyy", 2026, "mm", "11", "part", 7));
        assertEquals(root.resolve("ACME").resolve("2026").resolve("11").resolve("ACME_part007.pdf"), p);
    }

    @Test
    void tokenValuesCannotAddFoldersOrEscapeTheRoot() {
        Path p = SafePaths.build(root, "{branch}/file.pdf", Map.of("branch", "../../etc/passwd"));
        assertTrue(p.startsWith(root));
        assertEquals(2, root.relativize(p).getNameCount());
    }

    @Test
    void removesCharactersWindowsRejects() {
        assertEquals("ab cd", SafePaths.cleanSegment("a<b>: c|d?*\""));
    }

    @Test
    void stripsLeadingAndTrailingDotsAndSpaces() {
        assertEquals("report", SafePaths.cleanSegment(" .report. "));
    }

    @Test
    void prefixesWindowsReservedNames() {
        assertEquals("_CON", SafePaths.cleanSegment("CON"));
        assertEquals("_nul.pdf", SafePaths.cleanSegment("nul.pdf"));
        assertEquals("CONSOLE", SafePaths.cleanSegment("CONSOLE"));
    }

    @Test
    void cutsLongNamesWithoutSplittingCharacters() {
        String cleaned = SafePaths.cleanSegment("é".repeat(100));
        assertTrue(cleaned.getBytes(StandardCharsets.UTF_8).length <= SafePaths.MAX_SEGMENT_BYTES);
        assertEquals(60, cleaned.length());
    }

    @Test
    void emptySegmentIsRefused() {
        Map<String, Object> tokens = new HashMap<>();
        tokens.put("branch", null);
        assertThrows(SafePaths.PathException.class, () -> SafePaths.build(root, "{branch}/x.pdf", tokens));
    }

    @Test
    void unknownTokenIsRefused() {
        assertThrows(SafePaths.PathException.class, () -> SafePaths.build(root, "{nope}.pdf", Map.of()));
    }

    @Test
    void clashKeyIgnoresCase() {
        assertEquals(SafePaths.clashKey(root.resolve("Report.pdf")), SafePaths.clashKey(root.resolve("report.pdf")));
    }
}
