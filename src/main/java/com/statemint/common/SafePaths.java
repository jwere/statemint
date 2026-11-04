package com.statemint.common;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds output folder and file names from path templates such as
 * {@code {client}/{yyyy}/{mm}/{client}_part{part:3}.pdf}.
 *
 * <p>Every value is cleaned so it is safe on Windows, Linux and macOS, and the final path is
 * proved to stay inside its root folder. See docs/PSEUDOCODE.md, "Helpers: paths and safe writing".
 */
public final class SafePaths {

    /** Longest folder or file name we produce, in UTF-8 bytes. */
    public static final int MAX_SEGMENT_BYTES = 120;

    private static final Pattern TOKEN = Pattern.compile("\\{([a-zA-Z][a-zA-Z0-9_]*)(?::(\\d{1,2}))?}");
    private static final Pattern FORBIDDEN = Pattern.compile("[/\\\\:*?\"<>|\\p{Cntrl}]");
    private static final Set<String> WINDOWS_RESERVED = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private SafePaths() {
    }

    /**
     * Resolves a path template against a root folder.
     *
     * @param root     the configured output root
     * @param template segments separated by {@code /}, on every platform
     * @param tokens   token values; numbers may be zero-padded with {@code {name:width}}
     * @return a normalised path that is guaranteed to sit inside {@code root}
     * @throws PathException if a token is unknown, a segment comes out empty, or the path escapes
     */
    public static Path build(Path root, String template, Map<String, ?> tokens) {
        Path base = root.toAbsolutePath().normalize();
        Path result = base;
        for (String segment : template.split("/", -1)) {
            String cleaned = cleanSegment(replaceTokens(segment, tokens));
            if (cleaned.isEmpty()) {
                throw new PathException("A folder or file name came out empty in '" + segment + "'");
            }
            result = result.resolve(cleaned);
        }
        result = result.normalize();
        if (!result.startsWith(base) || result.equals(base)) {
            throw new PathException("Path leaves its root folder: " + template);
        }
        return result;
    }

    /** Cleans one folder or file name so every supported operating system accepts it. */
    public static String cleanSegment(String value) {
        if (value == null) {
            return "";
        }
        String v = Normalizer.normalize(value, Normalizer.Form.NFC);
        v = FORBIDDEN.matcher(v).replaceAll("");
        v = v.replace("..", "_");
        v = stripEnds(v);
        int dot = v.indexOf('.');
        String base = dot >= 0 ? v.substring(0, dot) : v;
        if (WINDOWS_RESERVED.contains(base.toUpperCase(Locale.ROOT))) {
            v = "_" + v;
        }
        v = cutToBytes(v, MAX_SEGMENT_BYTES);
        return stripEnds(v);
    }

    /** Key used to detect names that would clash on case-insensitive file systems. */
    public static String clashKey(Path path) {
        return Normalizer.normalize(path.toString(), Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
    }

    private static String replaceTokens(String segment, Map<String, ?> tokens) {
        Matcher m = TOKEN.matcher(segment);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            if (!tokens.containsKey(name)) {
                throw new PathException("Unknown token {" + name + "} in '" + segment + "'");
            }
            Object raw = tokens.get(name);
            String text = raw == null ? "" : String.valueOf(raw);
            if (m.group(2) != null) {
                text = pad(text, Integer.parseInt(m.group(2)));
            }
            m.appendReplacement(out, Matcher.quoteReplacement(cleanSegment(text)));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static String pad(String text, int width) {
        if (!text.chars().allMatch(Character::isDigit) || text.length() >= width) {
            return text;
        }
        return "0".repeat(width - text.length()) + text;
    }

    /** Windows silently drops trailing dots and spaces, so names never start or end with them. */
    private static String stripEnds(String v) {
        int start = 0;
        int end = v.length();
        while (start < end && (v.charAt(start) == ' ' || v.charAt(start) == '.')) {
            start++;
        }
        while (end > start && (v.charAt(end - 1) == ' ' || v.charAt(end - 1) == '.')) {
            end--;
        }
        return v.substring(start, end);
    }

    /** Cuts to at most {@code maxBytes} UTF-8 bytes without splitting a character. */
    private static String cutToBytes(String v, int maxBytes) {
        if (v.getBytes(StandardCharsets.UTF_8).length <= maxBytes) {
            return v;
        }
        StringBuilder out = new StringBuilder();
        int used = 0;
        for (int i = 0; i < v.length(); ) {
            int cp = v.codePointAt(i);
            String ch = new String(Character.toChars(cp));
            int len = ch.getBytes(StandardCharsets.UTF_8).length;
            if (used + len > maxBytes) {
                break;
            }
            out.append(ch);
            used += len;
            i += Character.charCount(cp);
        }
        return out.toString();
    }

    /** A path template or value that cannot produce a safe path. */
    public static final class PathException extends RuntimeException {
        public PathException(String message) {
            super(message);
        }
    }
}
