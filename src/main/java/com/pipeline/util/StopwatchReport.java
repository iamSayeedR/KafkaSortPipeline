package com.pipeline.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks named timing phases and produces a formatted summary report.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * var sw = new StopwatchReport();
 * sw.startPhase("Generate");
 * // ... work ...
 * sw.stopPhase("Generate");
 * System.out.println(sw.formatReport());
 * }</pre>
 */
public class StopwatchReport {

    /** Tracks elapsed millis per phase, in insertion order */
    private final Map<String, Long> elapsed = new LinkedHashMap<>();

    /** Tracks in-progress phases */
    private final Map<String, Long> running = new LinkedHashMap<>();

    /** Preserves insertion order for the report */
    private final List<String> phaseOrder = new ArrayList<>();

    /**
     * Starts timing a named phase.
     *
     * @param name phase name (e.g. "Generate", "Sort-ID")
     * @throws IllegalStateException if the phase is already running
     */
    public void startPhase(String name) {
        if (running.containsKey(name)) {
            throw new IllegalStateException("Phase '" + name + "' is already running");
        }
        running.put(name, System.currentTimeMillis());
        if (!elapsed.containsKey(name)) {
            phaseOrder.add(name);
        }
    }

    /**
     * Stops timing a named phase and returns the elapsed milliseconds.
     *
     * @param name phase name — must have been started
     * @return elapsed time in milliseconds
     * @throws IllegalStateException if the phase was not started
     */
    public long stopPhase(String name) {
        Long startTime = running.remove(name);
        if (startTime == null) {
            throw new IllegalStateException("Phase '" + name + "' was not started");
        }
        long millis = System.currentTimeMillis() - startTime;
        elapsed.merge(name, millis, Long::sum);
        return millis;
    }

    /**
     * Formats a human-readable report of all completed phases.
     *
     * @return multi-line report string
     */
    public String formatReport() {
        var sb = new StringBuilder();
        String divider = "------------------------------------";

        sb.append("=== Pipeline Run Report ===\n");
        sb.append(String.format("%-25s %s%n", "Phase", "Wall Time"));
        sb.append(divider).append('\n');

        long totalMs = 0;
        for (String name : phaseOrder) {
            Long ms = elapsed.get(name);
            if (ms != null) {
                sb.append(String.format("%-25s %s%n", name, formatDuration(ms)));
                totalMs += ms;
            }
        }

        sb.append(divider).append('\n');
        sb.append(String.format("%-25s %s%n", "TOTAL WALL CLOCK", formatDuration(totalMs)));

        return sb.toString();
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static String formatDuration(long ms) {
        if (ms < 1000) {
            return ms + "ms";
        } else if (ms < 60_000) {
            return String.format("%.1fs", ms / 1000.0);
        } else {
            long minutes = ms / 60_000;
            double seconds = (ms % 60_000) / 1000.0;
            return String.format("%dm %.1fs", minutes, seconds);
        }
    }
}
