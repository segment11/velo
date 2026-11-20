package io.velo.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import groovyjarjarasm.asm.ClassReader;
import groovyjarjarasm.asm.ClassVisitor;
import groovyjarjarasm.asm.FieldVisitor;
import groovyjarjarasm.asm.MethodVisitor;
import groovyjarjarasm.asm.Opcodes;
import groovyjarjarasm.asm.Type;
import org.codehaus.groovy.ast.ClassCodeVisitorSupport;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.FieldNode;
import org.codehaus.groovy.ast.ImportNode;
import org.codehaus.groovy.ast.InnerClassNode;
import org.codehaus.groovy.ast.MethodNode;
import org.codehaus.groovy.ast.ModuleNode;
import org.codehaus.groovy.ast.Parameter;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.AttributeExpression;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.CastExpression;
import org.codehaus.groovy.ast.expr.ClassExpression;
import org.codehaus.groovy.ast.expr.ClosureExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.ConstructorCallExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.MapEntryExpression;
import org.codehaus.groovy.ast.expr.MapExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.PostfixExpression;
import org.codehaus.groovy.ast.expr.PrefixExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.ExpressionStatement;
import org.codehaus.groovy.ast.stmt.ForStatement;
import org.codehaus.groovy.ast.stmt.ReturnStatement;
import org.codehaus.groovy.ast.stmt.Statement;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.ErrorCollector;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.syntax.Types;

/**
 * Checks Groovy unit tests for field, property, and method accesses that bypass Java visibility
 * and rely on dynamic Groovy reflective fallback at runtime.
 */
public class TestFieldAccessCheck {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_MAGENTA = "\u001B[35m";

    /**
     * Category of a detected visibility bypass in Groovy tests.
     */
    public enum Category {
        /** Non-public field, getter, or setter accessed from a different package. */
        CROSS_PACKAGE("CROSS-PKG"),
        /** Private field accessed outside its declaring class. */
        PRIVATE_FIELD("PRIVATE"),
        /** Private or non-accessible field accessed via Groovy's {@code .@} operator. */
        DIRECT_FIELD("DIRECT .@"),
        /** Non-accessible method called from a test. */
        INACCESSIBLE_METHOD("METHOD");

        private final String label;

        Category(String label) {
            this.label = label;
        }

        /**
         * Returns the short display label for this category.
         *
         * @return display label
         */
        public String label() {
            return label;
        }
    }

    /**
     * A single detected visibility-bypass occurrence in a Groovy test file.
     *
     * @param testFile           project-relative path to the test file
     * @param line               1-based source line number
     * @param testPackage        package declared by the test file
     * @param targetClass        fully qualified name of the target production class
     * @param targetPackage      package of the target production class
     * @param memberName         field, property, or method name
     * @param accessKind         access kind ({@code READ}, {@code WRITE}, {@code DIRECT .@}, or {@code CALL})
     * @param category           finding category
     * @param declarationSummary human-readable declaration and visibility description
     * @param fixHint            concise suggested remediation
     * @param expressionText     AST text of the offending expression
     * @param sourceLine         trimmed source text at {@code line}
     */
    public record Finding(
            String testFile,
            int line,
            String testPackage,
            String targetClass,
            String targetPackage,
            String memberName,
            String accessKind,
            Category category,
            String declarationSummary,
            String fixHint,
            String expressionText,
            String sourceLine
    ) {
    }

    /**
     * Command-line options controlling scan scope and output formatting.
     *
     * @param projectDir        root directory of the project
     * @param targetFilter      optional test class, package, or file filter
     * @param targetClassFilter optional production class filter (simple name or FQN)
     * @param showSource        whether to print trimmed source lines for each hit
     * @param groupByFile       whether to group output by test file instead of target member
     * @param crossPackageOnly  whether to show only cross-package findings
     * @param includeMethods    whether to include inaccessible method call findings
     * @param useColor          whether to emit ANSI colors
     * @param showHelp          whether to print usage text and exit
     */
    public record Options(
            Path projectDir,
            String targetFilter,
            String targetClassFilter,
            boolean showSource,
            boolean groupByFile,
            boolean crossPackageOnly,
            boolean includeMethods,
            boolean useColor,
            boolean showHelp
    ) {
    }

    /**
     * Aggregated scan results across all analyzed Groovy test files.
     *
     * @param filesScanned        number of Groovy test files analyzed
     * @param classesLoaded       number of compiled bytecode classes indexed
     * @param expressionsChecked  number of property expressions inspected
     * @param expressionsResolved number of property expressions whose receiver type was resolved
     * @param fieldFindings       detected field/property visibility bypasses
     * @param methodFindings      detected inaccessible method calls
     */
    public record CheckResult(
            int filesScanned,
            int classesLoaded,
            int expressionsChecked,
            int expressionsResolved,
            List<Finding> fieldFindings,
            List<Finding> methodFindings
    ) {
    }

    private static final class FieldInfo {
        String owner;
        String pkg;
        String name;
        int access;
        String typeInternal;
        String elemTypeInternal;

        boolean isPublic() {
            return (access & Opcodes.ACC_PUBLIC) != 0;
        }

        boolean isProtected() {
            return (access & Opcodes.ACC_PROTECTED) != 0;
        }

        boolean isPrivate() {
            return (access & Opcodes.ACC_PRIVATE) != 0;
        }

        boolean isStatic() {
            return (access & Opcodes.ACC_STATIC) != 0;
        }

        boolean isFinal() {
            return (access & Opcodes.ACC_FINAL) != 0;
        }

        String visibilityLabel() {
            if (isPublic()) {
                return "public";
            }
            if (isProtected()) {
                return "protected";
            }
            if (isPrivate()) {
                return "private";
            }
            return "package-private";
        }

        String declarationModifiers() {
            var sb = new StringBuilder(visibilityLabel());
            if (isStatic()) {
                sb.append(" static");
            }
            if (isFinal()) {
                sb.append(" final");
            }
            return sb.toString();
        }
    }

    private static final class MethodInfo {
        String owner;
        String pkg;
        String name;
        int access;
        int paramCount;
        String returnInternal;
        String returnElemInternal;

        boolean isPublic() {
            return (access & Opcodes.ACC_PUBLIC) != 0;
        }

        boolean isProtected() {
            return (access & Opcodes.ACC_PROTECTED) != 0;
        }

        boolean isPrivate() {
            return (access & Opcodes.ACC_PRIVATE) != 0;
        }

        String visibilityLabel() {
            if (isPublic()) {
                return "public";
            }
            if (isProtected()) {
                return "protected";
            }
            if (isPrivate()) {
                return "private";
            }
            return "package-private";
        }
    }

    private static final class ClassInfo {
        String internalName;
        String pkg;
        String superInternal;
        final List<String> interfaces = new ArrayList<>();
        boolean isRecord;
        boolean isTestClass;
        final Map<String, FieldInfo> fields = new LinkedHashMap<>();
        final List<MethodInfo> methods = new ArrayList<>();
    }

    private final Map<String, ClassInfo> classes = new HashMap<>();
    private final List<Finding> fieldFindings = new ArrayList<>();
    private final List<Finding> methodFindings = new ArrayList<>();
    private int expressionsChecked;
    private int expressionsResolved;

    /**
     * Entry point for CLI execution.
     *
     * @param args command-line arguments
     * @throws IOException if reading project files fails
     */
    public static void main(String[] args) throws IOException {
        int code = execute(args, System.out, System.err);
        if (code != 0) {
            System.exit(code);
        }
    }

    /**
     * Parses arguments, runs the checker, writes pretty output, and returns a process exit code
     * ({@code 0} when clean or showing help, {@code 1} when issues are found, {@code 2} on CLI or setup errors).
     *
     * @param args command-line arguments
     * @param out  standard output stream
     * @param err  error output stream
     * @return exit status code
     * @throws IOException if an I/O error occurs while reading files
     */
    public static int execute(String[] args, PrintStream out, PrintStream err) throws IOException {
        Options options;
        try {
            options = parseArgs(args);
        } catch (IllegalArgumentException e) {
            err.println("Error: " + e.getMessage());
            err.println();
            err.print(usageText());
            return 2;
        }

        if (options.showHelp()) {
            out.print(usageText());
            return 0;
        }

        var checker = new TestFieldAccessCheck();
        CheckResult result;
        try {
            result = checker.run(options);
        } catch (IllegalStateException e) {
            err.println("Error: " + e.getMessage());
            return 2;
        }

        out.println(formatResult(result, options));
        boolean hasIssues = !result.fieldFindings().isEmpty()
                || (options.includeMethods() && !result.methodFindings().isEmpty());
        return hasIssues ? 1 : 0;
    }

    /**
     * Parses CLI arguments into an {@link Options} instance.
     *
     * @param args raw CLI arguments
     * @return parsed options
     * @throws IllegalArgumentException if an unknown flag or missing option value is encountered
     */
    public static Options parseArgs(String[] args) {
        Path projectDir = Paths.get(".").toAbsolutePath().normalize();
        String targetFilter = null;
        String targetClassFilter = null;
        boolean showSource = false;
        boolean groupByFile = false;
        boolean crossPackageOnly = false;
        boolean includeMethods = false;
        Boolean explicitColor = null;
        boolean showHelp = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "-h", "--help" -> showHelp = true;
                case "--src" -> showSource = true;
                case "--by-file" -> groupByFile = true;
                case "--cross-package-only" -> crossPackageOnly = true;
                case "--include-methods" -> includeMethods = true;
                case "--color" -> explicitColor = Boolean.TRUE;
                case "--no-color" -> explicitColor = Boolean.FALSE;
                case "--dir" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--dir requires a directory path");
                    }
                    projectDir = Paths.get(args[++i]).toAbsolutePath().normalize();
                }
                case "--target-class" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--target-class requires a class name");
                    }
                    targetClassFilter = args[++i];
                }
                default -> {
                    if (arg.startsWith("-")) {
                        throw new IllegalArgumentException("Unknown option: " + arg);
                    }
                    if (targetFilter != null) {
                        throw new IllegalArgumentException("Unexpected extra positional argument: " + arg);
                    }
                    targetFilter = arg;
                }
            }
        }

        boolean useColor = explicitColor != null ? explicitColor : System.console() != null;
        return new Options(
                projectDir,
                targetFilter,
                targetClassFilter,
                showSource,
                groupByFile,
                crossPackageOnly,
                includeMethods,
                useColor,
                showHelp
        );
    }

    /**
     * Returns formatted CLI usage text.
     *
     * @return usage instructions
     */
    public static String usageText() {
        return """
                Usage:
                  scripts/test_field_access_check.sh [target] [options]

                Arguments:
                  target                 Optional test class, package, or path filter
                                         (e.g. BGroupTest, io.velo.command, src/test/groovy/...)

                Options:
                  --target-class <name>  Filter findings by accessed production class (FQN or simple name)
                  --cross-package-only   Show only cross-package visibility bypasses
                  --include-methods      Also report inaccessible method calls
                  --by-file              Group findings by test file instead of target class/member
                  --src                  Show the trimmed Groovy source line for each hit
                  --dir <path>           Project root directory (default: current directory)
                  --compile              Compile main classes before running (bash wrapper option)
                  --color / --no-color   Force or disable ANSI color output
                  -h, --help             Show this help message

                Exit codes:
                  0  No violations found
                  1  One or more visibility bypasses found
                  2  Invalid arguments or missing compiled classes
                """;
    }

    /**
     * Scans compiled bytecode classes and Groovy test files under the configured project directory.
     *
     * @param options scan options
     * @return aggregated check result
     * @throws IOException if reading class or Groovy files fails
     */
    public CheckResult run(Options options) throws IOException {
        classes.clear();
        fieldFindings.clear();
        methodFindings.clear();
        expressionsChecked = 0;
        expressionsResolved = 0;

        Path root = options.projectDir().toAbsolutePath().normalize();
        scanClassDir(root.resolve("build/classes/java/main"), false);
        scanClassDir(root.resolve("build/classes/groovy/main"), false);
        scanClassDir(root.resolve("build/classes/groovy/test"), true);
        scanClassDir(root.resolve("build/classes/java/test"), true);

        if (classes.isEmpty()) {
            throw new IllegalStateException(
                    "No compiled classes found under " + root.resolve("build/classes")
                            + ". Run './gradlew :compileJava :compileGroovy' first.");
        }

        List<Path> testFiles = collectTestFiles(root, options.targetFilter());
        if (testFiles.isEmpty()) {
            throw new IllegalStateException(
                    options.targetFilter() == null
                            ? "No Groovy test files found under " + root
                            : "No Groovy test files matched target: " + options.targetFilter());
        }

        for (Path testFile : testFiles) {
            analyzeGroovyFile(root, testFile);
        }

        List<Finding> filteredFields = filterFindings(fieldFindings, options);
        List<Finding> filteredMethods = filterFindings(methodFindings, options);
        return new CheckResult(
                testFiles.size(),
                classes.size(),
                expressionsChecked,
                expressionsResolved,
                filteredFields,
                filteredMethods
        );
    }

    /**
     * Formats a {@link CheckResult} into a pretty, human-readable terminal report.
     *
     * @param result  scan result
     * @param options formatting options
     * @return formatted report string
     */
    public static String formatResult(CheckResult result, Options options) {
        boolean color = options.useColor();
        var sb = new StringBuilder();
        String divider = style("=".repeat(80), ANSI_CYAN, color);

        sb.append(divider).append('\n');
        sb.append(style(" Velo Groovy Test Field Access Check", ANSI_BOLD + ANSI_CYAN, color)).append('\n');
        sb.append(divider).append('\n');

        sb.append(String.format(" Files scanned : %d  |  Classes loaded : %d  |  Expressions : %d (%d resolved)%n",
                result.filesScanned(), result.classesLoaded(),
                result.expressionsChecked(), result.expressionsResolved()));

        List<Finding> fields = result.fieldFindings();
        List<Finding> methods = result.methodFindings();

        long crossCount = fields.stream().filter(f -> f.category() == Category.CROSS_PACKAGE).count();
        long privateCount = fields.stream().filter(f -> f.category() == Category.PRIVATE_FIELD).count();
        long directCount = fields.stream().filter(f -> f.category() == Category.DIRECT_FIELD).count();
        long distinctFields = fields.stream().map(f -> f.targetClass() + "#" + f.memberName()).distinct().count();
        long distinctMethods = methods.stream().map(f -> f.targetClass() + "#" + f.memberName()).distinct().count();

        String fieldCountText = fields.isEmpty()
                ? style("0", ANSI_GREEN + ANSI_BOLD, color)
                : style(fields.size() + " across " + distinctFields + plural(distinctFields, " field", " fields"),
                        ANSI_RED + ANSI_BOLD, color)
                        + String.format("  (%d cross-package, %d private, %d direct .@)",
                        crossCount, privateCount, directCount);
        sb.append(" Field issues  : ").append(fieldCountText).append('\n');

        if (options.includeMethods()) {
            String methodCountText = methods.isEmpty()
                    ? style("0", ANSI_GREEN + ANSI_BOLD, color)
                    : style(methods.size() + " across " + distinctMethods + plural(distinctMethods, " method", " methods"),
                            ANSI_YELLOW + ANSI_BOLD, color);
            sb.append(" Method issues : ").append(methodCountText).append('\n');
        } else if (!methods.isEmpty()) {
            sb.append(" Method issues : ")
                    .append(style(methods.size() + " across " + distinctMethods
                            + plural(distinctMethods, " method", " methods"), ANSI_DIM, color))
                    .append(style("  (pass --include-methods to inspect)", ANSI_DIM, color))
                    .append('\n');
        }

        if (options.targetFilter() != null || options.targetClassFilter() != null || options.crossPackageOnly()) {
            var activeFilters = new ArrayList<String>();
            if (options.targetFilter() != null) {
                activeFilters.add("target=" + options.targetFilter());
            }
            if (options.targetClassFilter() != null) {
                activeFilters.add("targetClass=" + options.targetClassFilter());
            }
            if (options.crossPackageOnly()) {
                activeFilters.add("cross-package-only");
            }
            sb.append(" Filters       : ").append(String.join(", ", activeFilters)).append('\n');
        }
        sb.append(divider).append('\n');

        List<Finding> displayed = new ArrayList<>(fields);
        if (options.includeMethods()) {
            displayed.addAll(methods);
        }

        if (displayed.isEmpty()) {
            sb.append('\n')
                    .append(style("[OK]", ANSI_GREEN + ANSI_BOLD, color))
                    .append(" No non-accessible field/property accesses found.");
            return sb.toString();
        }

        if (options.groupByFile()) {
            appendGroupedByFile(sb, displayed, options.showSource(), color);
        } else {
            appendGroupedByMember(sb, fields, "Field / Property Access Issues", options.showSource(), color);
            if (options.includeMethods() && !methods.isEmpty()) {
                appendGroupedByMember(sb, methods, "Inaccessible Method Call Issues", options.showSource(), color);
            }
        }

        appendCategorySummary(sb, displayed, color);
        return sb.toString();
    }

    private static void appendGroupedByMember(
            StringBuilder sb,
            List<Finding> findings,
            String sectionTitle,
            boolean showSource,
            boolean color
    ) {
        if (findings.isEmpty()) {
            return;
        }

        sb.append('\n')
                .append(style("## " + sectionTitle, ANSI_BOLD + ANSI_CYAN, color))
                .append('\n');

        Map<String, List<Finding>> grouped = new TreeMap<>();
        for (Finding f : findings) {
            String key = f.category().ordinal() + "|" + f.targetClass() + "#" + f.memberName();
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(f);
        }

        for (List<Finding> group : grouped.values()) {
            group.sort(Comparator.comparing(Finding::testFile).thenComparingInt(Finding::line));
            Finding sample = group.get(0);
            int maxLocLen = group.stream()
                    .mapToInt(f -> (f.testFile() + ":" + f.line()).length())
                    .max()
                    .orElse(20);

            sb.append('\n');
            sb.append(style("[!!]", ANSI_RED + ANSI_BOLD, color)).append(' ')
                    .append(style(sample.targetClass() + "#" + sample.memberName(), ANSI_BOLD, color))
                    .append("  ")
                    .append(formatBadge(sample.category(), color))
                    .append("  ")
                    .append(style("(" + group.size() + plural(group.size(), " hit", " hits") + ")", ANSI_DIM, color))
                    .append('\n');
            sb.append("     ").append(style("Decl :", ANSI_DIM, color)).append(' ')
                    .append(sample.declarationSummary()).append('\n');
            sb.append("     ").append(style("Fix  :", ANSI_DIM, color)).append(' ')
                    .append(style(sample.fixHint(), ANSI_GREEN, color)).append('\n');

            for (int i = 0; i < group.size(); i++) {
                Finding f = group.get(i);
                boolean isLast = i == group.size() - 1;
                String branch = style(isLast ? "└─ " : "├─ ", ANSI_DIM, color);
                String loc = padRight(f.testFile() + ":" + f.line(), maxLocLen);
                String kind = formatAccessKind(f.accessKind(), color);

                sb.append("     ").append(branch)
                        .append(loc).append("  ")
                        .append(kind).append("  ")
                        .append(f.expressionText())
                        .append('\n');

                if (showSource && f.sourceLine() != null && !f.sourceLine().isEmpty()) {
                    String cont = style(isLast ? "   " : "│  ", ANSI_DIM, color);
                    sb.append("     ").append(cont)
                            .append(style("↳ " + truncate(f.sourceLine(), 110), ANSI_DIM, color))
                            .append('\n');
                }
            }
        }
    }

    private static void appendGroupedByFile(
            StringBuilder sb,
            List<Finding> findings,
            boolean showSource,
            boolean color
    ) {
        Map<String, List<Finding>> byFile = new TreeMap<>();
        for (Finding f : findings) {
            byFile.computeIfAbsent(f.testFile(), k -> new ArrayList<>()).add(f);
        }

        for (Map.Entry<String, List<Finding>> entry : byFile.entrySet()) {
            List<Finding> group = entry.getValue();
            group.sort(Comparator.comparingInt(Finding::line).thenComparing(Finding::memberName));

            sb.append('\n');
            sb.append(style("[!!]", ANSI_RED + ANSI_BOLD, color)).append(' ')
                    .append(style(entry.getKey(), ANSI_BOLD, color))
                    .append("  ")
                    .append(style("(" + group.size() + plural(group.size(), " hit", " hits") + ")", ANSI_DIM, color))
                    .append('\n');

            for (int i = 0; i < group.size(); i++) {
                Finding f = group.get(i);
                boolean isLast = i == group.size() - 1;
                String branch = style(isLast ? "└─ " : "├─ ", ANSI_DIM, color);
                String lineStr = padRight("L" + f.line(), 6);

                sb.append("     ").append(branch)
                        .append(lineStr).append(' ')
                        .append(formatAccessKind(f.accessKind(), color)).append(' ')
                        .append(formatBadge(f.category(), color)).append("  ")
                        .append(f.targetClass()).append('#').append(f.memberName())
                        .append('\n');

                if (showSource && f.sourceLine() != null && !f.sourceLine().isEmpty()) {
                    String cont = style(isLast ? "   " : "│  ", ANSI_DIM, color);
                    sb.append("     ").append(cont)
                            .append(style("↳ " + truncate(f.sourceLine(), 110), ANSI_DIM, color))
                            .append('\n');
                }
            }
        }
    }

    private static void appendCategorySummary(StringBuilder sb, List<Finding> findings, boolean color) {
        Map<Category, Integer> hitCounts = new EnumMap<>(Category.class);
        Map<Category, Set<String>> memberSets = new EnumMap<>(Category.class);
        for (Finding f : findings) {
            hitCounts.merge(f.category(), 1, Integer::sum);
            memberSets.computeIfAbsent(f.category(), k -> new HashSet<>())
                    .add(f.targetClass() + "#" + f.memberName());
        }

        long totalMembers = findings.stream()
                .map(f -> f.targetClass() + "#" + f.memberName())
                .distinct()
                .count();

        sb.append('\n');
        sb.append(style("Summary by Category:", ANSI_BOLD, color)).append('\n');
        for (Category cat : Category.values()) {
            int hits = hitCounts.getOrDefault(cat, 0);
            if (hits == 0) {
                continue;
            }
            int members = memberSets.get(cat).size();
            String unit = cat == Category.INACCESSIBLE_METHOD
                    ? plural(members, " method", " methods")
                    : plural(members, " field", " fields");
            sb.append(String.format("  %-11s %4d %s (%d%s)%n",
                    cat.label(), hits, plural(hits, "hit ", "hits"), members, unit));
        }
        sb.append(style(String.format("  %-11s %4d %s (%d distinct)",
                "TOTAL", findings.size(), plural(findings.size(), "hit ", "hits"), totalMembers),
                ANSI_BOLD, color));
    }

    private static String formatBadge(Category category, boolean color) {
        String raw = "[" + category.label() + "]";
        return switch (category) {
            case CROSS_PACKAGE -> style(raw, ANSI_YELLOW + ANSI_BOLD, color);
            case PRIVATE_FIELD -> style(raw, ANSI_MAGENTA + ANSI_BOLD, color);
            case DIRECT_FIELD -> style(raw, ANSI_RED + ANSI_BOLD, color);
            case INACCESSIBLE_METHOD -> style(raw, ANSI_CYAN + ANSI_BOLD, color);
        };
    }

    private static String formatAccessKind(String accessKind, boolean color) {
        String padded = String.format("[%-5s]", accessKind);
        if ("WRITE".equals(accessKind)) {
            return style(padded, ANSI_RED, color);
        }
        if ("CALL".equals(accessKind)) {
            return style(padded, ANSI_CYAN, color);
        }
        return style(padded, ANSI_YELLOW, color);
    }

    private static String style(String text, String ansi, boolean color) {
        return color ? ansi + text + ANSI_RESET : text;
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }

    private static String plural(long n, String singular, String plural) {
        return n == 1 ? singular : plural;
    }

    private static String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }

    private static List<Finding> filterFindings(List<Finding> raw, Options options) {
        List<Finding> out = new ArrayList<>();
        for (Finding f : raw) {
            if (options.crossPackageOnly() && f.category() != Category.CROSS_PACKAGE) {
                continue;
            }
            if (options.targetClassFilter() != null && !matchesTargetClass(f.targetClass(), options.targetClassFilter())) {
                continue;
            }
            out.add(f);
        }
        return out;
    }

    private static boolean matchesTargetClass(String fqn, String filter) {
        if (fqn.equals(filter)) {
            return true;
        }
        int lastDot = fqn.lastIndexOf('.');
        String simple = lastDot >= 0 ? fqn.substring(lastDot + 1) : fqn;
        return simple.equals(filter) || simple.endsWith("$" + filter);
    }

    private static List<Path> collectTestFiles(Path root, String targetFilter) throws IOException {
        List<Path> all = new ArrayList<>();
        for (String dir : List.of("src/test/groovy", "dyn/test")) {
            Path p = root.resolve(dir);
            if (Files.exists(p)) {
                try (var stream = Files.walk(p)) {
                    stream.filter(f -> f.toString().endsWith(".groovy"))
                            .sorted()
                            .forEach(all::add);
                }
            }
        }
        if (targetFilter == null || targetFilter.isEmpty()) {
            return all;
        }

        String normFilter = targetFilter.replace('\\', '/');
        List<Path> matched = new ArrayList<>();
        for (Path f : all) {
            String rel = root.relativize(f).toString().replace('\\', '/');
            if (matchesTestTarget(rel, normFilter)) {
                matched.add(f);
            }
        }
        return matched;
    }

    private static boolean matchesTestTarget(String relPath, String filter) {
        if (relPath.equals(filter) || relPath.endsWith("/" + filter)) {
            return true;
        }
        String withoutRoot = relPath;
        if (withoutRoot.startsWith("src/test/groovy/")) {
            withoutRoot = withoutRoot.substring("src/test/groovy/".length());
        } else if (withoutRoot.startsWith("dyn/test/")) {
            withoutRoot = withoutRoot.substring("dyn/test/".length());
        }
        if (withoutRoot.equals(filter)) {
            return true;
        }
        String fqn = withoutRoot.endsWith(".groovy")
                ? withoutRoot.substring(0, withoutRoot.length() - ".groovy".length()).replace('/', '.')
                : withoutRoot.replace('/', '.');
        int lastDot = fqn.lastIndexOf('.');
        String simple = lastDot >= 0 ? fqn.substring(lastDot + 1) : fqn;
        String pkg = lastDot >= 0 ? fqn.substring(0, lastDot) : "";

        return fqn.equals(filter)
                || simple.equals(filter)
                || pkg.equals(filter)
                || pkg.startsWith(filter + ".");
    }

    private void scanClassDir(Path dir, boolean isTestClass) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            List<Path> classFiles = stream.filter(p -> p.toString().endsWith(".class")).toList();
            for (Path p : classFiles) {
                byte[] bytes = Files.readAllBytes(p);
                var cr = new ClassReader(bytes);
                var ci = new ClassInfo();
                ci.isTestClass = isTestClass;
                cr.accept(new ClassVisitor(Opcodes.ASM9) {
                    @Override
                    public void visit(int version, int access, String name, String signature,
                                      String superName, String[] interfaces) {
                        ci.internalName = name;
                        ci.pkg = pkgOf(name);
                        ci.superInternal = superName;
                        ci.isRecord = "java/lang/Record".equals(superName);
                        if (interfaces != null) {
                            ci.interfaces.addAll(Arrays.asList(interfaces));
                        }
                    }

                    @Override
                    public FieldVisitor visitField(int access, String name, String descriptor,
                                                   String signature, Object value) {
                        if ((access & Opcodes.ACC_SYNTHETIC) != 0) {
                            return null;
                        }
                        var fi = new FieldInfo();
                        fi.owner = ci.internalName;
                        fi.pkg = ci.pkg;
                        fi.name = name;
                        fi.access = access;
                        fi.typeInternal = typeDescToInternal(descriptor);
                        fi.elemTypeInternal = extractGenericElem(signature, descriptor);
                        ci.fields.put(name, fi);
                        return null;
                    }

                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor,
                                                     String signature, String[] exceptions) {
                        if ((access & Opcodes.ACC_SYNTHETIC) != 0 || (access & Opcodes.ACC_BRIDGE) != 0) {
                            return null;
                        }
                        var mi = new MethodInfo();
                        mi.owner = ci.internalName;
                        mi.pkg = ci.pkg;
                        mi.name = name;
                        mi.access = access;
                        mi.paramCount = Type.getArgumentTypes(descriptor).length;
                        Type ret = Type.getReturnType(descriptor);
                        if (ret.getSort() == Type.OBJECT) {
                            mi.returnInternal = ret.getInternalName();
                        } else if (ret.getSort() == Type.ARRAY && ret.getElementType().getSort() == Type.OBJECT) {
                            mi.returnInternal = "[" + ret.getElementType().getInternalName();
                        } else {
                            mi.returnInternal = ret.getDescriptor();
                        }
                        if (signature != null) {
                            int rp = signature.lastIndexOf(')');
                            if (rp >= 0) {
                                mi.returnElemInternal = extractGenericElem(signature.substring(rp + 1), ret.getDescriptor());
                            }
                        } else {
                            mi.returnElemInternal = extractGenericElem(null, ret.getDescriptor());
                        }
                        ci.methods.add(mi);
                        return null;
                    }
                }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
                classes.put(ci.internalName, ci);
            }
        }
    }

    private static String pkgOf(String internalName) {
        int idx = internalName.lastIndexOf('/');
        return idx < 0 ? "" : internalName.substring(0, idx).replace('/', '.');
    }

    private static String typeDescToInternal(String desc) {
        Type t = Type.getType(desc);
        if (t.getSort() == Type.OBJECT) {
            return t.getInternalName();
        }
        if (t.getSort() == Type.ARRAY && t.getElementType().getSort() == Type.OBJECT) {
            return "[" + t.getElementType().getInternalName();
        }
        return t.getDescriptor();
    }

    private static String extractGenericElem(String sig, String desc) {
        Type t = Type.getType(desc);
        if (t.getSort() == Type.ARRAY && t.getElementType().getSort() == Type.OBJECT) {
            return t.getElementType().getInternalName();
        }
        if (sig != null) {
            int lt = sig.indexOf('<');
            int gt = sig.lastIndexOf('>');
            if (lt >= 0 && gt > lt) {
                String inner = sig.substring(lt + 1, gt);
                int lastL = inner.lastIndexOf("Lio/velo/");
                if (lastL >= 0) {
                    int semi = inner.indexOf(';', lastL);
                    if (semi > lastL) {
                        String name = inner.substring(lastL + 1, semi);
                        int gen = name.indexOf('<');
                        if (gen > 0) {
                            name = name.substring(0, gen);
                        }
                        return name;
                    }
                }
            }
        }
        return null;
    }

    private List<ClassInfo> hierarchy(ClassInfo ci) {
        List<ClassInfo> res = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Deque<ClassInfo> q = new ArrayDeque<>();
        q.add(ci);
        while (!q.isEmpty()) {
            ClassInfo cur = q.poll();
            if (cur == null || !seen.add(cur.internalName)) {
                continue;
            }
            res.add(cur);
            if (cur.superInternal != null && classes.containsKey(cur.superInternal)) {
                q.add(classes.get(cur.superInternal));
            }
            for (String iface : cur.interfaces) {
                if (classes.containsKey(iface)) {
                    q.add(classes.get(iface));
                }
            }
        }
        return res;
    }

    private boolean isSubclassOf(String subInternal, String targetInternal) {
        if (subInternal == null || !classes.containsKey(subInternal)) {
            return false;
        }
        for (ClassInfo c : hierarchy(classes.get(subInternal))) {
            if (c.internalName.equals(targetInternal)) {
                return true;
            }
        }
        return false;
    }

    private boolean isMethodAccessible(MethodInfo m, String callerPkg, String callerClassInternal) {
        if (m.isPublic()) {
            return true;
        }
        if (m.isPrivate()) {
            return m.owner.equals(callerClassInternal);
        }
        if (m.pkg.equals(callerPkg)) {
            return true;
        }
        return m.isProtected() && isSubclassOf(callerClassInternal, m.owner);
    }

    private boolean isFieldAccessible(FieldInfo f, String callerPkg, String callerClassInternal) {
        if (f.isPublic()) {
            return true;
        }
        if (f.isPrivate()) {
            return f.owner.equals(callerClassInternal);
        }
        if (f.pkg.equals(callerPkg)) {
            return true;
        }
        return f.isProtected() && isSubclassOf(callerClassInternal, f.owner);
    }

    private static String cap(String prop) {
        if (prop == null || prop.isEmpty()) {
            return prop;
        }
        return Character.toUpperCase(prop.charAt(0)) + prop.substring(1);
    }

    private MethodInfo findGetter(ClassInfo ci, String prop, String callerPkg, String callerClassInternal) {
        String g1 = "get" + cap(prop);
        String g2 = "is" + cap(prop);
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.paramCount != 0) {
                    continue;
                }
                boolean match = m.name.equals(g1) || m.name.equals(g2) || (c.isRecord && m.name.equals(prop));
                if (match && isMethodAccessible(m, callerPkg, callerClassInternal)) {
                    return m;
                }
            }
        }
        return null;
    }

    private MethodInfo findSetter(ClassInfo ci, String prop, String callerPkg, String callerClassInternal) {
        String s1 = "set" + cap(prop);
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.paramCount == 1 && m.name.equals(s1) && isMethodAccessible(m, callerPkg, callerClassInternal)) {
                    return m;
                }
            }
        }
        return null;
    }

    private MethodInfo findAnyGetterIgnoreVis(ClassInfo ci, String prop) {
        String g1 = "get" + cap(prop);
        String g2 = "is" + cap(prop);
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.paramCount == 0
                        && (m.name.equals(g1) || m.name.equals(g2) || (c.isRecord && m.name.equals(prop)))) {
                    return m;
                }
            }
        }
        return null;
    }

    private MethodInfo findAnySetterIgnoreVis(ClassInfo ci, String prop) {
        String s1 = "set" + cap(prop);
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.paramCount == 1 && m.name.equals(s1)) {
                    return m;
                }
            }
        }
        return null;
    }

    private FieldInfo findField(ClassInfo ci, String prop) {
        for (ClassInfo c : hierarchy(ci)) {
            FieldInfo fi = c.fields.get(prop);
            if (fi != null) {
                return fi;
            }
        }
        return null;
    }

    private MethodInfo findMethod(ClassInfo ci, String name, int argCount) {
        MethodInfo fallback = null;
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.name.equals(name)) {
                    if (m.paramCount == argCount) {
                        return m;
                    }
                    if (fallback == null) {
                        fallback = m;
                    }
                }
            }
        }
        return fallback;
    }

    private List<MethodInfo> findAllMethodsByName(ClassInfo ci, String name) {
        List<MethodInfo> res = new ArrayList<>();
        for (ClassInfo c : hierarchy(ci)) {
            for (MethodInfo m : c.methods) {
                if (m.name.equals(name)) {
                    res.add(m);
                }
            }
        }
        return res;
    }

    private final class TypeResolver {
        String filePkg;
        String currentOuterInternal;
        Map<String, String> imports = new HashMap<>();
        List<String> starPkgs = new ArrayList<>();
        final Map<String, String> varTypes = new HashMap<>();
        final Map<String, String> varElemTypes = new HashMap<>();
        Map<String, String> localMethodReturns = new HashMap<>();
        Map<String, Map<String, String>> localMethodMapKeys = new HashMap<>();

        String resolveTypeName(String rawName) {
            if (rawName == null) {
                return null;
            }
            String n = rawName.replace('.', '/');
            if (classes.containsKey(n)) {
                return n;
            }
            if (rawName.contains(".")) {
                String[] parts = rawName.split("\\.");
                String outer = resolveSimple(parts[0]);
                if (outer != null) {
                    var cand = new StringBuilder(outer);
                    for (int i = 1; i < parts.length; i++) {
                        cand.append('$').append(parts[i]);
                    }
                    if (classes.containsKey(cand.toString())) {
                        return cand.toString();
                    }
                }
            }
            return resolveSimple(rawName);
        }

        String resolveSimple(String simple) {
            if (imports.containsKey(simple)) {
                return imports.get(simple);
            }
            if (currentOuterInternal != null) {
                String inner = currentOuterInternal + "$" + simple;
                if (classes.containsKey(inner)) {
                    return inner;
                }
            }
            String samePkg = (filePkg.isEmpty() ? "" : filePkg.replace('.', '/') + "/") + simple;
            if (classes.containsKey(samePkg)) {
                return samePkg;
            }
            for (String sp : starPkgs) {
                String cand = sp.replace('.', '/') + "/" + simple;
                if (classes.containsKey(cand)) {
                    return cand;
                }
                String candInner = sp.replace('.', '/') + "$" + simple;
                if (classes.containsKey(candInner)) {
                    return candInner;
                }
            }
            return null;
        }

        String inferElemType(Expression expr) {
            if (expr instanceof VariableExpression ve) {
                return varElemTypes.get(ve.getName());
            }
            if (expr instanceof MethodCallExpression mce) {
                String recvType = inferExprType(mce.getObjectExpression());
                if (recvType != null && classes.containsKey(recvType)) {
                    int argc = mce.getArguments() instanceof ArgumentListExpression ale
                            ? ale.getExpressions().size() : 0;
                    MethodInfo mi = findMethod(classes.get(recvType), mce.getMethodAsString(), argc);
                    if (mi != null) {
                        return mi.returnElemInternal;
                    }
                }
            }
            if (expr instanceof PropertyExpression pe) {
                String recvType = inferExprType(pe.getObjectExpression());
                String prop = pe.getPropertyAsString();
                if (recvType != null && prop != null && classes.containsKey(recvType)) {
                    ClassInfo ci = classes.get(recvType);
                    MethodInfo g = findAnyGetterIgnoreVis(ci, prop);
                    if (g != null && g.returnElemInternal != null) {
                        return g.returnElemInternal;
                    }
                    FieldInfo fi = findField(ci, prop);
                    if (fi != null && fi.elemTypeInternal != null) {
                        return fi.elemTypeInternal;
                    }
                }
            }
            return null;
        }

        String inferExprType(Expression expr) {
            if (expr == null) {
                return null;
            }
            if (expr instanceof VariableExpression ve) {
                String name = ve.getName();
                if ("this".equals(name)) {
                    return currentOuterInternal;
                }
                if (varTypes.containsKey(name)) {
                    return varTypes.get(name);
                }
                return resolveTypeName(name);
            }
            if (expr instanceof ClassExpression ce) {
                return resolveTypeName(ce.getType().getName());
            }
            if (expr instanceof ConstructorCallExpression cce) {
                return resolveTypeName(cce.getType().getName());
            }
            if (expr instanceof CastExpression ce) {
                return resolveTypeName(ce.getType().getName());
            }
            if (expr instanceof BinaryExpression be && be.getOperation().getType() == Types.LEFT_SQUARE_BRACKET) {
                String leftType = inferExprType(be.getLeftExpression());
                if (leftType != null && leftType.startsWith("[")) {
                    return leftType.substring(1);
                }
                String elem = inferElemType(be.getLeftExpression());
                if (elem != null && classes.containsKey(elem)) {
                    return elem;
                }
                return null;
            }
            if (expr instanceof MethodCallExpression mce) {
                Expression obj = mce.getObjectExpression();
                String mname = mce.getMethodAsString();
                if (obj instanceof VariableExpression ve && "this".equals(ve.getName())
                        && localMethodReturns.containsKey(mname)) {
                    return localMethodReturns.get(mname);
                }
                if ("get".equals(mname) || "getFirst".equals(mname) || "getLast".equals(mname)
                        || "first".equals(mname) || "last".equals(mname)) {
                    String elem = inferElemType(obj);
                    if (elem != null && classes.containsKey(elem)) {
                        return elem;
                    }
                }
                String recvType = inferExprType(obj);
                if (recvType != null && classes.containsKey(recvType)) {
                    int argc = mce.getArguments() instanceof ArgumentListExpression ale
                            ? ale.getExpressions().size() : 0;
                    MethodInfo mi = findMethod(classes.get(recvType), mname, argc);
                    if (mi != null && (classes.containsKey(mi.returnInternal)
                            || (mi.returnInternal != null && mi.returnInternal.startsWith("[")))) {
                        return mi.returnInternal;
                    }
                }
                return null;
            }
            if (expr instanceof PropertyExpression pe) {
                String asClass = resolveTypeName(pe.getText());
                if (asClass != null) {
                    return asClass;
                }
                if (pe.getObjectExpression() instanceof VariableExpression ve && varTypes.containsKey(ve.getName())) {
                    String vt = varTypes.get(ve.getName());
                    if (vt.startsWith("MAP:")) {
                        Map<String, String> m = localMethodMapKeys.get(vt.substring(4));
                        if (m != null && m.containsKey(pe.getPropertyAsString())) {
                            return m.get(pe.getPropertyAsString());
                        }
                    }
                }
                String recvType = inferExprType(pe.getObjectExpression());
                String prop = pe.getPropertyAsString();
                if (recvType != null && prop != null && classes.containsKey(recvType)) {
                    ClassInfo ci = classes.get(recvType);
                    String inner = recvType + "$" + prop;
                    if (classes.containsKey(inner)) {
                        return inner;
                    }
                    MethodInfo g = findAnyGetterIgnoreVis(ci, prop);
                    if (g != null && (classes.containsKey(g.returnInternal)
                            || (g.returnInternal != null && g.returnInternal.startsWith("[")))) {
                        return g.returnInternal;
                    }
                    FieldInfo fi = findField(ci, prop);
                    if (fi != null && (classes.containsKey(fi.typeInternal)
                            || (fi.typeInternal != null && fi.typeInternal.startsWith("[")))) {
                        return fi.typeInternal;
                    }
                }
            }
            return null;
        }
    }

    private void checkPropAccess(
            String relFile,
            List<String> sourceLines,
            String testPkg,
            String callerClassInternal,
            TypeResolver tr,
            PropertyExpression pe,
            boolean isWrite,
            boolean isDirectField
    ) {
        String prop = pe.getPropertyAsString();
        if (prop == null) {
            return;
        }
        Expression objExpr = pe.getObjectExpression();
        if (objExpr instanceof VariableExpression ve
                && ("this".equals(ve.getName()) || "super".equals(ve.getName()))) {
            return;
        }
        if ("class".equals(prop) || "length".equals(prop) || "bytes".equals(prop)) {
            return;
        }

        expressionsChecked++;
        String recvType = tr.inferExprType(objExpr);
        if (recvType != null && recvType.startsWith("MAP:")) {
            expressionsResolved++;
            return;
        }
        if (recvType == null || !classes.containsKey(recvType)) {
            return;
        }
        expressionsResolved++;

        if (classes.containsKey(recvType + "$" + prop)) {
            return;
        }
        ClassInfo ci = classes.get(recvType);
        if (!ci.internalName.startsWith("io/velo/")) {
            return;
        }

        FieldInfo fi = findField(ci, prop);
        if (fi != null && classes.containsKey(fi.owner) && classes.get(fi.owner).isTestClass) {
            return;
        }
        String srcLine = lineAt(sourceLines, pe.getLineNumber());

        if (fi == null) {
            if (ci.isTestClass) {
                return;
            }
            if (isWrite) {
                if (findSetter(ci, prop, testPkg, callerClassInternal) == null) {
                    MethodInfo anySetter = findAnySetterIgnoreVis(ci, prop);
                    if (anySetter != null) {
                        Category cat = anySetter.pkg.equals(testPkg) ? Category.PRIVATE_FIELD : Category.CROSS_PACKAGE;
                        fieldFindings.add(new Finding(
                                relFile, pe.getLineNumber(), testPkg,
                                anySetter.owner.replace('/', '.'), anySetter.pkg,
                                prop, "WRITE", cat,
                                anySetter.visibilityLabel() + " setter " + anySetter.name + "() in " + anySetter.pkg,
                                "Make " + anySetter.name + "() public",
                                pe.getText(), srcLine
                        ));
                    }
                }
            } else if (findGetter(ci, prop, testPkg, callerClassInternal) == null) {
                MethodInfo anyGetter = findAnyGetterIgnoreVis(ci, prop);
                if (anyGetter != null) {
                    Category cat = anyGetter.pkg.equals(testPkg) ? Category.PRIVATE_FIELD : Category.CROSS_PACKAGE;
                    fieldFindings.add(new Finding(
                            relFile, pe.getLineNumber(), testPkg,
                            anyGetter.owner.replace('/', '.'), anyGetter.pkg,
                            prop, "READ", cat,
                            anyGetter.visibilityLabel() + " getter " + anyGetter.name + "() in " + anyGetter.pkg,
                            "Make " + anyGetter.name + "() public",
                            pe.getText(), srcLine
                    ));
                }
            }
            return;
        }

        boolean fieldDirectlyAccessible = isFieldAccessible(fi, testPkg, callerClassInternal);
        boolean samePkg = fi.pkg.equals(testPkg);

        if (isDirectField) {
            if (!fieldDirectlyAccessible) {
                fieldFindings.add(new Finding(
                        relFile, pe.getLineNumber(), testPkg,
                        fi.owner.replace('/', '.'), fi.pkg,
                        prop, isWrite ? "WRITE" : "READ", Category.DIRECT_FIELD,
                        fi.declarationModifiers() + " field in " + fi.pkg + " (accessed via .@)",
                        "Add @TestOnly getter/setter instead of .@ field bypass",
                        pe.getText(), srcLine
                ));
            }
            return;
        }

        if (isWrite) {
            if (findSetter(ci, prop, testPkg, callerClassInternal) != null) {
                return;
            }
            if (fieldDirectlyAccessible && !fi.isFinal()) {
                return;
            }
            MethodInfo inaccSetter = findAnySetterIgnoreVis(ci, prop);
            Category cat = samePkg ? Category.PRIVATE_FIELD : Category.CROSS_PACKAGE;
            String decl = fi.declarationModifiers() + " field in " + fi.pkg
                    + (inaccSetter != null
                    ? " (" + inaccSetter.visibilityLabel() + " " + inaccSetter.name + "() not accessible)"
                    : " (no accessible setter)");
            String fix = inaccSetter != null
                    ? "Make existing " + inaccSetter.name + "() public"
                    : (samePkg ? "Add @TestOnly setter or widen visibility" : "Add @TestOnly public setter");
            fieldFindings.add(new Finding(
                    relFile, pe.getLineNumber(), testPkg,
                    fi.owner.replace('/', '.'), fi.pkg,
                    prop, "WRITE", cat, decl, fix, pe.getText(), srcLine
            ));
        } else {
            if (findGetter(ci, prop, testPkg, callerClassInternal) != null) {
                return;
            }
            if (fieldDirectlyAccessible) {
                return;
            }
            MethodInfo inaccGetter = findAnyGetterIgnoreVis(ci, prop);
            Category cat = samePkg ? Category.PRIVATE_FIELD : Category.CROSS_PACKAGE;
            String decl = fi.declarationModifiers() + " field in " + fi.pkg
                    + (inaccGetter != null
                    ? " (" + inaccGetter.visibilityLabel() + " " + inaccGetter.name + "() not accessible)"
                    : " (no accessible getter)");
            String fix = inaccGetter != null
                    ? "Make existing " + inaccGetter.name + "() public"
                    : (samePkg ? "Add @TestOnly getter or widen visibility" : "Add @TestOnly public getter");
            fieldFindings.add(new Finding(
                    relFile, pe.getLineNumber(), testPkg,
                    fi.owner.replace('/', '.'), fi.pkg,
                    prop, "READ", cat, decl, fix, pe.getText(), srcLine
            ));
        }
    }

    private void checkMethodAccess(
            String relFile,
            List<String> sourceLines,
            String testPkg,
            String callerClassInternal,
            TypeResolver tr,
            MethodCallExpression mce
    ) {
        String mname = mce.getMethodAsString();
        if (mname == null) {
            return;
        }
        Expression objExpr = mce.getObjectExpression();
        if (objExpr instanceof VariableExpression ve
                && ("this".equals(ve.getName()) || "super".equals(ve.getName()))) {
            return;
        }
        String recvType = tr.inferExprType(objExpr);
        if (recvType == null || recvType.startsWith("MAP:") || !classes.containsKey(recvType)) {
            return;
        }
        ClassInfo ci = classes.get(recvType);
        if (!ci.internalName.startsWith("io/velo/") || ci.isTestClass) {
            return;
        }

        List<MethodInfo> matches = findAllMethodsByName(ci, mname);
        if (matches.isEmpty()) {
            return;
        }
        int argc = mce.getArguments() instanceof ArgumentListExpression ale ? ale.getExpressions().size() : -1;
        List<MethodInfo> exactMatches = new ArrayList<>();
        for (MethodInfo m : matches) {
            if (argc < 0 || m.paramCount == argc) {
                exactMatches.add(m);
            }
        }
        List<MethodInfo> toCheck = exactMatches.isEmpty() ? matches : exactMatches;
        for (MethodInfo m : toCheck) {
            if (isMethodAccessible(m, testPkg, callerClassInternal)) {
                return;
            }
        }
        MethodInfo first = toCheck.get(0);
        if (classes.containsKey(first.owner) && classes.get(first.owner).isTestClass) {
            return;
        }
        String fix = first.pkg.equals(testPkg)
                ? "Widen visibility or test via public API"
                : "Make @TestOnly public or test via public API";
        methodFindings.add(new Finding(
                relFile, mce.getLineNumber(), testPkg,
                first.owner.replace('/', '.'), first.pkg,
                mname + "(" + first.paramCount + " args)", "CALL", Category.INACCESSIBLE_METHOD,
                first.visibilityLabel() + " method in " + first.pkg,
                fix, mce.getText(), lineAt(sourceLines, mce.getLineNumber())
        ));
    }

    private static String lineAt(List<String> lines, int oneBasedLine) {
        if (oneBasedLine < 1 || oneBasedLine > lines.size()) {
            return "";
        }
        return lines.get(oneBasedLine - 1).trim();
    }

    private void analyzeGroovyFile(Path root, Path groovyFile) throws IOException {
        String relFile = root.relativize(groovyFile).toString().replace('\\', '/');
        List<String> sourceLines = Files.readAllLines(groovyFile, StandardCharsets.UTF_8);

        var cfg = new CompilerConfiguration();
        var su = new SourceUnit(groovyFile.toFile(), cfg, null, new ErrorCollector(cfg));
        su.parse();
        su.completePhase();
        su.convert();
        ModuleNode mn = su.getAST();
        if (mn == null) {
            return;
        }

        String testPkg = mn.getPackageName();
        if (testPkg == null) {
            testPkg = "";
        } else if (testPkg.endsWith(".")) {
            testPkg = testPkg.substring(0, testPkg.length() - 1);
        }
        final String finalTestPkg = testPkg;

        String outerInternal = null;
        for (ClassNode cn : mn.getClasses()) {
            if (!(cn instanceof InnerClassNode)) {
                outerInternal = cn.getName().replace('.', '/');
                break;
            }
        }

        var baseTr = new TypeResolver();
        baseTr.filePkg = finalTestPkg;
        baseTr.currentOuterInternal = outerInternal;
        for (ImportNode imp : mn.getImports()) {
            baseTr.imports.put(imp.getAlias(), imp.getClassName().replace('.', '/'));
        }
        for (ImportNode imp : mn.getStarImports()) {
            String p = imp.getPackageName();
            if (p != null && p.endsWith(".")) {
                p = p.substring(0, p.length() - 1);
            }
            if (p != null) {
                baseTr.starPkgs.add(p);
            }
        }
        for (ImportNode imp : mn.getStaticImports().values()) {
            baseTr.imports.put(imp.getAlias(), imp.getClassName().replace('.', '/'));
        }
        for (ImportNode imp : mn.getStaticStarImports().values()) {
            baseTr.starPkgs.add(imp.getClassName());
        }

        for (ClassNode cn : mn.getClasses()) {
            final String callerClassInternal = cn.getName().replace('.', '/');
            Map<String, String> classVarTypes = new HashMap<>();
            for (FieldNode fn : cn.getFields()) {
                String t = baseTr.resolveTypeName(fn.getType().getName());
                if (t == null && fn.getInitialExpression() != null) {
                    t = baseTr.inferExprType(fn.getInitialExpression());
                }
                if (t != null) {
                    classVarTypes.put(fn.getName(), t);
                }
            }
            scanHelperMethods(cn, baseTr, classVarTypes);

            for (MethodNode m : cn.getMethods()) {
                var tr = new TypeResolver();
                tr.filePkg = baseTr.filePkg;
                tr.currentOuterInternal = baseTr.currentOuterInternal;
                tr.imports = baseTr.imports;
                tr.starPkgs = baseTr.starPkgs;
                tr.localMethodReturns = baseTr.localMethodReturns;
                tr.localMethodMapKeys = baseTr.localMethodMapKeys;
                tr.varTypes.putAll(classVarTypes);
                for (Parameter p : m.getParameters()) {
                    String t = tr.resolveTypeName(p.getType().getName());
                    if (t != null) {
                        tr.varTypes.put(p.getName(), t);
                    }
                }
                Set<PropertyExpression> writeTargets = Collections.newSetFromMap(new IdentityHashMap<>());

                CodeVisitorSupport visitor = new ClassCodeVisitorSupport() {
                    @Override
                    protected SourceUnit getSourceUnit() {
                        return su;
                    }

                    @Override
                    public void visitForLoop(ForStatement forLoop) {
                        Parameter p = forLoop.getVariable();
                        if (p != null) {
                            String t = tr.resolveTypeName(p.getType().getName());
                            if (t == null || "java/lang/Object".equals(t)) {
                                String colType = tr.inferExprType(forLoop.getCollectionExpression());
                                if (colType != null && colType.startsWith("[")) {
                                    t = colType.substring(1);
                                } else {
                                    t = tr.inferElemType(forLoop.getCollectionExpression());
                                }
                            }
                            if (t != null) {
                                tr.varTypes.put(p.getName(), t);
                            }
                        }
                        super.visitForLoop(forLoop);
                    }

                    @Override
                    public void visitDeclarationExpression(DeclarationExpression expression) {
                        if (expression.getRightExpression() != null) {
                            expression.getRightExpression().visit(this);
                        }
                        if (!expression.isMultipleAssignmentDeclaration()) {
                            VariableExpression ve = expression.getVariableExpression();
                            String t = tr.resolveTypeName(ve.getType().getName());
                            if (t == null || "java/lang/Object".equals(t)) {
                                t = tr.inferExprType(expression.getRightExpression());
                            }
                            if (t != null) {
                                tr.varTypes.put(ve.getName(), t);
                            }
                            String et = tr.inferElemType(expression.getRightExpression());
                            if (et != null) {
                                tr.varElemTypes.put(ve.getName(), et);
                            }
                        }
                    }

                    @Override
                    public void visitBinaryExpression(BinaryExpression expression) {
                        int op = expression.getOperation().getType();
                        boolean isAssign = op == Types.ASSIGN || op == Types.PLUS_EQUAL || op == Types.MINUS_EQUAL
                                || op == Types.MULTIPLY_EQUAL || op == Types.DIVIDE_EQUAL;
                        if (isAssign) {
                            Expression left = expression.getLeftExpression();
                            Expression right = expression.getRightExpression();
                            right.visit(this);
                            if (left instanceof VariableExpression ve) {
                                if (op == Types.ASSIGN) {
                                    String rt = tr.inferExprType(right);
                                    if (rt != null) {
                                        tr.varTypes.put(ve.getName(), rt);
                                    }
                                    String et = tr.inferElemType(right);
                                    if (et != null) {
                                        tr.varElemTypes.put(ve.getName(), et);
                                    }
                                }
                                return;
                            }
                            if (left instanceof PropertyExpression pe) {
                                writeTargets.add(pe);
                                pe.getObjectExpression().visit(this);
                                boolean isDirect = pe instanceof AttributeExpression;
                                checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, true, isDirect);
                                if (op != Types.ASSIGN) {
                                    checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, false, isDirect);
                                }
                                return;
                            }
                        }
                        super.visitBinaryExpression(expression);
                    }

                    @Override
                    public void visitPostfixExpression(PostfixExpression expression) {
                        if (expression.getExpression() instanceof PropertyExpression pe) {
                            writeTargets.add(pe);
                            pe.getObjectExpression().visit(this);
                            boolean isDirect = pe instanceof AttributeExpression;
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, true, isDirect);
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, false, isDirect);
                            return;
                        }
                        super.visitPostfixExpression(expression);
                    }

                    @Override
                    public void visitPrefixExpression(PrefixExpression expression) {
                        int op = expression.getOperation().getType();
                        if ((op == Types.PLUS_PLUS || op == Types.MINUS_MINUS)
                                && expression.getExpression() instanceof PropertyExpression pe) {
                            writeTargets.add(pe);
                            pe.getObjectExpression().visit(this);
                            boolean isDirect = pe instanceof AttributeExpression;
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, true, isDirect);
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, pe, false, isDirect);
                            return;
                        }
                        super.visitPrefixExpression(expression);
                    }

                    @Override
                    public void visitPropertyExpression(PropertyExpression expression) {
                        super.visitPropertyExpression(expression);
                        if (!writeTargets.contains(expression)) {
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, expression, false, false);
                        }
                    }

                    @Override
                    public void visitAttributeExpression(AttributeExpression expression) {
                        super.visitAttributeExpression(expression);
                        if (!writeTargets.contains(expression)) {
                            checkPropAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, expression, false, true);
                        }
                    }

                    @Override
                    public void visitMethodCallExpression(MethodCallExpression call) {
                        String mname = call.getMethodAsString();
                        String prevIt = tr.varTypes.get("it");
                        if ("each".equals(mname) || "collect".equals(mname) || "find".equals(mname)
                                || "findAll".equals(mname) || "any".equals(mname) || "every".equals(mname)) {
                            String elem = tr.inferElemType(call.getObjectExpression());
                            if (elem == null) {
                                String rt = tr.inferExprType(call.getObjectExpression());
                                if (rt != null && rt.startsWith("[")) {
                                    elem = rt.substring(1);
                                }
                            }
                            if (elem != null) {
                                tr.varTypes.put("it", elem);
                            }
                        }
                        super.visitMethodCallExpression(call);
                        if (prevIt != null) {
                            tr.varTypes.put("it", prevIt);
                        } else {
                            tr.varTypes.remove("it");
                        }
                        checkMethodAccess(relFile, sourceLines, finalTestPkg, callerClassInternal, tr, call);
                    }

                    @Override
                    public void visitClosureExpression(ClosureExpression expression) {
                        if (expression.getParameters() != null) {
                            for (Parameter p : expression.getParameters()) {
                                String t = tr.resolveTypeName(p.getType().getName());
                                if (t != null) {
                                    tr.varTypes.put(p.getName(), t);
                                }
                            }
                        }
                        super.visitClosureExpression(expression);
                    }
                };
                if (m.getCode() != null) {
                    m.getCode().visit(visitor);
                }
            }
        }
    }

    private void scanHelperMethods(ClassNode cn, TypeResolver baseTr, Map<String, String> classVarTypes) {
        for (MethodNode m : cn.getMethods()) {
            String rt = baseTr.resolveTypeName(m.getReturnType().getName());
            if (rt != null) {
                baseTr.localMethodReturns.put(m.getName(), rt);
                continue;
            }
            if (!(m.getCode() instanceof BlockStatement bs) || bs.getStatements().isEmpty()) {
                continue;
            }
            Statement last = bs.getStatements().get(bs.getStatements().size() - 1);
            Expression retExpr = null;
            if (last instanceof ReturnStatement rs) {
                retExpr = rs.getExpression();
            } else if (last instanceof ExpressionStatement es) {
                retExpr = es.getExpression();
            }
            if (!(retExpr instanceof MapExpression me)) {
                continue;
            }
            Map<String, String> keyTypes = new HashMap<>();
            var htr = new TypeResolver();
            htr.filePkg = baseTr.filePkg;
            htr.currentOuterInternal = baseTr.currentOuterInternal;
            htr.imports = baseTr.imports;
            htr.starPkgs = baseTr.starPkgs;
            htr.varTypes.putAll(classVarTypes);
            for (Statement st : bs.getStatements()) {
                if (st instanceof ExpressionStatement es2
                        && es2.getExpression() instanceof DeclarationExpression de
                        && !de.isMultipleAssignmentDeclaration()) {
                    String vt = htr.resolveTypeName(de.getVariableExpression().getType().getName());
                    if (vt == null || "java/lang/Object".equals(vt)) {
                        vt = htr.inferExprType(de.getRightExpression());
                    }
                    if (vt != null) {
                        htr.varTypes.put(de.getVariableExpression().getName(), vt);
                    }
                }
            }
            for (MapEntryExpression mee : me.getMapEntryExpressions()) {
                if (mee.getKeyExpression() instanceof ConstantExpression ce
                        && ce.getValue() instanceof String k) {
                    String vt = htr.inferExprType(mee.getValueExpression());
                    if (vt != null) {
                        keyTypes.put(k, vt);
                    }
                }
            }
            if (!keyTypes.isEmpty()) {
                baseTr.localMethodReturns.put(m.getName(), "MAP:" + m.getName());
                baseTr.localMethodMapKeys.put(m.getName(), keyTypes);
            }
        }
    }
}
