<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Closure;
use Flow\Telemetry\Attributes;

use function dirname;
use function file_put_contents;
use function getmypid;
use function is_dir;
use function is_file;
use function is_readable;
use function mkdir;
use function rename;
use function sha1;
use function sys_get_temp_dir;
use function unlink;

/**
 * Builds the closure that an {@see AttributeFilter} uses to evaluate its matcher.
 */
final class CompiledMatcher
{
    /**
     * Build the closure that evaluates the matcher against a single attribute set.
     *
     * The closure reports the raw match only - polarity ({@see AttributeFilter::$exclude})
     * and combination across multiple sources are applied by {@see AttributeFilter},
     * so the compiled expression (and its cache file) is independent of both.
     *
     * @param int $cacheDirPermissions mode for the cache directory when it is created (octal, subject to umask)
     *
     * @return Closure(Attributes): bool true when the matcher matches
     */
    public static function build(Matcher $matcher, ?string $cacheDir, int $cacheDirPermissions = 0o700): Closure
    {
        try {
            if (!$matcher instanceof CompilableMatcher) {
                throw new NotCompilable('root matcher is not compilable');
            }

            $expression = $matcher->compile(new Compilation('$a', '$__v'));
        } catch (NotCompilable) {
            return self::interpret($matcher);
        }

        $file = ($cacheDir ?? sys_get_temp_dir()) . '/flow_telemetry_filter_' . sha1($expression) . '.php';

        if (is_file($file) && is_readable($file)) {
            $loaded = self::tryRequire($file);

            if ($loaded !== null) {
                return $loaded;
            }
        }

        $directory = dirname($file);

        if (!is_dir($directory)) {
            // Defaults to 0700: the cache holds executable PHP, so it must not be
            // writable (or readable) by other users on the host.
            @mkdir($directory, $cacheDirPermissions, true);
        }

        $tmp = $file . '.' . (string) getmypid() . '.tmp';

        if (@file_put_contents($tmp, self::source($expression)) !== false && @rename($tmp, $file)) {
            $loaded = self::tryRequire($file);

            if ($loaded !== null) {
                return $loaded;
            }
        }

        @unlink($tmp);

        return self::interpret($matcher);
    }

    private static function source(string $expression): string
    {
        // Bind the raw value map once so single-segment lookups index an array
        // instead of calling Attributes::get() per leaf.
        return (
            '<?php return static function (\Flow\Telemetry\Attributes $a): bool { $__v = $a->all(); return '
            . $expression
            . '; };'
            . "\n"
        );
    }

    /**
     * @return null|Closure(Attributes): bool
     */
    private static function tryRequire(string $file): ?Closure
    {
        /** @var mixed $loaded */
        $loaded = require $file;

        if ($loaded instanceof Closure) {
            /** @var Closure(Attributes): bool $loaded */
            return $loaded;
        }

        return null;
    }

    /**
     * @return Closure(Attributes): bool
     */
    private static function interpret(Matcher $matcher): Closure
    {
        return static fn(Attributes $attributes): bool => $matcher->matches($attributes);
    }
}
