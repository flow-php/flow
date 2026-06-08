<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use Closure;
use Flow\Telemetry\Attributes;
use InvalidArgumentException;

use function array_values;
use function sprintf;

/**
 * Decides whether a telemetry signal should be dropped based on its attributes.
 *
 * Evaluates a single root {@see Matcher} (compose them with {@see All}, {@see Any}
 * and {@see Not}) against one or more {@see AttributeSource}s. The matcher is run
 * independently against each configured source and the results are OR-combined: a
 * signal "matches" when the matcher matches in ANY of the sources. The polarity
 * ({@see $exclude}) is then applied once to that combined result - when the matcher
 * matches, the signal is dropped (exclude, the default) or kept (when exclude is
 * false, i.e. keep ONLY matching signals).
 *
 * The whole decision is baked into a single cached closure on construction: the raw
 * matcher is compiled once (see {@see CompiledMatcher}) and wrapped with the
 * per-source OR and the polarity. Processors fetch it via {@see self::dropFunction()}
 * and invoke it directly with one {@see Attributes} per source, avoiding a
 * method-call layer on the hot path.
 */
final readonly class AttributeFilter
{
    /**
     * @var Closure(Attributes ...): bool
     */
    private Closure $dropFunction;

    /**
     * @var non-empty-list<AttributeSource>
     */
    private array $sources;

    /**
     * @param bool $exclude when true (default) a match drops the signal; when false only matching signals are kept
     * @param list<AttributeSource> $sources attribute sets to inspect; the matcher is OR-combined across them
     *                                       (defaults to the signal's own attributes)
     * @param null|string $cacheDir directory for the generated matcher file (defaults to the system temp directory).
     *                              It is `require`d, so it MUST be trusted - not writable by untrusted users. Prefer an
     *                              application-private directory over the shared system temp in multi-tenant environments.
     * @param int $cacheDirPermissions mode applied when the cache directory is created (octal, subject to umask;
     *                                 defaults to 0700 - owner only, since the directory holds `require`d PHP)
     */
    public function __construct(
        Matcher $matcher,
        bool $exclude = true,
        array $sources = [AttributeSource::SIGNAL],
        ?string $cacheDir = null,
        int $cacheDirPermissions = 0o700,
    ) {
        if ($cacheDirPermissions < 0 || $cacheDirPermissions > 0o777) {
            throw new InvalidArgumentException(sprintf(
                'Cache directory permissions must be between 0 and 0777, got %o',
                $cacheDirPermissions,
            ));
        }

        $this->sources = $sources === [] ? [AttributeSource::SIGNAL] : array_values($sources);

        $match = CompiledMatcher::build($matcher, $cacheDir, $cacheDirPermissions);

        $this->dropFunction = static function (Attributes ...$perSource) use ($match, $exclude): bool {
            $matched = false;

            foreach ($perSource as $attributes) {
                if ($match($attributes)) {
                    $matched = true;

                    break;
                }
            }

            return $exclude ? $matched : !$matched;
        };
    }

    /**
     * @return non-empty-list<AttributeSource>
     */
    public function sources(): array
    {
        return $this->sources;
    }

    /**
     * The compiled drop decision: true when a signal carrying the given per-source
     * attributes should be dropped. Pass one {@see Attributes} per configured source
     * (see {@see self::sources()}); the matcher is OR-combined across them. Processors
     * hold this closure and call it directly to skip the {@see self::shouldDrop()}
     * method-call layer on the hot path.
     *
     * @return Closure(Attributes ...): bool
     */
    public function dropFunction(): Closure
    {
        return $this->dropFunction;
    }

    /**
     * True when a signal carrying these per-source attributes should be dropped.
     * Pass one {@see Attributes} per configured source (see {@see self::sources()}).
     */
    public function shouldDrop(Attributes ...$perSource): bool
    {
        return ($this->dropFunction)(...$perSource);
    }
}
