<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

/**
 * Warm from #[BeforeMethods] so no timed window pays for the tree walk.
 */
final class DigestCache
{
    /** @var array<string, string> */
    private static array $digests = [];

    public static function clear(): void
    {
        self::$digests = [];
    }

    public static function tree(string $relativeTree): string
    {
        return self::$digests[$relativeTree] ??= (new SourceTreeDigest($relativeTree))->value();
    }
}
