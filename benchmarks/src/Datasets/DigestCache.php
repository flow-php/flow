<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

/**
 * One source-tree walk per tree per process. Warmed from a #[Bench\BeforeMethods] method so no
 * timed window ever pays for it.
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
