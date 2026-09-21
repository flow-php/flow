<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustColumnFoldNative
{
    /**
     * @param list<string> $names
     * @param list<string> $candidates - the candidate types' toString()
     */
    public function __construct(array $names, array $candidates)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function narrowOne(string $value): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return int<0, max>
     */
    public function rows(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return array<array-key, string>
     */
    public function types(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
