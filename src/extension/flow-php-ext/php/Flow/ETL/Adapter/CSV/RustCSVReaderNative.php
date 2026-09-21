<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Row\RawRowValues;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustCSVReaderNative
{
    public function __construct(
        string $separator,
        string $enclosure,
        string $escape,
        bool $withHeader,
        bool $emptyToNull,
        bool $removeBOM,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function feed(string $chunk): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function finish(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param int<0, max>|-1 $limit
     *
     * @return int<0, max>
     */
    public function fold(RustColumnFoldNative $fold, int $limit): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return list<string>
     */
    public function headers(): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return list<RawRowValues>
     */
    public function next(int $batchSize): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
