<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\Reader;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SourceStreamAdapter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Generator;

use function array_keys;
use function array_push;
use function array_slice;
use function count;
use function min;

final readonly class ArrowParquetFileReader implements ParquetFileReader
{
    public function __construct(
        private PhpParquetFileReader $footer,
        private SourceStream $stream,
        private Options $options,
    ) {}

    public function close(): void
    {
        $this->footer->close();
    }

    public function metadata(): Metadata
    {
        return $this->footer->metadata();
    }

    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): Generator
    {
        $adapter = new SourceStreamAdapter($this->stream);
        $extensionOptions = OptionsConverter::toExtension($this->options);
        $reader = new Reader($adapter, $extensionOptions);

        try {
            $toSkip = $offset ?? 0;
            $remaining = $limit;
            /** @var array<string, list<mixed>> $carry */
            $carry = [];
            $carryCount = 0;

            while (($remaining === null || $remaining > 0) && null !== ($got = $reader->readRowGroup($columns))) {
                $names = array_keys($got);

                if ($names === []) {
                    continue;
                }

                $available = count($got[$names[0]]);
                $cursor = min($toSkip, $available);
                $toSkip -= $cursor;

                while ($cursor < $available && ($remaining === null || $remaining > 0)) {
                    $take = min($batchSize - $carryCount, $available - $cursor, $remaining ?? $available);

                    foreach ($names as $name) {
                        /** @var list<mixed> $slice */
                        $slice = array_slice($got[$name], $cursor, $take);

                        if ($carryCount === 0) {
                            $carry[$name] = $slice;
                        } else {
                            array_push($carry[$name], ...$slice);
                        }
                    }

                    $carryCount += $take;
                    $cursor += $take;

                    if ($remaining !== null) {
                        $remaining -= $take;
                    }

                    if ($carryCount === $batchSize) {
                        yield $carry;
                        $carry = [];
                        $carryCount = 0;
                    }
                }
            }

            if ($carryCount > 0) {
                yield $carry;
            }
        } finally {
            $reader->close();
        }
    }

    public function rowsNumber(): int
    {
        return $this->footer->rowsNumber();
    }

    public function schema(): Schema
    {
        return $this->footer->schema();
    }

    public function totalByteSize(): int
    {
        return $this->footer->totalByteSize();
    }
}
