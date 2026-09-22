<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, JsonFileSample>
 */
final class JsonSamples implements IteratorAggregate
{
    /**
     * @var list<JsonFileSample>
     */
    private array $pulled = [];

    /**
     * @param iterable<JsonFileSample> $samples
     */
    public function __construct(
        private readonly iterable $samples,
    ) {}

    /**
     * Keeps only the samples the consumer pulled: a listing the inference stopped short of costs nothing.
     *
     * @return Generator<int, JsonFileSample>
     */
    public function getIterator(): Generator
    {
        foreach ($this->samples as $sample) {
            $this->pulled[] = $sample;

            yield $sample;
        }
    }

    public function sampledFiles(): JsonSampledFiles
    {
        $files = 0;
        $rows = 0;
        $bytes = 0;
        $whole = true;

        foreach ($this->pulled as $sample) {
            $sampled = $sample->sampled();

            if ($sampled === null) {
                continue;
            }

            $files++;
            $rows += $sampled->rows;
            $bytes += $sampled->bytes;
            $whole = $whole && $sampled->wholeFile;
        }

        return new JsonSampledFiles($files, $rows, $bytes, $whole);
    }
}
