<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Generator;

use function count;

final class GoogleSheetSampler implements SchemaSampler
{
    private ?GoogleSheetSample $sample = null;

    /**
     * @param int<1, max>|-1 $rowBudget
     */
    public function __construct(
        private readonly GoogleSheetReader $reader,
        private readonly int $rowBudget,
    ) {}

    /**
     * The rows the sample already holds, batched, when its range covered the whole grid - so a read does not fetch
     * the same rows a second time. Null when it did not, and the caller must read the sheet itself.
     *
     * @param int<1, max> $batchSize
     *
     * @return null|Generator<int, list<RawRowValues>>
     */
    public function batches(int $batchSize): ?Generator
    {
        if (!$this->sample()->wholeSheet) {
            return null;
        }

        return (function () use ($batchSize): Generator {
            $batch = [];

            foreach ($this->sample()->rows as $rowValues) {
                $batch[] = $rowValues;

                if (count($batch) >= $batchSize) {
                    yield $batch;
                    $batch = [];
                }
            }

            if ($batch !== []) {
                yield $batch;
            }
        })();
    }

    /**
     * @return list<string>
     */
    public function header(): array
    {
        return $this->sample()->names;
    }

    public function sample(): GoogleSheetSample
    {
        // one range is one HTTP round trip, and header() and samples() both need it
        return $this->sample ??= $this->reader->sample($this->rowBudget);
    }

    /**
     * $rowBudget is deliberately unused: the budget sized the single range when this sampler was constructed,
     * and sample() is memoised, so advancing further cannot fetch more.
     *
     * @return Generator<int, Generator<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable
    {
        yield (function (): Generator {
            yield from $this->sample()->rows;
        })();
    }
}
