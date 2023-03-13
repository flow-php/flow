<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Cache;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @infection-ignore-all
 */
class From
{
    /**
     * @param array<array<string, mixed>> $array
     * @param int<1, max> $batch_size
     * @param string $entry_row_name
     */
    final public static function array(array $array, int $batch_size = 100, string $entry_row_name = 'row') : Extractor
    {
        return new MemoryExtractor(new ArrayMemory($array), $batch_size, $entry_row_name);
    }

    final public static function buffer(Extractor $extractor, int $maxRowsSize) : Extractor
    {
        return new Extractor\BufferExtractor($extractor, $maxRowsSize);
    }

    final public static function cache(string $id, Cache $cache, bool $clear = false) : Extractor
    {
        return new Extractor\CacheExtractor($id, $cache, $clear);
    }

    final public static function chain(Extractor ...$extractors) : Extractor
    {
        return new Extractor\ChainExtractor(...$extractors);
    }

    final public static function chunks_from(Extractor $extractor, int $chunkSize) : Extractor
    {
        return new Extractor\ChunkExtractor($extractor, $chunkSize);
    }

    /**
     * @param Memory $memory
     * @param int<1, max> $chunkSize
     * @param string $rowEntryName
     *
     * @return Extractor
     */
    final public static function memory(Memory $memory, int $chunkSize = 100, string $rowEntryName = 'row') : Extractor
    {
        return new MemoryExtractor($memory, $chunkSize, $rowEntryName);
    }

    final public static function pipeline(Pipeline $pipeline) : Extractor
    {
        return new Extractor\PipelineExtractor($pipeline);
    }

    final public static function rows(Rows ...$rows) : Extractor
    {
        return new ProcessExtractor(...$rows);
    }

    /**
     * @param string $entry_name
     * @param \DateTimeInterface $start
     * @param \DateInterval $interval
     * @param \DateTimeInterface $end
     * @param 0|1 $options
     *
     * @return Extractor
     */
    final public static function sequence_date_period(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, \DateTimeInterface $end, int $options = 0) : Extractor
    {
        return new Extractor\SequenceExtractor(
            new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
            $entry_name
        );
    }

    /**
     * @param string $entry_name
     * @param \DateTimeInterface $start
     * @param \DateInterval $interval
     * @param int<1, max> $recurrences
     * @param 0|1 $options
     *
     * @return Extractor
     */
    final public static function sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : Extractor
    {
        return new Extractor\SequenceExtractor(
            new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $recurrences, $options)),
            $entry_name
        );
    }

    final public static function sequence_number(string $entry_name, string|int|float $start, string|int|float $end, int|float $step = 1) : Extractor
    {
        return new Extractor\SequenceExtractor(
            new Extractor\SequenceGenerator\NumberSequenceGenerator($start, $end, $step),
            $entry_name
        );
    }
}
