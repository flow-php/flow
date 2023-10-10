<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\DataFrame;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Pipeline;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;

/**
 * @infection-ignore-all
 */
class From
{
    /**
     * Alias for `chain()` method.
     */
    final public static function all(Extractor ...$extractors) : Extractor
    {
        return self::chain(...$extractors);
    }

    /**
     * @param array<array<string, mixed>> $array
     * @param int<1, max> $batch_size
     */
    final public static function array(array $array, int $batch_size = 100, EntryFactory $entry_factory = new NativeEntryFactory()) : Extractor
    {
        return new MemoryExtractor(new ArrayMemory($array), $batch_size, $entry_factory);
    }

    final public static function buffer(Extractor $extractor, int $max_row_size) : Extractor
    {
        return new Extractor\BufferExtractor($extractor, $max_row_size);
    }

    final public static function cache(string $id, Extractor $fallback_extractor = null, bool $clear = false) : Extractor
    {
        return new Extractor\CacheExtractor($id, $fallback_extractor, $clear);
    }

    final public static function chain(Extractor ...$extractors) : Extractor
    {
        return new Extractor\ChainExtractor(...$extractors);
    }

    final public static function chunks_from(Extractor $extractor, int $chunk_size) : Extractor
    {
        return new Extractor\ChunkExtractor($extractor, $chunk_size);
    }

    final public static function data_frame(DataFrame $data_frame) : Extractor
    {
        return new Extractor\DataFrameExtractor($data_frame);
    }

    /**
     * @param Memory $memory
     * @param int<1, max> $chunk_size
     *
     * @return Extractor
     */
    final public static function memory(Memory $memory, int $chunk_size = 100, EntryFactory $entry_factory = new NativeEntryFactory()) : Extractor
    {
        return new MemoryExtractor($memory, $chunk_size, $entry_factory);
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
