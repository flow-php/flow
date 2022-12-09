<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\GroupBy\Aggregator\Average;
use Flow\ETL\GroupBy\Aggregator\Collect;
use Flow\ETL\GroupBy\Aggregator\CollectUnique;
use Flow\ETL\GroupBy\Aggregator\Count;
use Flow\ETL\GroupBy\Aggregator\First;
use Flow\ETL\GroupBy\Aggregator\Last;
use Flow\ETL\GroupBy\Aggregator\Max;
use Flow\ETL\GroupBy\Aggregator\Min;
use Flow\ETL\GroupBy\Aggregator\Sum;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

final class Aggregation
{
    private readonly Reference $entry;

    /**
     * @param string $type
     * @param Reference|string $entry
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $type,
        string|Reference $entry
    ) {
        if (!\in_array($type, ['avg', 'count', 'max', 'min', 'sum', 'first', 'last', 'collect', 'collect_unique'], true)) {
            throw new InvalidArgumentException("Unknown aggregation \"{$type}\", expected one of: 'avg', 'count', 'max', 'min', 'sum', 'first', 'last'");
        }

        $this->entry = $entry instanceof Reference ? $entry : new EntryReference($entry);
    }

    public static function avg(string|EntryReference $entry) : self
    {
        return new self('avg', $entry);
    }

    public static function collect(string|Reference $entry) : self
    {
        return new self('collect', $entry);
    }

    public static function collect_unique(string|Reference $entry) : self
    {
        return new self('collect_unique', $entry);
    }

    public static function count(string|EntryReference $entry) : self
    {
        return new self('count', $entry);
    }

    public static function first(string|EntryReference $entry) : self
    {
        return new self('first', $entry);
    }

    public static function last(string|EntryReference $entry) : self
    {
        return new self('last', $entry);
    }

    public static function max(string|EntryReference $entry) : self
    {
        return new self('max', $entry);
    }

    public static function min(string|EntryReference $entry) : self
    {
        return new self('min', $entry);
    }

    public static function sum(string|EntryReference $entry) : self
    {
        return new self('sum', $entry);
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     */
    public function create() : Aggregator
    {
        return match ($this->type) {
            /** @phpstan-ignore-next-line */
            'avg' => new Average($this->entry),
            /** @phpstan-ignore-next-line */
            'count' => new Count($this->entry),
            /** @phpstan-ignore-next-line */
            'max' => new Max($this->entry),
            /** @phpstan-ignore-next-line */
            'min' => new Min($this->entry),
            /** @phpstan-ignore-next-line */
            'sum' => new Sum($this->entry),
            /** @phpstan-ignore-next-line */
            'first' => new First($this->entry),
            /** @phpstan-ignore-next-line */
            'last' => new Last($this->entry),
            'collect' => new Collect($this->entry),
            'collect_unique' => new CollectUnique($this->entry),
            default => throw new RuntimeException("Unknown aggregation \"{$this->type}\", expected one of: 'avg', 'count', 'max', 'min', 'sum'"),
        };
    }
}
