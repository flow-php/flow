<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\GroupBy\Aggregator\Average;
use Flow\ETL\GroupBy\Aggregator\Count;
use Flow\ETL\GroupBy\Aggregator\Max;
use Flow\ETL\GroupBy\Aggregator\Min;
use Flow\ETL\GroupBy\Aggregator\Sum;

final class Aggregation
{
    private string $entry;

    private string $type;

    public function __construct(string $type, string $entry)
    {
        if (!\in_array($type, ['avg', 'count', 'max', 'min', 'sum'], true)) {
            throw new InvalidArgumentException("Unknown aggregation \"{$type}\", expected one of: 'avg', 'count', 'max', 'min', 'sum'");
        }

        $this->type = $type;
        $this->entry = $entry;
    }

    public static function avg(string $entry) : self
    {
        return new self('avg', $entry);
    }

    public static function max(string $entry) : self
    {
        return new self('max', $entry);
    }

    public static function min(string $entry) : self
    {
        return new self('min', $entry);
    }

    public static function sum(string $entry) : self
    {
        return new self('sum', $entry);
    }

    public function create() : Aggregator
    {
        switch ($this->type) {
            case 'avg':
                return new Average($this->entry);
            case 'count':
                return new Count($this->entry);
            case 'max':
                return new Max($this->entry);
            case 'min':
                return new Min($this->entry);
            case 'sum':
                return new Sum($this->entry);

            default:
                /** @codeCoverageIgnore */
                throw new RuntimeException("Unknown aggregation \"{$this->type}\", expected one of: 'avg', 'count', 'max', 'min', 'sum'");
        }
    }

    public function entry() : string
    {
        return $this->entry;
    }
}
