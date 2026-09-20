<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Transparency;

use function array_values;
use function count;

/**
 * The root of a plan with several consumers of one prefix
 */
final readonly class Outputs implements Node
{
    /**
     * @var non-empty-list<Result|Transaction|Write>
     */
    private array $consumers;

    public function __construct(Result|Transaction|Write ...$consumers)
    {
        if (count($consumers) < 2) {
            throw InvalidLogicException::because('Outputs needs two or more consumers');
        }

        if ($consumers[0] instanceof Transaction) {
            throw InvalidLogicException::firstConsumerIsATransaction();
        }

        $this->consumers = array_values($consumers);
    }

    /**
     * @return non-empty-list<Result|Transaction|Write>
     */
    public function children(): array
    {
        return $this->consumers;
    }

    public function sinks(): Sinks
    {
        $sinks = [];

        foreach ($this->consumers as $consumer) {
            if ($consumer instanceof Write || $consumer instanceof Transaction) {
                $sinks[] = $consumer;
            }
        }

        return new Sinks(...$sinks);
    }

    public function withChildren(array $children): self
    {
        if ($children === $this->consumers) {
            return $this;
        }

        $consumers = [];

        foreach ($children as $child) {
            $consumers[] =
                $child instanceof Result || $child instanceof Write || $child instanceof Transaction
                    ? $child
                    : throw InvalidLogicException::consumerRewritten($child::class);
        }

        return new self(...$consumers);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
