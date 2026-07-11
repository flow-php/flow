<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function is_numeric;

final class Sum implements AggregatingFunction, WindowFunction
{
    private float|int $sum;

    private ?Window $window;

    public function __construct(
        private readonly Reference $ref,
        private readonly ScalarFunction|bool $exact = false,
    ) {
        $this->sum = 0;
        $this->window = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        try {
            $value = $row->valueOf($this->ref);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->sum = $this->add($this->sum, $value, $row, $context);
            }
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Sum error: ' . $e->getMessage()));
        }
    }

    public function apply(Row $row, Rows $partition, FlowContext $context): mixed
    {
        $sum = 0;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            try {
                $value = $partitionRow->valueOf($this->ref);

                if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                    $sum = $this->add($sum, $value, $partitionRow, $context);
                }
            } catch (InvalidArgumentException $e) {
                $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException('Sum window function error: ' . $e->getMessage(), 0, $e),
                    );
            }
        }

        return $sum;
    }

    public function over(Window $window): WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return Entry<?float>|Entry<?int>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_sum');
        }

        if (!is_float($this->sum)) {
            return int_entry($this->ref->name(), (int) $this->sum);
        }

        return float_entry($this->ref->name(), $this->sum);
    }

    public function toString(): string
    {
        return 'sum()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }

    /**
     * @param float|int|numeric-string $value
     */
    private function add(float|int $sum, float|int|string $value, Row $row, FlowContext $context): float|int
    {
        if ($this->isExact($row, $context)) {
            return $context->calculator()->add($sum, $value);
        }

        $result = $sum + $value;

        if (
            is_float($result)
            && floor($result) === $result
            && $result >= (float) PHP_INT_MIN
            && $result < (float) PHP_INT_MAX
        ) {
            return (int) $result;
        }

        return $result;
    }

    private function isExact(Row $row, FlowContext $context): bool
    {
        if (is_bool($this->exact)) {
            return $this->exact;
        }

        return (new Parameter($this->exact))->asBoolean($row, $context);
    }
}
