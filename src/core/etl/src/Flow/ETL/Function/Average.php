<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
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
use function Flow\ETL\DSL\integer_entry;
use function is_int;
use function is_numeric;

final class Average implements AggregatingFunction, WindowFunction
{
    private int $count;

    private float $sum;

    private ?Window $window;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale = 2,
        private readonly Rounding $rounding = Rounding::HALF_UP,
    ) {
        $this->window = null;
        $this->count = 0;
        $this->sum = 0;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (is_numeric($value)) {
                // @mago-ignore analysis:possibly-invalid-argument
                $this->sum = $context->calculator()->add($this->sum, $value);
                $this->count++;
            }
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Average error: ' . $e->getMessage()));
        }
    }

    public function apply(Row $row, Rows $partition, FlowContext $context): mixed
    {
        $sum = 0;
        $count = 0;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            try {
                /** @var mixed $value */
                $value = $partitionRow->valueOf($this->ref);

                if (is_numeric($value)) {
                    // @mago-ignore analysis:possibly-invalid-argument
                    $sum = $context->calculator()->add($sum, $value);
                    $count++;
                }
            } catch (InvalidArgumentException $e) {
                $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException('Average window function error: ' . $e->getMessage(), 0, $e),
                    );
            }
        }

        return $context->calculator()->divide($sum, $count, $this->scale, $this->rounding);
    }

    public function over(Window $window): WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_avg');
        }

        if (0 !== $this->count) {
            $result = (new Calculator())->divide($this->sum, $this->count, $this->scale, $this->rounding);
        } else {
            $result = 0;
        }

        if (is_int($result)) {
            return integer_entry($this->ref->name(), $result);
        }

        return float_entry($this->ref->name(), $result);
    }

    public function toString(): string
    {
        return 'average()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
