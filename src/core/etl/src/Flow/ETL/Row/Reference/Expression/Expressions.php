<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\ExpandResults;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\UnpackResults;

final class Expressions implements ExpandResults, Expression, UnpackResults
{
    use Row\Reference\EntryExpression;

    /**
     * @var array<Expression>
     */
    private array $expressions;

    public function __construct(Expression ...$expressions)
    {
        $this->expressions = $expressions;
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    public function eval(Row $row) : mixed
    {
        $lastValue = null;

        foreach ($this->expressions as $expression) {
            $lastValue = $expression->eval($row);

            if ($expression instanceof Row\EntryReference) {
                $row = $row->set((new Row\Factory\NativeEntryFactory())->create($expression->to(), $lastValue));
            }
        }

        return $lastValue;
    }

    public function expand() : bool
    {
        foreach ($this->expressions as $expression) {
            if ($expression instanceof ExpandResults) {
                return $expression->expand();
            }
        }

        return false;
    }

    public function unpack() : bool
    {
        foreach ($this->expressions as $expression) {
            if ($expression instanceof UnpackResults) {
                return $expression->unpack();
            }
        }

        return false;
    }
}
