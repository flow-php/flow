<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

final readonly class ThrowWhenRowMatches implements Transformer
{
    public function __construct(
        private string $column,
        private mixed $value,
        private Throwable $throwable,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        foreach ($rows as $row) {
            if ($row->get($this->column) === $this->value) {
                throw $this->throwable;
            }
        }

        return $rows;
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }
}
