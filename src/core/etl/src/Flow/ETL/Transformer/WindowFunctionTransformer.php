<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class WindowFunctionTransformer implements Transformer
{
    public function __construct(
        private readonly string $entryName,
        private readonly WindowFunction $function,
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     * @throws RuntimeException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = new Rows();

        foreach ($rows as $row) {
            $newRows = $newRows->add(
                $row->add($context->entryFactory()->create($this->entryName, $this->function->apply($row, $rows)))
            );
        }

        return $newRows;
    }
}
