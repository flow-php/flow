<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\{FlowContext, Rows, Transformer};

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
