<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\{FlowContext, Row\Schema\Definition, Rows, Transformer};

final readonly class WindowFunctionTransformer implements Transformer
{
    public function __construct(
        private string|Definition $entry,
        private WindowFunction $function,
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = new Rows();

        foreach ($rows as $row) {
            $newRows = $newRows->add(
                $row->add(
                    $context->entryFactory()->create(
                        $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry,
                        $this->function->apply($row, $rows),
                        $this->entry instanceof Definition ? $this->entry : null
                    )
                )
            );
        }

        return $newRows;
    }
}
