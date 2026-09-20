<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

final class ContextRecordingTransformer implements Transformer
{
    /**
     * @var list<FlowContext>
     */
    public array $contexts = [];

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $this->contexts[] = $context;

        return $rows;
    }
}
