<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

/**
 * Closes a cycle only on the second batch, so the re-entry lands after the pipeline has already
 * yielded once. A cycle closing on the first batch is caught before Pipeline::process() ever parks.
 */
final class ReadsBackOnSecondBatch implements Transformer
{
    public int $transforms = 0;

    private ?DataFrame $dataFrame = null;

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function readBackFrom(DataFrame $dataFrame): void
    {
        $this->dataFrame = $dataFrame;
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $this->transforms++;

        if ($this->transforms === 2 && $this->dataFrame !== null) {
            $this->dataFrame->fetch();
        }

        return $rows;
    }
}
