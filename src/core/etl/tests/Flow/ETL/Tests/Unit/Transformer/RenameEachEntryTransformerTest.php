<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\{Exception\InvalidArgumentException, Tests\FlowTestCase, Transformer\RenameEachEntryTransformer};

final class RenameEachEntryTransformerTest extends FlowTestCase
{
    public function test_renaming_fails_without_any_strategy() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one strategy must be provided.');

        new RenameEachEntryTransformer();
    }
}
