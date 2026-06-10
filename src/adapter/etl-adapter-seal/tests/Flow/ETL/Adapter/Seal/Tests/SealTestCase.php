<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests;

use Flow\ETL\Adapter\Seal\Tests\Context\SealContext;
use Flow\ETL\Tests\FlowTestCase;

abstract class SealTestCase extends FlowTestCase
{
    private ?SealContext $sealContext = null;

    protected function tearDown(): void
    {
        $this->sealContext?->dropIndexes();
        $this->sealContext = null;
    }

    protected function sealContext(): SealContext
    {
        if ($this->sealContext === null) {
            $this->sealContext = new SealContext();
        }

        return $this->sealContext;
    }
}
