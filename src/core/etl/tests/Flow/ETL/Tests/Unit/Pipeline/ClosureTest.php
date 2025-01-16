<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\data_frame;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{FlowContext, Loader, Rows, Tests\FlowTestCase};

final class ClosureTest extends FlowTestCase
{
    public function test_loader_closure() : void
    {
        (data_frame())
            ->extract(new FakeExtractor(40))
            ->batchSize(2)
            ->load($loader = new class implements Closure, Loader {
                public bool $closureCalled = false;

                public int $rowsLoaded = 0;

                public function load(Rows $rows, FlowContext $context) : void
                {
                    $this->rowsLoaded++;
                }

                public function closure(FlowContext $context) : void
                {
                    $this->closureCalled = true;
                }
            })
            ->run();

        self::assertTrue($loader->closureCalled);
        self::assertSame(20, $loader->rowsLoaded);
    }
}
