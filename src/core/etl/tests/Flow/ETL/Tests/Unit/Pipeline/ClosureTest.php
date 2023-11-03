<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use PHPUnit\Framework\TestCase;

final class ClosureTest extends TestCase
{
    public function test_loader_closure() : void
    {
        (new Flow())
            ->extract(new AllRowTypesFakeExtractor(20, 2))
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

                public function __serialize() : array
                {
                    return [];
                }

                public function __unserialize(array $data) : void
                {
                }
            })
            ->run();

        $this->assertTrue($loader->closureCalled);
        $this->assertSame(20, $loader->rowsLoaded);
    }
}
