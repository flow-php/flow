<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Pipeline\Pipes;

final class PipesTest extends TestCase
{
    public function test_getting_only_loaders_from_pipes() : void
    {
        $pipes = new Pipes([
            Transform::add_string('string', 'test'),
            $outputLoader = To::output(),
            $memoryLoader = To::memory(new ArrayMemory()),
        ]);

        $this->assertEquals(
            [$outputLoader, $memoryLoader],
            $pipes->loaders()
        );
    }
}
