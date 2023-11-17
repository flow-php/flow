<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\DSL\To;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

final class PipesTest extends TestCase
{
    public function test_getting_only_loaders_from_pipes() : void
    {
        $pipes = new Pipes([
            new ScalarFunctionTransformer('string', lit('test')),
            $outputLoader = To::output(),
            $memoryLoader = To::memory(new ArrayMemory()),
        ]);

        $this->assertEquals(
            [$outputLoader, $memoryLoader],
            $pipes->loaders()
        );
    }

    public function test_has_transformer() : void
    {
        $pipes = new Pipes([
            new ScalarFunctionTransformer('string', lit('test')),
            $outputLoader = To::output(),
            $memoryLoader = To::memory(new ArrayMemory()),
        ]);

        $this->assertTrue($pipes->has(ScalarFunctionTransformer::class));
        $this->assertFalse($pipes->has(DropDuplicatesTransformer::class));
    }

    public function test_has_transformer_when_passed_class_does_not_exists() : void
    {
        $pipes = new Pipes([
            new ScalarFunctionTransformer('string', lit('test')),
            $outputLoader = To::output(),
            $memoryLoader = To::memory(new ArrayMemory()),
        ]);

        $this->assertFalse($pipes->has('SomeClassThatDoesNotExist'));
    }

    public function test_has_transformer_when_passed_class_is_not_a_transformer_class() : void
    {
        $pipes = new Pipes([
            new ScalarFunctionTransformer('string', lit('test')),
            $outputLoader = To::output(),
            $memoryLoader = To::memory(new ArrayMemory()),
        ]);

        $this->assertFalse($pipes->has(Pipes::class));
    }
}
