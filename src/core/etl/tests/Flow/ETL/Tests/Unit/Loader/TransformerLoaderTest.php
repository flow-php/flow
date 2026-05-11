<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Types\DSL\type_string;

final class TransformerLoaderTest extends FlowTestCase
{
    public function test_transformer_loader(): void
    {
        $transformerMock = $this->createMock(Transformer::class);
        $transformerMock->expects(self::once())->method('transform')->willReturn(rows());

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects(self::once())->method('load');

        $transformer = to_transformation($transformerMock, $loaderMock);

        $transformer->load(rows(), flow_context(config()));
    }

    public function test_transformer_loader_with_transformation(): void
    {
        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->write(to_transformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('id_string', ref('id')->cast(type_string()));
                }
            }, to_memory($memory = new ArrayMemory())))
            ->run();

        static::assertEquals(
            [
                ['id' => 1, 'id_string' => '1'],
                ['id' => 2, 'id_string' => '2'],
                ['id' => 3, 'id_string' => '3'],
            ],
            $memory->dump(),
        );
    }
}
