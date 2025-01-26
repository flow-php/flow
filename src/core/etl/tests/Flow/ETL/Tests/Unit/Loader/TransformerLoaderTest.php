<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, rows};
use function Flow\ETL\DSL\{df, flow_context, from_array, ref, to_memory, to_transformation, type_string};
use Flow\ETL\{DataFrame, Loader, Memory\ArrayMemory, Tests\FlowTestCase, Transformation, Transformer};

final class TransformerLoaderTest extends FlowTestCase
{
    public function test_transformer_loader() : void
    {
        $transformerMock = $this->createMock(Transformer::class);
        $transformerMock->expects(self::once())
            ->method('transform')
            ->willReturn(rows());

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects(self::once())
            ->method('load');

        $transformer = to_transformation(
            $transformerMock,
            $loaderMock
        );

        $transformer->load(rows(), flow_context(config()));
    }

    public function test_transformer_loader_with_transformation() : void
    {
        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->write(to_transformation(
                new class implements Transformation {
                    public function transform(DataFrame $dataFrame) : DataFrame
                    {
                        return $dataFrame->withEntry('id_string', ref('id')->cast(type_string()));
                    }
                },
                to_memory($memory = new ArrayMemory())
            ))
            ->run();

        self::assertEquals(
            [
                ['id' => 1, 'id_string' => '1'],
                ['id' => 2, 'id_string' => '2'],
                ['id' => 3, 'id_string' => '3'],
            ],
            $memory->dump()
        );
    }
}
