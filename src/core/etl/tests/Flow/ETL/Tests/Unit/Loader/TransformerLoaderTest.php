<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, rows};
use function Flow\ETL\DSL\{flow_context, to_transformation};
use Flow\ETL\{Loader, Tests\FlowTestCase, Transformer};

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
}
