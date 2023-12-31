<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\to_transformation;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;

final class TransformerLoaderTest extends TestCase
{
    public function test_transformer_loader() : void
    {
        $transformerMock = $this->createMock(Transformer::class);
        $transformerMock->expects($this->once())
            ->method('transform')
            ->willReturn(new Rows());

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects($this->once())
            ->method('load');

        $transformer = to_transformation(
            $transformerMock,
            $loaderMock
        );

        $transformer->load(new Rows(), new FlowContext(Config::default()));
    }
}
