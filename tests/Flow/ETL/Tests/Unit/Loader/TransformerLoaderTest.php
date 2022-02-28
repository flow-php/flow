<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DSL\To;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\Serializer\NativePHPSerializer;
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

        $transformer = To::transform_to(
            $transformerMock,
            $loaderMock
        );

        $transformer->load(new Rows());
    }

    public function test_transformer_loader_with_serialization() : void
    {
        $transformerMock = $this->createMock(Transformer::class);

        $loaderMock = $this->createMock(Loader::class);

        $transformer = To::transform_to(
            $transformerMock,
            $loaderMock
        );

        $serializer = new NativePHPSerializer();

        $this->assertEquals($transformer, $serializer->unserialize($serializer->serialize($transformer)));
    }
}
