<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ChainTransformer;
use PHPUnit\Framework\TestCase;

final class ChainTransformerTest extends TestCase
{
    public function test_using_all_transfomers() : void
    {
        $transformer1 = $this->createMock(Transformer::class);
        $transformer1->expects($this->once())
            ->method('transform')
            ->willReturn(new Rows());
        $transformer2 = $this->createMock(Transformer::class);
        $transformer2->expects($this->once())
            ->method('transform')
            ->willReturn(new Rows());

        $transformer = new ChainTransformer($transformer1, $transformer2);

        $transformer->transform(new Rows());
    }
}
