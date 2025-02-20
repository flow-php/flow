<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{df, from_array};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\{Transformation, Transformations};

final class TransformationsTest extends FlowTestCase
{
    public function test_transformations() : void
    {
        $transformation1 = $this->createMock(Transformation::class);
        $transformation2 = $this->createMock(Transformation::class);

        $dataFrame = df()->read(from_array([['id' => 1], ['id' => 2]]));

        $transformation1->expects(self::once())->method('transform')->with($dataFrame)->willReturn($dataFrame);
        $transformation2->expects(self::once())->method('transform')->with($dataFrame)->willReturn($dataFrame);

        (new Transformations($transformation1, $transformation2))->transform($dataFrame);
    }
}
