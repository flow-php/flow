<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API;

use Flow\ETL\API\Transformation;
use Flow\ETL\API\Transformations;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\select;

final class TransformationsTest extends FlowTestCase
{
    public function test_transformations(): void
    {
        $transformation1 = $this->createMock(Transformation::class);
        $transformation2 = $this->createMock(Transformation::class);

        $dataFrame = df()->read(from_array([['id' => 1], ['id' => 2]]));

        $transformation1->expects(self::once())->method('__invoke')->with($dataFrame)->willReturn($dataFrame);
        $transformation2->expects(self::once())->method('__invoke')->with($dataFrame)->willReturn($dataFrame);

        static::assertSame($dataFrame, (new Transformations($transformation1, $transformation2))($dataFrame));
    }

    public function test_invoking_transformations_gives_the_rows_of_with(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]));
        $expected = [['id' => 1]];

        static::assertSame($expected, (new Transformations(select('id'), limit(1)))($dataFrame)->fetch()->toArray());
        static::assertSame(
            $expected,
            $dataFrame
                ->with(new Transformations(select('id'), limit(1)))
                ->fetch()
                ->toArray(),
        );
    }
}
