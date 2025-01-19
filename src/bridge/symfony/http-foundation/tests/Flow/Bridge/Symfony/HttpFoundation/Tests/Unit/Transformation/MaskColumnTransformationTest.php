<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Transformation;

use function Flow\ETL\DSL\{df, from_array};
use Flow\Bridge\Symfony\HttpFoundation\Transformation\MaskColumns;
use Flow\ETL\Tests\FlowTestCase;

final class MaskColumnTransformationTest extends FlowTestCase
{
    public function test_masking_columns_transformation() : void
    {
        $output = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John Doe', 'salary' => 7000, 'currency' => 'USD'],
                ['id' => 2, 'name' => 'Jane Doe', 'salary' => 8000, 'currency' => 'USD'],
                ['id' => 3, 'name' => 'John Smith', 'salary' => 9000, 'currency' => 'USD'],
                ['id' => 4, 'name' => 'Jane Smith', 'salary' => 10000, 'currency' => 'USD'],
            ]))
            ->with(new MaskColumns(['salary']))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'John Doe', 'salary' => '******', 'currency' => 'USD'],
                ['id' => 2, 'name' => 'Jane Doe', 'salary' => '******', 'currency' => 'USD'],
                ['id' => 3, 'name' => 'John Smith', 'salary' => '******', 'currency' => 'USD'],
                ['id' => 4, 'name' => 'Jane Smith', 'salary' => '******', 'currency' => 'USD'],
            ],
            $output
        );
    }
}
