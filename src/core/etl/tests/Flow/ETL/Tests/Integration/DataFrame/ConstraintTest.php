<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{constraint_unique, df, from_array};
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Tests\FlowTestCase;

final class ConstraintTest extends FlowTestCase
{
    public function test_unique_on_multiple_fields() : void
    {
        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('Constraint violation: Unique constraint on [id, sub_id] - Values: [id<integer> = 4, sub_id<integer> = 4] in row: 5');

        df()
            ->read(from_array([
                ['id' => 1, 'sub_id' => 1, 'name' => 'John'],
                ['id' => 2, 'sub_id' => 1, 'name' => 'Jane'],
                ['id' => 3, 'sub_id' => 1, 'name' => 'Doe'],
                ['id' => 1, 'sub_id' => 2, 'name' => 'John'],
                ['id' => 4, 'sub_id' => 4, 'name' => 'Michael'],
                ['id' => 4, 'sub_id' => 4, 'name' => 'Michael'],
            ]))
            ->constrain(constraint_unique('id', 'sub_id'))
            ->run();
    }
}
