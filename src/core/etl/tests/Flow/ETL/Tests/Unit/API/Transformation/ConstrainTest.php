<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Foundation\Exception\ConstraintViolationException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\constrain;
use function Flow\ETL\DSL\constraint_unique;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class ConstrainTest extends FlowTestCase
{
    public function test_constrain_through_invoke(): void
    {
        $this->expectException(ConstraintViolationException::class);

        constrain(constraint_unique('a'))(df()->read(from_array([['a' => 1], ['a' => 1]])))->fetch();
    }

    public function test_constrain_through_with(): void
    {
        $this->expectException(ConstraintViolationException::class);

        df()
            ->read(from_array([['a' => 1], ['a' => 1]]))
            ->with(constrain(constraint_unique('a')))
            ->fetch();
    }
}
