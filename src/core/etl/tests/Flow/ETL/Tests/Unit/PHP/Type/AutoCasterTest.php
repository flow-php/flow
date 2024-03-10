<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use Flow\ETL\PHP\Type\{AutoCaster, Caster};
use PHPUnit\Framework\TestCase;

final class AutoCasterTest extends TestCase
{
    public function test_auto_casting_array_of_ints_and_floats_into_array_of_floats() : void
    {
        self::assertSame(
            [1.0, 2.0, 3.0],
            (new AutoCaster(Caster::default()))->cast([1, 2, 3.0])
        );
    }
}
