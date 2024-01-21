<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Caster\ListCastingHandler;
use PHPUnit\Framework\TestCase;

final class ListCastingHandlerTest extends TestCase
{
    public function test_casting_list_of_ints_to_list_of_floats() : void
    {
        $this->assertSame(
            [1.0, 2.0, 3.0],
            (new ListCastingHandler())->value([1, 2, 3], type_list(type_float()), Caster::default())
        );
    }

    public function test_casting_string_to_list_of_ints() : void
    {
        $this->assertSame(
            [1],
            (new ListCastingHandler())->value(['1'], type_list(type_int()), Caster::default())
        );
    }
}
