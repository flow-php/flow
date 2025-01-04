<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_float, type_int, type_list};
use Flow\ETL\PHP\Type\Caster\ListCastingHandler;
use Flow\ETL\Tests\FlowTestCase;

final class ListCastingHandlerTest extends FlowTestCase
{
    public function test_casting_list_of_ints_to_list_of_floats() : void
    {
        self::assertSame(
            [1.0, 2.0, 3.0],
            (new ListCastingHandler())->value([1, 2, 3], type_list(type_float()), caster(), caster_options())
        );
    }

    public function test_casting_string_to_list_of_ints() : void
    {
        self::assertSame(
            [1],
            (new ListCastingHandler())->value(['1'], type_list(type_int()), caster(), caster_options())
        );
    }
}
