<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_float, type_int, type_integer, type_map, type_string};
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster\MapCastingHandler;
use Flow\ETL\Tests\FlowTestCase;

final class MapCastingHandlerTest extends FlowTestCase
{
    public function test_casting_map_of_ints_into_map_of_floats() : void
    {
        self::assertSame(
            [
                'a' => 1.0,
                'b' => 2.0,
                'c' => 3.0,
            ],
            (new MapCastingHandler())->value(['a' => 1, 'b' => 2, 'c' => 3], type_map(type_string(), type_float()), caster(), caster_options())
        );
    }

    public function test_casting_map_of_string_to_ints_into_map_of_int_to_float() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "array" into "map<integer, float>"');

        self::assertSame(
            [
                'a' => 1.0,
                'b' => 2.0,
                'c' => 3.0,
            ],
            (new MapCastingHandler())->value(['a' => 1, 'b' => 2, 'c' => 3], type_map(type_int(), type_float()), caster(), caster_options())
        );
    }

    public function test_casting_scalar_to_map() : void
    {
        self::assertSame(
            [
                '0' => 2,
            ],
            (new MapCastingHandler())->value('2', type_map(type_string(), type_integer()), caster(), caster_options())
        );
    }
}
