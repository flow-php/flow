<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_enum};
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster\EnumCastingHandler;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\PHP\Type\Caster\Fixtures\ColorsEnum;

final class EnumCastingHandlerTest extends FlowTestCase
{
    public function test_casting_integer_to_enum() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "integer" into "enum<Flow\ETL\Tests\Unit\PHP\Type\Caster\Fixtures\ColorsEnum>" type');

        (new EnumCastingHandler())->value(1, type_enum(ColorsEnum::class), caster(), caster_options());
    }

    public function test_casting_string_to_enum() : void
    {
        self::assertEquals(
            ColorsEnum::RED,
            (new EnumCastingHandler())->value('red', type_enum(ColorsEnum::class), caster(), caster_options())
        );
    }
}
