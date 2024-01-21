<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_enum;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster\EnumCastingHandler;
use Flow\ETL\Tests\Unit\PHP\Type\Caster\Fixtures\ColorsEnum;
use PHPUnit\Framework\TestCase;

final class EnumCastingHandlerTest extends TestCase
{
    public function test_casting_integer_to_enum() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "integer" into "enum<Flow\ETL\Tests\Unit\PHP\Type\Caster\Fixtures\ColorsEnum>" type');

        (new EnumCastingHandler())->value(1, type_enum(ColorsEnum::class));
    }

    public function test_casting_string_to_enum() : void
    {
        $this->assertEquals(
            ColorsEnum::RED,
            (new EnumCastingHandler())->value('red', type_enum(ColorsEnum::class))
        );
    }
}
