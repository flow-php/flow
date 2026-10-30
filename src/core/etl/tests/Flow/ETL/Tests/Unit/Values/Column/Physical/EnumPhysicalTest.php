<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Physical;

use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Values\Column\Physical\EnumPhysical;
use PHPUnit\Framework\TestCase;

final class EnumPhysicalTest extends TestCase
{
    public function test_the_case_name(): void
    {
        $physical = new EnumPhysical(BasicEnum::class);

        static::assertSame('one', $physical->toPhysical(BasicEnum::one));
        static::assertSame(BasicEnum::one, $physical->fromPhysical('one'));
        static::assertSame([BasicEnum::two, null], $physical->fromPhysicalAll(['two', null]));
    }
}
