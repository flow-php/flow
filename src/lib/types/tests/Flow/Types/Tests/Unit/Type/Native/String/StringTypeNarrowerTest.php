<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native\String;

use DateTimeInterface;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class StringTypeNarrowerTest extends TestCase
{
    public function test_detecting_boolean(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_boolean(), $narrower->narrow('true'));
        static::assertEquals(type_boolean(), $narrower->narrow('false'));
        static::assertEquals(type_string(), $narrower->narrow('yes'));
        static::assertEquals(type_string(), $narrower->narrow('no'));
        static::assertEquals(type_string(), $narrower->narrow('on'));
        static::assertEquals(type_string(), $narrower->narrow('off'));
        static::assertEquals(type_integer(), $narrower->narrow('0'));
        static::assertEquals(type_string(), $narrower->narrow('not bool'));
    }

    public function test_detecting_date_time(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_string(), $narrower->narrow('not date time'));
        static::assertEquals(type_string(), $narrower->narrow('2021-13-01'));
        static::assertEquals(type_string(), $narrower->narrow('now'));
        static::assertEquals(type_string(), $narrower->narrow('midnight'));
        static::assertEquals(type_string(), $narrower->narrow('today'));
        static::assertEquals(type_string(), $narrower->narrow('yesterday'));
        static::assertEquals(type_string(), $narrower->narrow('tomorrow'));
        static::assertEquals(type_string(), $narrower->narrow('+24h'));
        static::assertEquals(type_string(), $narrower->narrow('00:00:00'));
        static::assertEquals(type_datetime(), $narrower->narrow('2023-01-01 +10 hours'));
        static::assertEquals(type_datetime(), $narrower->narrow('Thursday, 02-Jun-2022 16:58:35 UTC'));
        static::assertEquals(type_datetime(), $narrower->narrow('2022-06-02T16:58:35+0000'));
        static::assertEquals(type_datetime(), $narrower->narrow('2022-06-02T16:58:35+00:00'));
        static::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        static::assertEquals(type_datetime(), $narrower->narrow('Thursday, 02-Jun-22 16:58:35 UTC'));
        static::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        static::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 2022 16:58:35 +0000'));
        static::assertEquals(type_string(), $narrower->narrow('2024-01'));
        static::assertEquals(type_date(), $narrower->narrow('12/31/2024'));
        static::assertEquals(type_date(), $narrower->narrow('2024-01-01'));
        static::assertEquals(type_datetime(), $narrower->narrow('2024-01-01 10:00'));
    }

    public function test_every_string_the_ladder_types_temporal_is_castable_by_that_type(): void
    {
        $narrower = new StringTypeNarrower();

        foreach ([
            '12/31/2024',
            '2024-01-01',
            '2023-01-01',
            '2023-01-01 +10 hours',
            'Thursday, 02-Jun-2022 16:58:35 UTC',
            '2022-06-02T16:58:35+0000',
            '2022-06-02T16:58:35+00:00',
            'Thu, 02 Jun 22 16:58:35 +0000',
            'Thursday, 02-Jun-22 16:58:35 UTC',
            'Thu, 02 Jun 2022 16:58:35 +0000',
            '2024-01-01 10:00',
        ] as $literal) {
            $narrowed = $narrower->narrow($literal);

            static::assertContains($narrowed->toString(), ['date', 'datetime'], $literal);
            static::assertInstanceOf(DateTimeInterface::class, $narrowed->cast($literal), $literal);
        }
    }

    public function test_detecting_float(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_float(), $narrower->narrow('1.0'));
        static::assertEquals(type_float(), $narrower->narrow('2.1E-5'));
        static::assertEquals(type_float(), $narrower->narrow('2.1e-5'));
        static::assertEquals(type_float(), $narrower->narrow('0.0'));
        static::assertEquals(type_string(), $narrower->narrow('not float'));
        static::assertEquals(type_integer(), $narrower->narrow('1'));
        static::assertEquals(type_string(), $narrower->narrow('1.0.0'));
    }

    public function test_compact_iso_still_narrows_to_integer(): void
    {
        // StringTemporalParts recognises '20240305' as a date so type_date() can cast it, but the
        // ladder runs its isInteger rung above the temporal ones and must keep doing so
        static::assertEquals(type_integer(), (new StringTypeNarrower())->narrow('20240305'));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_detecting_html(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(
            type_html(),
            $narrower->narrow(
                '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            ),
        );
        static::assertEquals(type_string(), $narrower->narrow('not html'));
    }

    public function test_detecting_integer(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_integer(), $narrower->narrow('1'));
        static::assertEquals(type_integer(), $narrower->narrow('0'));
        static::assertEquals(type_string(), $narrower->narrow('not integer'));
        static::assertEquals(type_float(), $narrower->narrow('1.0'));
        static::assertEquals(type_integer(), $narrower->narrow('112312312'));
        static::assertEquals(type_string(), $narrower->narrow('11_2312_312'));
        static::assertEquals(type_integer(), $narrower->narrow('20240101'));
        static::assertEquals(type_integer(), $narrower->narrow('19991231'));
        static::assertEquals(type_integer(), $narrower->narrow('1012024'));
    }

    public function test_detecting_json(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_json(), $narrower->narrow('{"foo":"bar"}'));
        static::assertEquals(type_json(), $narrower->narrow('[{"foo":"bar"}]'));
        static::assertEquals(type_string(), $narrower->narrow('not json'));
    }

    public function test_detecting_null(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_null(), $narrower->narrow('null'));
        static::assertEquals(type_null(), $narrower->narrow('NULL'));
        static::assertEquals(type_null(), $narrower->narrow('Nil'));
        static::assertEquals(type_null(), $narrower->narrow('nil'));
        static::assertEquals(type_string(), $narrower->narrow('not null'));
        static::assertEquals(type_string(), $narrower->narrow(''));
    }

    public function test_detecting_timezone(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_time_zone(), $narrower->narrow('UTC'));
        static::assertEquals(type_time_zone(), $narrower->narrow('America/New_York'));
        static::assertEquals(type_time_zone(), $narrower->narrow('Europe/London'));
        static::assertEquals(type_time_zone(), $narrower->narrow('Europe/Warsaw'));
        static::assertEquals(type_time_zone(), $narrower->narrow('Asia/Tokyo'));
        static::assertEquals(type_time_zone(), $narrower->narrow('Australia/Sydney'));

        static::assertEquals(type_time_zone(), $narrower->narrow('+00:00'));
        static::assertEquals(type_time_zone(), $narrower->narrow('+05:30'));
        static::assertEquals(type_time_zone(), $narrower->narrow('-08:00'));

        // (military time zones not in official list - might cause a lot of false positives)
        static::assertEquals(type_string(), $narrower->narrow('A'));
        static::assertEquals(type_string(), $narrower->narrow('B'));
        static::assertEquals(type_string(), $narrower->narrow('Z'));

        static::assertEquals(type_string(), $narrower->narrow('PST'));
        static::assertEquals(type_string(), $narrower->narrow('EST'));
        static::assertEquals(type_string(), $narrower->narrow('CET'));

        static::assertEquals(type_string(), $narrower->narrow('not a timezone'));
        static::assertEquals(type_string(), $narrower->narrow('Invalid/Timezone'));
        static::assertEquals(type_date(), $narrower->narrow('2023-01-01'));
        static::assertEquals(type_string(), $narrower->narrow(''));
        static::assertEquals(type_integer(), $narrower->narrow('123'));
    }

    public function test_detecting_uuid(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_uuid(), $narrower->narrow('f47ac10b-58cc-4372-a567-0e02b2c3d479'));
        static::assertEquals(type_string(), $narrower->narrow('not uuid'));
    }

    public function test_detecting_xml(): void
    {
        $narrower = new StringTypeNarrower();
        static::assertEquals(type_xml(), $narrower->narrow('<foo>bar</foo>'));
        static::assertEquals(type_string(), $narrower->narrow('not xml'));
        static::assertEquals(type_string(), $narrower->narrow('<unclosed'));
    }

    public function test_a_non_string_value_is_a_string_column(): void
    {
        $narrower = new StringTypeNarrower();

        static::assertEquals(type_string(), $narrower->narrow(1));
        static::assertEquals(type_string(), $narrower->narrow(null));
        static::assertEquals(type_string(), $narrower->narrow([1, 2]));
    }

    public function test_a_rung_outside_the_emitted_types_is_skipped(): void
    {
        $narrower = new StringTypeNarrower([type_integer(), type_string()]);
        static::assertEquals(type_string(), $narrower->narrow('<a><b>1</b></a>'));
        static::assertEquals(type_string(), $narrower->narrow('<div>x</div>'));
        static::assertEquals(
            type_string(),
            $narrower->narrow(
                '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            ),
        );
        static::assertEquals(type_xml(), (new StringTypeNarrower())->narrow('<a><b>1</b></a>'));
    }

    public function test_time_zone_identifiers_are_matched_after_the_cache_is_warm(): void
    {
        foreach ([new StringTypeNarrower(), new StringTypeNarrower()] as $narrower) {
            static::assertEquals(type_time_zone(), $narrower->narrow('Europe/Warsaw'));
            static::assertEquals(type_time_zone(), $narrower->narrow('UTC'));
            static::assertEquals(type_time_zone(), $narrower->narrow('+02:00'));
            static::assertEquals(type_string(), $narrower->narrow('Europe/Nowhere'));
        }
    }
}
