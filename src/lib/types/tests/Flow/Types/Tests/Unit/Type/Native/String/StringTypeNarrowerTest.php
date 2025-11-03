<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native\String;

use function Flow\Types\DSL\{type_boolean, type_date, type_datetime, type_float, type_html, type_integer, type_json, type_null, type_string, type_time_zone, type_uuid, type_xml};
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\TestCase;

final class StringTypeNarrowerTest extends TestCase
{
    public function test_detecting_boolean() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_boolean(), $narrower->narrow('true'));
        self::assertEquals(type_boolean(), $narrower->narrow('false'));
        self::assertEquals(type_string(), $narrower->narrow('yes'));
        self::assertEquals(type_string(), $narrower->narrow('no'));
        self::assertEquals(type_string(), $narrower->narrow('on'));
        self::assertEquals(type_string(), $narrower->narrow('off'));
        self::assertEquals(type_integer(), $narrower->narrow('0'));
        self::assertEquals(type_string(), $narrower->narrow('not bool'));
    }

    public function test_detecting_date_time() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_string(), $narrower->narrow('not date time'));
        self::assertEquals(type_string(), $narrower->narrow('2021-13-01'));
        self::assertEquals(type_string(), $narrower->narrow('now'));
        self::assertEquals(type_string(), $narrower->narrow('midnight'));
        self::assertEquals(type_string(), $narrower->narrow('today'));
        self::assertEquals(type_string(), $narrower->narrow('yesterday'));
        self::assertEquals(type_string(), $narrower->narrow('tomorrow'));
        self::assertEquals(type_string(), $narrower->narrow('+24h'));
        self::assertEquals(type_string(), $narrower->narrow('00:00:00'));
        self::assertEquals(type_datetime(), $narrower->narrow('2023-01-01 +10 hours'));
        self::assertEquals(type_datetime(), $narrower->narrow('Thursday, 02-Jun-2022 16:58:35 UTC'));
        self::assertEquals(type_datetime(), $narrower->narrow('2022-06-02T16:58:35+0000'));
        self::assertEquals(type_datetime(), $narrower->narrow('2022-06-02T16:58:35+00:00'));
        self::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), $narrower->narrow('Thursday, 02-Jun-22 16:58:35 UTC'));
        self::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), $narrower->narrow('Thu, 02 Jun 2022 16:58:35 +0000'));
    }

    public function test_detecting_float() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_float(), $narrower->narrow('1.0'));
        self::assertEquals(type_float(), $narrower->narrow('2.1E-5'));
        self::assertEquals(type_float(), $narrower->narrow('2.1e-5'));
        self::assertEquals(type_float(), $narrower->narrow('0.0'));
        self::assertEquals(type_string(), $narrower->narrow('not float'));
        self::assertEquals(type_integer(), $narrower->narrow('1'));
        self::assertEquals(type_string(), $narrower->narrow('1.0.0'));
    }

    public function test_detecting_html() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_html(), $narrower->narrow('<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>'));
        self::assertEquals(type_string(), $narrower->narrow('not html'));
    }

    public function test_detecting_integer() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_integer(), $narrower->narrow('1'));
        self::assertEquals(type_integer(), $narrower->narrow('0'));
        self::assertEquals(type_string(), $narrower->narrow('not integer'));
        self::assertEquals(type_float(), $narrower->narrow('1.0'));
        self::assertEquals(type_integer(), $narrower->narrow('112312312'));
        self::assertEquals(type_string(), $narrower->narrow('11_2312_312'));
    }

    public function test_detecting_json() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_json(), $narrower->narrow('{"foo":"bar"}'));
        self::assertEquals(type_json(), $narrower->narrow('[{"foo":"bar"}]'));
        self::assertEquals(type_string(), $narrower->narrow('not json'));
    }

    public function test_detecting_null() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_null(), $narrower->narrow('null'));
        self::assertEquals(type_null(), $narrower->narrow('NULL'));
        self::assertEquals(type_null(), $narrower->narrow('Nil'));
        self::assertEquals(type_null(), $narrower->narrow('nil'));
        self::assertEquals(type_string(), $narrower->narrow('not null'));
        self::assertEquals(type_string(), $narrower->narrow(''));
    }

    public function test_detecting_timezone() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_time_zone(), $narrower->narrow('UTC'));
        self::assertEquals(type_time_zone(), $narrower->narrow('America/New_York'));
        self::assertEquals(type_time_zone(), $narrower->narrow('Europe/London'));
        self::assertEquals(type_time_zone(), $narrower->narrow('Europe/Warsaw'));
        self::assertEquals(type_time_zone(), $narrower->narrow('Asia/Tokyo'));
        self::assertEquals(type_time_zone(), $narrower->narrow('Australia/Sydney'));

        self::assertEquals(type_time_zone(), $narrower->narrow('+00:00'));
        self::assertEquals(type_time_zone(), $narrower->narrow('+05:30'));
        self::assertEquals(type_time_zone(), $narrower->narrow('-08:00'));

        // (military time zones not in official list - might cause a lot of false positives)
        self::assertEquals(type_string(), $narrower->narrow('A'));
        self::assertEquals(type_string(), $narrower->narrow('B'));
        self::assertEquals(type_string(), $narrower->narrow('Z'));

        self::assertEquals(type_string(), $narrower->narrow('PST'));
        self::assertEquals(type_string(), $narrower->narrow('EST'));
        self::assertEquals(type_string(), $narrower->narrow('CET'));

        self::assertEquals(type_string(), $narrower->narrow('not a timezone'));
        self::assertEquals(type_string(), $narrower->narrow('Invalid/Timezone'));
        self::assertEquals(type_date(), $narrower->narrow('2023-01-01'));
        self::assertEquals(type_string(), $narrower->narrow(''));
        self::assertEquals(type_integer(), $narrower->narrow('123'));
    }

    public function test_detecting_uuid() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_uuid(), $narrower->narrow('f47ac10b-58cc-4372-a567-0e02b2c3d479'));
        self::assertEquals(type_string(), $narrower->narrow('not uuid'));
    }

    public function test_detecting_xml() : void
    {
        $narrower = new StringTypeNarrower();
        self::assertEquals(type_xml(), $narrower->narrow('<foo>bar</foo>'));
        self::assertEquals(type_string(), $narrower->narrow('not xml'));
    }
}
