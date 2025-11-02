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
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('true'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('false'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('yes'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('no'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('on'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow('off'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not bool'));
    }

    public function test_detecting_date_time() : void
    {
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not date time'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('2021-13-01'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('now'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('midnight'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('today'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('yesterday'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('tomorrow'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('+24h'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('00:00:00'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('2023-01-01 +10 hours'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('Thursday, 02-Jun-2022 16:58:35 UTC'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('2022-06-02T16:58:35+0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('2022-06-02T16:58:35+00:00'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('Thursday, 02-Jun-22 16:58:35 UTC'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow('Thu, 02 Jun 2022 16:58:35 +0000'));
    }

    public function test_detecting_float() : void
    {
        self::assertEquals(type_float(), StringTypeNarrower::narrow('1.0'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow('2.1E-5'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow('2.1e-5'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow('0.0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not float'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow('1'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('1.0.0'));
    }

    public function test_detecting_html() : void
    {
        self::assertEquals(type_html(), StringTypeNarrower::narrow('<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not html'));
    }

    public function test_detecting_integer() : void
    {
        self::assertEquals(type_integer(), StringTypeNarrower::narrow('1'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow('0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not integer'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow('1.0'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow('112312312'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('11_2312_312'));
    }

    public function test_detecting_json() : void
    {
        self::assertEquals(type_json(), StringTypeNarrower::narrow('{"foo":"bar"}'));
        self::assertEquals(type_json(), StringTypeNarrower::narrow('[{"foo":"bar"}]'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not json'));
    }

    public function test_detecting_null() : void
    {
        self::assertEquals(type_null(), StringTypeNarrower::narrow('null'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow('NULL'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow('Nil'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow('nil'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not null'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(''));
    }

    public function test_detecting_timezone() : void
    {
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('UTC'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('America/New_York'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('Europe/London'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('Europe/Warsaw'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('Asia/Tokyo'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('Australia/Sydney'));

        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('+00:00'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('+05:30'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow('-08:00'));

        // (military time zones not in official list - might cause a lot of false positives)
        self::assertEquals(type_string(), StringTypeNarrower::narrow('A'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('B'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('Z'));

        self::assertEquals(type_string(), StringTypeNarrower::narrow('PST'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('EST'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('CET'));

        self::assertEquals(type_string(), StringTypeNarrower::narrow('not a timezone'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('Invalid/Timezone'));
        self::assertEquals(type_date(), StringTypeNarrower::narrow('2023-01-01'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(''));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow('123'));
    }

    public function test_detecting_uuid() : void
    {
        self::assertEquals(type_uuid(), StringTypeNarrower::narrow('f47ac10b-58cc-4372-a567-0e02b2c3d479'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not uuid'));
    }

    public function test_detecting_xml() : void
    {
        self::assertEquals(type_xml(), StringTypeNarrower::narrow('<foo>bar</foo>'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow('not xml'));
    }
}
