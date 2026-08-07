<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\DateOrTime;
use Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum;
use Flow\Types\Tests\Unit\Type\Fixtures\TypeNarrowing;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;

use function array_keys;
use function fclose;
use function fopen;

/**
 * Runtime companion to {@see TypeNarrowing}. The fixture is the real guard — its sinks fail `just analyze` if any
 * `Type` implementation stops re-narrowing `assert()`, `cast()` or `isValid()` — and these cases keep it executed and
 * honest, so a DSL function that disappears or changes shape breaks a test rather than rotting silently.
 */
final class TypeNarrowingTest extends TestCase
{
    public function test_assert_narrows_logical_types(): void
    {
        $dateTime = new DateTimeImmutable('2026-08-07 10:00:00');
        $date = new DateTimeImmutable('2026-08-07 00:00:00');
        $document = new DOMDocument();
        $timeZone = new DateTimeZone('UTC');
        $uuid = Uuid::fromString('00000000-0000-0000-0000-000000000000');

        $narrowed = (new TypeNarrowing())->assertNarrowsLogical(
            stdClass::class,
            $dateTime,
            $date,
            $dateTime,
            Json::fromString('{"a":1}'),
            [1, 2],
            'fixed',
            ['one' => 1],
            'x',
            '12',
            5,
            7,
            'scalar',
            ['id' => 1],
            new DateInterval('PT1S'),
            $timeZone,
            $uuid,
            $document->createElement('a'),
            $document,
        );

        static::assertSame(stdClass::class, $narrowed['class_string']);
        static::assertSame($dateTime, $narrowed['date_time']);
        static::assertSame($date, $narrowed['date']);
        static::assertSame([1, 2], $narrowed['list']);
        static::assertSame('fixed', $narrowed['literal']);
        static::assertSame(['one' => 1], $narrowed['map']);
        static::assertSame('x', $narrowed['non_empty_string']);
        static::assertSame('12', $narrowed['numeric_string']);
        static::assertSame(5, $narrowed['optional']);
        static::assertSame(7, $narrowed['positive_integer']);
        static::assertSame(['id' => 1], $narrowed['structure']);
        static::assertSame($timeZone, $narrowed['time_zone']);
        static::assertSame($uuid, $narrowed['uuid']);
        static::assertSame($document, $narrowed['xml']);
    }

    public function test_assert_narrows_native_types(): void
    {
        $handle = fopen('php://memory', 'rb');
        $object = new stdClass();

        $narrowed = (new TypeNarrowing())->assertNarrowsNative(
            [],
            true,
            'strlen',
            SomeEnum::A,
            1.5,
            1,
            new DateOrTime(),
            'anything',
            null,
            $object,
            $handle,
            'a',
            42,
        );

        static::assertSame([], $narrowed['array']);
        static::assertTrue($narrowed['boolean']);
        static::assertSame('strlen', $narrowed['callable']);
        static::assertSame(SomeEnum::A, $narrowed['enum']);
        static::assertSame(1.5, $narrowed['float']);
        static::assertSame(1, $narrowed['integer']);
        static::assertSame('anything', $narrowed['mixed']);
        static::assertNull($narrowed['null']);
        static::assertSame($object, $narrowed['object']);
        static::assertSame($handle, $narrowed['resource']);
        static::assertSame('a', $narrowed['string']);
        static::assertSame(42, $narrowed['union']);

        fclose($handle);
    }

    public function test_cast_narrows_logical_types(): void
    {
        $dateTime = new DateTimeImmutable('2026-08-07 10:00:00');
        $document = new DOMDocument();

        $narrowed = (new TypeNarrowing())->castNarrowsLogical(
            stdClass::class,
            $dateTime,
            $dateTime,
            $dateTime,
            Json::fromString('{"a":1}'),
            [1, 2],
            'fixed',
            ['one' => 1],
            'x',
            '12',
            5,
            7,
            'scalar',
            ['id' => 1],
            new DateInterval('PT1S'),
            new DateTimeZone('UTC'),
            Uuid::fromString('00000000-0000-0000-0000-000000000000'),
            $document->createElement('a'),
            $document,
        );

        static::assertSame(stdClass::class, $narrowed['class_string']);
        static::assertSame([1, 2], $narrowed['list']);
        static::assertSame('fixed', $narrowed['literal']);
        static::assertSame('x', $narrowed['non_empty_string']);
        static::assertSame('12', $narrowed['numeric_string']);
        static::assertSame(7, $narrowed['positive_integer']);
        static::assertSame(['id' => 1], $narrowed['structure']);
    }

    public function test_cast_narrows_native_types(): void
    {
        $handle = fopen('php://memory', 'rb');

        $narrowed = (new TypeNarrowing())->castNarrowsNative(
            [],
            true,
            'strlen',
            SomeEnum::A,
            1.5,
            1,
            new DateOrTime(),
            'anything',
            null,
            new stdClass(),
            $handle,
            'a',
            42,
        );

        static::assertSame([], $narrowed['array']);
        static::assertTrue($narrowed['boolean']);
        static::assertSame(SomeEnum::A, $narrowed['enum']);
        static::assertSame(1.5, $narrowed['float']);
        static::assertSame(1, $narrowed['integer']);
        static::assertNull($narrowed['null']);
        static::assertSame('a', $narrowed['string']);
        static::assertSame(42, $narrowed['union']);

        fclose($handle);
    }

    public function test_cast_narrows_enum_from_backed_value(): void
    {
        $handle = fopen('php://memory', 'rb');

        $narrowed = (new TypeNarrowing())->castNarrowsNative(
            [],
            true,
            'strlen',
            SomeEnum::A->value,
            1.5,
            1,
            new DateOrTime(),
            'anything',
            null,
            new stdClass(),
            $handle,
            'a',
            42,
        );

        static::assertSame(SomeEnum::A, $narrowed['enum']);

        fclose($handle);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_types_narrow(): void
    {
        // @mago-expect analysis:unavailable-method
        $document = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body></body></html>');
        $element = $document->createElement('p');

        $narrowed = (new TypeNarrowing())->narrowsHtml($document, $element, $document, $element, $document, $element);

        static::assertSame($document, $narrowed['asserted_html']);
        static::assertSame($element, $narrowed['asserted_html_element']);
        static::assertSame($document, $narrowed['cast_html']);
        static::assertSame($element, $narrowed['cast_html_element']);
        static::assertSame($document, $narrowed['valid_html']);
        static::assertSame($element, $narrowed['valid_html_element']);
    }

    public function test_is_valid_narrows_logical_types(): void
    {
        $dateTime = new DateTimeImmutable('2026-08-07 10:00:00');
        $document = new DOMDocument();

        $narrowed = (new TypeNarrowing())->isValidNarrowsLogical(
            stdClass::class,
            $dateTime,
            new DateTimeImmutable('2026-08-07 00:00:00'),
            $dateTime,
            Json::fromString('{"a":1}'),
            [1, 2],
            'fixed',
            ['one' => 1],
            'x',
            '12',
            5,
            7,
            'scalar',
            ['id' => 1],
            new DateInterval('PT1S'),
            new DateTimeZone('UTC'),
            Uuid::fromString('00000000-0000-0000-0000-000000000000'),
            $document->createElement('a'),
            $document,
        );

        static::assertSame(
            [
                'class_string',
                'date_time',
                'date',
                'instance_of',
                'json',
                'list',
                'literal',
                'map',
                'non_empty_string',
                'numeric_string',
                'optional',
                'positive_integer',
                'scalar',
                'structure',
                'time',
                'time_zone',
                'uuid',
                'xml_element',
                'xml',
            ],
            array_keys($narrowed),
        );
    }

    public function test_is_valid_narrows_native_types(): void
    {
        $handle = fopen('php://memory', 'rb');

        $narrowed = (new TypeNarrowing())->isValidNarrowsNative(
            [],
            true,
            'strlen',
            SomeEnum::A,
            1.5,
            1,
            new DateOrTime(),
            null,
            new stdClass(),
            $handle,
            'a',
            42,
        );

        static::assertSame(
            [
                'array',
                'boolean',
                'callable',
                'enum',
                'float',
                'integer',
                'intersection',
                'null',
                'object',
                'resource',
                'string',
                'union',
            ],
            array_keys($narrowed),
        );

        fclose($handle);
    }

    public function test_is_valid_rejects_mismatched_values(): void
    {
        $narrowed = (new TypeNarrowing())->isValidNarrowsNative(
            'not-an-array',
            'not-a-bool',
            'not/a/callable',
            'not-an-enum',
            'not-a-float',
            'not-an-int',
            new stdClass(),
            'not-null',
            'not-an-object',
            'not-a-resource',
            1,
            1.5,
        );

        static::assertSame([], $narrowed);
    }
}
