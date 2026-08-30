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
use Flow\Types\Tests\Unit\Type\Fixtures\StructureShapeInference;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;

use function fclose;
use function fopen;

/**
 * Runtime companion to {@see StructureShapeInference}. The fixture is the real guard — its declared return types fail
 * `just analyze` if `type_structure()` stops resolving element shapes — and these cases keep it executed and honest.
 */
final class StructureShapeInferenceTest extends TestCase
{
    public function test_container_element_shapes(): void
    {
        $shape = (new StructureShapeInference())->containers([
            'list' => ['a', 'b'],
            'map' => ['one' => 1],
            'nested' => ['inner' => 7],
            'optional' => 'present',
        ]);

        static::assertSame(['a', 'b'], $shape['list']);
        static::assertSame(1, $shape['map']['one']);
        static::assertSame(7, $shape['nested']['inner']);
        static::assertSame('present', $shape['optional']);
    }

    public function test_marker_element_shapes(): void
    {
        $inference = new StructureShapeInference();

        $present = $inference->markerElements(['id' => 1, 'interleaved' => 'here', 'name' => 'flow']);

        static::assertSame(1, $present['id']);
        static::assertSame('here', $present['interleaved'] ?? null);
        static::assertSame('flow', $present['name']);

        $absent = $inference->markerElements(['id' => 2, 'name' => 'flow']);

        static::assertSame(2, $absent['id']);
        static::assertArrayNotHasKey('interleaved', $absent);
    }

    public function test_container_accessor_shapes(): void
    {
        $shape = (new StructureShapeInference())->containerAccessors(['a'], ['one' => 1], 'element', 'key', 9);

        static::assertSame(['a'], $shape['list']);
        static::assertSame(['one' => 1], $shape['map']);
        static::assertSame('element', $shape['element']);
        static::assertSame('key', $shape['key']);
        static::assertSame(9, $shape['value']);
    }

    public function test_generic_element_shapes(): void
    {
        $shape = (new StructureShapeInference())->generics([
            'enum' => SomeEnum::A,
            'instance_of' => new DateTimeImmutable('2026-08-07 10:00:00'),
            'class_string' => stdClass::class,
            'literal' => 'fixed',
            'union' => 42,
            'intersection' => new DateOrTime(),
        ]);

        static::assertSame(SomeEnum::A, $shape['enum']);
        static::assertSame(stdClass::class, $shape['class_string']);
        static::assertSame('fixed', $shape['literal']);
        static::assertSame(42, $shape['union']);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_element_shapes(): void
    {
        // @mago-expect analysis:unavailable-method
        $document = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body></body></html>');
        $element = $document->createElement('p');

        $shape = (new StructureShapeInference())->html(['html' => $document, 'html_element' => $element]);

        static::assertSame($document, $shape['html']);
        static::assertSame($element, $shape['html_element']);
    }

    public function test_object_element_shapes(): void
    {
        $document = new DOMDocument();
        $dateTime = new DateTimeImmutable('2026-08-07 10:00:00');

        $shape = (new StructureShapeInference())->objects([
            'json' => Json::fromString('{"a":1}'),
            'uuid' => Uuid::fromString('00000000-0000-0000-0000-000000000000'),
            'datetime' => $dateTime,
            'date' => new DateTimeImmutable('2026-08-07 00:00:00'),
            'time' => new DateInterval('PT1S'),
            'time_zone' => new DateTimeZone('UTC'),
            'xml' => $document,
            'xml_element' => $document->createElement('a'),
        ]);

        static::assertSame($dateTime, $shape['datetime']);
        static::assertSame('UTC', $shape['time_zone']->getName());
        static::assertSame($document, $shape['xml']);
    }

    public function test_scalar_element_shapes(): void
    {
        $handle = fopen('php://memory', 'rb');
        $object = new stdClass();

        $shape = (new StructureShapeInference())->scalars([
            'integer' => 1,
            'string' => 'a',
            'float' => 1.5,
            'boolean' => true,
            'numeric_string' => '12',
            'non_empty_string' => 'x',
            'positive_integer' => 5,
            'null' => null,
            'mixed' => 'anything',
            'array' => [],
            'object' => $object,
            'scalar' => 'scalar',
            'callable' => 'strlen',
            'resource' => $handle,
        ]);

        static::assertSame(1, $shape['integer']);
        static::assertSame('a', $shape['string']);
        static::assertSame(1.5, $shape['float']);
        static::assertTrue($shape['boolean']);
        static::assertNull($shape['null']);
        static::assertSame($object, $shape['object']);

        fclose($handle);
    }
}
