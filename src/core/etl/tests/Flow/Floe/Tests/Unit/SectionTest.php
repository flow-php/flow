<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Section;
use PHPUnit\Framework\TestCase;

final class SectionTest extends TestCase
{
    public function test_from_array_with_missing_field_throws(): void
    {
        $this->expectException(FloeException::class);

        Section::fromArray(['offset' => 6, 'schemaId' => 0]);
    }

    public function test_from_array_with_non_integer_field_throws(): void
    {
        $this->expectException(FloeException::class);

        Section::fromArray(['offset' => '6', 'schemaId' => 0, 'rowCount' => 1]);
    }

    public function test_from_array_with_non_integer_schema_id_throws(): void
    {
        $this->expectException(FloeException::class);

        Section::fromArray(['offset' => 6, 'schemaId' => '0', 'rowCount' => 1]);
    }

    public function test_normalize_round_trips(): void
    {
        $section = new Section(6, 0, 100);

        static::assertEquals($section, Section::fromArray($section->normalize()));
        static::assertSame(['offset' => 6, 'schemaId' => 0, 'rowCount' => 100], $section->normalize());
    }
}
