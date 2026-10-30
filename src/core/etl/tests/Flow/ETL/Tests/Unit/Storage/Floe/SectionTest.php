<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Floe;

use Flow\ETL\Storage\Floe\Section;
use Flow\ETL\Values\Column\Frame\FloeException;
use PHPUnit\Framework\TestCase;

final class SectionTest extends TestCase
{
    public function test_from_array_with_missing_field_throws(): void
    {
        $this->expectException(FloeException::class);

        Section::fromArray(['offset' => 6, 'partitionsId' => 0]);
    }

    public function test_from_array_with_non_integer_field_throws(): void
    {
        $this->expectException(FloeException::class);

        Section::fromArray(['offset' => '6', 'partitionsId' => 0, 'rowCount' => 1]);
    }

    public function test_an_unknown_section_key_is_ignored(): void
    {
        static::assertEquals(
            new Section(6, 1),
            Section::fromArray(['offset' => 6, 'partitionsId' => '0', 'rowCount' => 1]),
        );
    }

    public function test_normalize_round_trips(): void
    {
        $section = new Section(6, 100);

        static::assertEquals($section, Section::fromArray($section->normalize()));
        static::assertSame(['offset' => 6, 'rowCount' => 100], $section->normalize());
    }
}
