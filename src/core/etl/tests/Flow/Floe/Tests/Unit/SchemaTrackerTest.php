<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\FloeWriter;
use Flow\Floe\SchemaTracker;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class SchemaTrackerTest extends TestCase
{
    public function test_narrower_row_fits(): void
    {
        // a row missing a section column rides the section with an absent marker
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x')));
        $row = row(int_entry('a', 2));

        static::assertTrue((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_same_entries_fits(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x')));
        $row = row(int_entry('a', 2), str_entry('b', 'y'));

        static::assertTrue((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_a_new_column_does_not_fit(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1)));
        $row = row(int_entry('a', 2), int_entry('b', 3));

        static::assertFalse((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_a_column_absent_from_the_plan_does_not_fit(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1)));
        $row = row(int_entry('b', 2));

        static::assertFalse((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_an_incompatible_type_does_not_fit(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1)));
        $row = row(str_entry('a', 'x'));

        static::assertFalse((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_a_different_list_element_type_does_not_fit(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(list_entry('a', [1], type_list(type_integer()))));
        $row = row(list_entry('a', ['x'], type_list(type_string())));

        static::assertFalse((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_equal_but_not_identical_list_type_fits(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(list_entry('a', [1], type_list(type_integer()))));

        $tracker = new SchemaTracker();
        $row = row(list_entry('a', [2, 3], type_list(type_integer())));

        static::assertTrue($tracker->fits($plan, $row));
        static::assertTrue($tracker->fits($plan, $row));
    }

    public function test_cached_scalar_fingerprint_does_not_mask_a_type_change(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1)));

        $tracker = new SchemaTracker();

        static::assertTrue($tracker->fits($plan, row(int_entry('a', 2))));
        static::assertTrue($tracker->fits($plan, row(int_entry('a', 3))));
        static::assertFalse($tracker->fits($plan, row(str_entry('a', 'x'))));
    }

    public function test_row_with_custom_metadata_still_fits(): void
    {
        // documented limitation: column metadata is captured once per schema frame,
        // per-row metadata drift does not force a new section
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1)));
        $row = row(int_entry('a', 2, Metadata::fromArray(['custom' => 'meta'])));

        static::assertTrue((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_numeric_column_name_fits(): void
    {
        // a numeric column name coerces to an int key in the name-keyed plan;
        // the lookup key coerces identically, so the column still matches
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('123', 1)));

        static::assertTrue((new SchemaTracker())->fits($plan, row(int_entry('123', 2))));
        static::assertFalse((new SchemaTracker())->fits($plan, row(str_entry('123', 'x'))));
    }

    public function test_row_with_from_null_string_entry_fits_plain_string_plan(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(str_entry('a', 'value')));
        $row = row(StringEntry::fromNull('a'));

        static::assertTrue((new SchemaTracker())->fits($plan, $row));
    }

    public function test_row_with_null_value_still_fits(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(str_entry('a', 'value')));
        $row = row(str_entry('a', null));

        static::assertTrue((new SchemaTracker())->fits($plan, $row));
    }
}
