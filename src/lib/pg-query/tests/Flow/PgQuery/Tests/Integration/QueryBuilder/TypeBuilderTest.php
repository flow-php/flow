<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_enum_type, create_composite_type, create_enum_type, create_range_type, drop_type, type_attr};

final class TypeBuilderTest extends PGQueryTestCase
{
    public function test_alter_enum_type_add_value() : void
    {
        $builder = alter_enum_type('status')
            ->addValue('archived');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'archived'"
        );
    }

    public function test_alter_enum_type_add_value_after() : void
    {
        $builder = alter_enum_type('status')
            ->addValueAfter('archived', 'closed');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'archived' AFTER 'closed'"
        );
    }

    public function test_alter_enum_type_add_value_before() : void
    {
        $builder = alter_enum_type('status')
            ->addValueBefore('pending', 'active');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'pending' BEFORE 'active'"
        );
    }

    public function test_alter_enum_type_add_value_if_not_exists() : void
    {
        $builder = alter_enum_type('status')
            ->addValue('archived')
            ->ifNotExists();

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE IF NOT EXISTS 'archived'"
        );
    }

    public function test_alter_enum_type_rename_value() : void
    {
        $builder = alter_enum_type('status')
            ->renameValue('old_name', 'new_name');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status RENAME VALUE 'old_name' TO 'new_name'"
        );
    }

    public function test_create_composite_type_simple() : void
    {
        $builder = create_composite_type('address')
            ->attributes(
                type_attr('street', 'text'),
                type_attr('city', 'text'),
                type_attr('zip', 'text')
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE address AS (street text, city text, zip text)'
        );
    }

    public function test_create_composite_type_with_collation() : void
    {
        $builder = create_composite_type('person')
            ->attributes(
                type_attr('name', 'text')->collate('en_US')
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE person AS (name text COLLATE "en_US")'
        );
    }

    public function test_create_composite_type_with_schema() : void
    {
        $builder = create_composite_type('public.address')
            ->attributes(
                type_attr('street', 'text')
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE public.address AS (street text)'
        );
    }

    public function test_create_enum_type_simple() : void
    {
        $builder = create_enum_type('status')
            ->labels('pending', 'active', 'closed');

        $this->assertCreateEnumTypeQuery(
            $builder,
            "CREATE TYPE status AS ENUM ('pending', 'active', 'closed')"
        );
    }

    public function test_create_enum_type_with_schema() : void
    {
        $builder = create_enum_type('public.status')
            ->labels('pending', 'active');

        $this->assertCreateEnumTypeQuery(
            $builder,
            "CREATE TYPE public.status AS ENUM ('pending', 'active')"
        );
    }

    public function test_create_range_type_simple() : void
    {
        $builder = create_range_type('floatrange')
            ->subtype('float8');

        $this->assertCreateRangeTypeQuery(
            $builder,
            'CREATE TYPE floatrange AS RANGE (subtype = float8)'
        );
    }

    public function test_create_range_type_with_collation() : void
    {
        $builder = create_range_type('textrange')
            ->subtype('text')
            ->collation('en_US');

        $this->assertCreateRangeTypeQuery(
            $builder,
            "CREATE TYPE textrange AS RANGE (subtype = text, \"collation\" = 'en_US')"
        );
    }

    public function test_create_range_type_with_options() : void
    {
        $builder = create_range_type('floatrange')
            ->subtype('float8')
            ->subtypeOpclass('float8_ops');

        $this->assertCreateRangeTypeQuery(
            $builder,
            "CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_opclass = 'float8_ops')"
        );
    }

    public function test_create_range_type_with_schema() : void
    {
        $builder = create_range_type('public.floatrange')
            ->subtype('float8');

        $this->assertCreateRangeTypeQuery(
            $builder,
            'CREATE TYPE public.floatrange AS RANGE (subtype = float8)'
        );
    }

    public function test_drop_type_cascade() : void
    {
        $builder = drop_type('address')
            ->cascade();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address CASCADE'
        );
    }

    public function test_drop_type_if_exists() : void
    {
        $builder = drop_type('address')
            ->ifExists();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE IF EXISTS address'
        );
    }

    public function test_drop_type_if_exists_cascade() : void
    {
        $builder = drop_type('address')
            ->ifExists()
            ->cascade();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE IF EXISTS address CASCADE'
        );
    }

    public function test_drop_type_multiple() : void
    {
        $builder = drop_type('address', 'status', 'floatrange');

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address, status, floatrange'
        );
    }

    public function test_drop_type_restrict() : void
    {
        $builder = drop_type('address')
            ->restrict();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address'
        );
    }

    public function test_drop_type_simple() : void
    {
        $builder = drop_type('address');

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address'
        );
    }
}
