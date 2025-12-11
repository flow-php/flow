<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter, create, drop, sql_type_text, type_attr};

final class TypeBuilderTest extends PGQueryTestCase
{
    public function test_alter_enum_type_add_value() : void
    {
        $builder = alter()->enumType('status')
            ->addValue('archived');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'archived'"
        );
    }

    public function test_alter_enum_type_add_value_after() : void
    {
        $builder = alter()->enumType('status')
            ->addValueAfter('archived', 'closed');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'archived' AFTER 'closed'"
        );
    }

    public function test_alter_enum_type_add_value_before() : void
    {
        $builder = alter()->enumType('status')
            ->addValueBefore('pending', 'active');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE 'pending' BEFORE 'active'"
        );
    }

    public function test_alter_enum_type_add_value_if_not_exists() : void
    {
        $builder = alter()->enumType('status')
            ->addValue('archived')
            ->ifNotExists();

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status ADD VALUE IF NOT EXISTS 'archived'"
        );
    }

    public function test_alter_enum_type_rename_value() : void
    {
        $builder = alter()->enumType('status')
            ->renameValue('old_name', 'new_name');

        $this->assertAlterEnumTypeQuery(
            $builder,
            "ALTER TYPE status RENAME VALUE 'old_name' TO 'new_name'"
        );
    }

    public function test_create_composite_type_simple() : void
    {
        $builder = create()->compositeType('address')
            ->attributes(
                type_attr('street', sql_type_text()),
                type_attr('city', sql_type_text()),
                type_attr('zip', sql_type_text())
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE address AS (street pg_catalog.text, city pg_catalog.text, zip pg_catalog.text)'
        );
    }

    public function test_create_composite_type_with_collation() : void
    {
        $builder = create()->compositeType('person')
            ->attributes(
                type_attr('name', sql_type_text())->collate('en_US')
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE person AS (name pg_catalog.text COLLATE "en_US")'
        );
    }

    public function test_create_composite_type_with_schema() : void
    {
        $builder = create()->compositeType('public.address')
            ->attributes(
                type_attr('street', sql_type_text())
            );

        $this->assertCreateCompositeTypeQuery(
            $builder,
            'CREATE TYPE public.address AS (street pg_catalog.text)'
        );
    }

    public function test_create_enum_type_simple() : void
    {
        $builder = create()->enumType('status')
            ->labels('pending', 'active', 'closed');

        $this->assertCreateEnumTypeQuery(
            $builder,
            "CREATE TYPE status AS ENUM ('pending', 'active', 'closed')"
        );
    }

    public function test_create_enum_type_with_schema() : void
    {
        $builder = create()->enumType('public.status')
            ->labels('pending', 'active');

        $this->assertCreateEnumTypeQuery(
            $builder,
            "CREATE TYPE public.status AS ENUM ('pending', 'active')"
        );
    }

    public function test_create_range_type_simple() : void
    {
        $builder = create()->rangeType('floatrange')
            ->subtype('float8');

        $this->assertCreateRangeTypeQuery(
            $builder,
            'CREATE TYPE floatrange AS RANGE (subtype = float8)'
        );
    }

    public function test_create_range_type_with_collation() : void
    {
        $builder = create()->rangeType('textrange')
            ->subtype('text')
            ->collation('en_US');

        $this->assertCreateRangeTypeQuery(
            $builder,
            "CREATE TYPE textrange AS RANGE (subtype = text, \"collation\" = 'en_US')"
        );
    }

    public function test_create_range_type_with_options() : void
    {
        $builder = create()->rangeType('floatrange')
            ->subtype('float8')
            ->subtypeOpclass('float8_ops');

        $this->assertCreateRangeTypeQuery(
            $builder,
            "CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_opclass = 'float8_ops')"
        );
    }

    public function test_create_range_type_with_schema() : void
    {
        $builder = create()->rangeType('public.floatrange')
            ->subtype('float8');

        $this->assertCreateRangeTypeQuery(
            $builder,
            'CREATE TYPE public.floatrange AS RANGE (subtype = float8)'
        );
    }

    public function test_drop_type_cascade() : void
    {
        $builder = drop()->type('address')
            ->cascade();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address CASCADE'
        );
    }

    public function test_drop_type_if_exists() : void
    {
        $builder = drop()->type('address')
            ->ifExists();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE IF EXISTS address'
        );
    }

    public function test_drop_type_if_exists_cascade() : void
    {
        $builder = drop()->type('address')
            ->ifExists()
            ->cascade();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE IF EXISTS address CASCADE'
        );
    }

    public function test_drop_type_multiple() : void
    {
        $builder = drop()->type('address', 'status', 'floatrange');

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address, status, floatrange'
        );
    }

    public function test_drop_type_restrict() : void
    {
        $builder = drop()->type('address')
            ->restrict();

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address'
        );
    }

    public function test_drop_type_simple() : void
    {
        $builder = drop()->type('address');

        $this->assertDropTypeQuery(
            $builder,
            'DROP TYPE address'
        );
    }
}
