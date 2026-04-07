<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\ChangeSet;
use PHPUnit\Framework\TestCase;

final class ChangeSetTest extends TestCase
{
    public function test_constructor_with_added_and_removed_only() : void
    {
        $changeSet = new ChangeSet(['a', 'b'], ['c']);

        self::assertSame(['a', 'b'], $changeSet->added);
        self::assertSame(['c'], $changeSet->removed);
        self::assertNull($changeSet->modified);
        self::assertNull($changeSet->renamed);
    }

    public function test_constructor_with_all_properties() : void
    {
        $changeSet = new ChangeSet(['a'], ['b'], ['diff_ab'], ['old' => 'new']);

        self::assertSame(['a'], $changeSet->added);
        self::assertSame(['b'], $changeSet->removed);
        self::assertSame(['diff_ab'], $changeSet->modified);
        self::assertSame(['old' => 'new'], $changeSet->renamed);
    }

    public function test_constructor_with_empty_lists() : void
    {
        $changeSet = new ChangeSet([], []);

        self::assertSame([], $changeSet->added);
        self::assertSame([], $changeSet->removed);
        self::assertNull($changeSet->modified);
        self::assertNull($changeSet->renamed);
    }

    public function test_from_named_objects_detects_added() : void
    {
        $result = ChangeSet::fromNamedObjects(
            [],
            [['name' => 'a'], ['name' => 'b']],
            static fn (array $item) : string => $item['name'],
        );

        self::assertCount(2, $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_from_named_objects_detects_added_removed_and_modified_simultaneously() : void
    {
        $source = [
            ['name' => 'keep', 'value' => 1],
            ['name' => 'modify', 'value' => 10],
            ['name' => 'remove', 'value' => 99],
        ];
        $target = [
            ['name' => 'keep', 'value' => 1],
            ['name' => 'modify', 'value' => 20],
            ['name' => 'add', 'value' => 50],
        ];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn (array $item) : string => $item['name'],
            static fn (array $a, array $b) : ?string => $a['value'] === $b['value'] ? null : 'modified:' . $a['name'],
        );

        self::assertCount(1, $result->added);
        self::assertSame('add', $result->added[0]['name']);
        self::assertCount(1, $result->removed);
        self::assertSame('remove', $result->removed[0]['name']);
        self::assertSame(['modified:modify'], $result->modified);
    }

    public function test_from_named_objects_detects_modified() : void
    {
        $source = [['name' => 'a', 'value' => 1]];
        $target = [['name' => 'a', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn (array $item) : string => $item['name'],
            static fn (array $a, array $b) : ?string => $a['value'] === $b['value'] ? null : 'value_changed',
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertSame(['value_changed'], $result->modified);
    }

    public function test_from_named_objects_detects_removed() : void
    {
        $result = ChangeSet::fromNamedObjects(
            [['name' => 'a'], ['name' => 'b']],
            [],
            static fn (array $item) : string => $item['name'],
        );

        self::assertSame([], $result->added);
        self::assertCount(2, $result->removed);
    }

    public function test_from_named_objects_modified_returns_null_when_all_equal() : void
    {
        $items = [['name' => 'a', 'value' => 1]];

        $result = ChangeSet::fromNamedObjects(
            $items,
            $items,
            static fn (array $item) : string => $item['name'],
            static fn (array $a, array $b) : ?string => $a === $b ? null : 'diff',
        );

        self::assertNull($result->modified);
    }

    public function test_from_named_objects_no_changes_for_identical() : void
    {
        $items = [['name' => 'a', 'value' => 1], ['name' => 'b', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects(
            $items,
            $items,
            static fn (array $item) : string => $item['name'],
            static fn (array $a, array $b) : ?string => $a === $b ? null : 'changed',
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertNull($result->modified);
    }

    public function test_from_named_objects_uses_identity_function_for_matching() : void
    {
        $source = [['id' => 'x', 'label' => 'old']];
        $target = [['id' => 'x', 'label' => 'new']];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn (array $item) : string => $item['id'],
            static fn (array $a, array $b) : ?string => $a['label'] === $b['label'] ? null : 'label_changed',
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertSame(['label_changed'], $result->modified);
    }

    public function test_from_named_objects_with_empty_collections() : void
    {
        $result = ChangeSet::fromNamedObjects(
            [],
            [],
            static fn (array $item) : string => $item['name'],
            static fn (array $a, array $b) : ?string => null,
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertNull($result->modified);
    }

    public function test_from_named_objects_without_modified_fn() : void
    {
        $source = [['name' => 'a', 'value' => 1]];
        $target = [['name' => 'a', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn (array $item) : string => $item['name'],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertNull($result->modified);
    }
}
