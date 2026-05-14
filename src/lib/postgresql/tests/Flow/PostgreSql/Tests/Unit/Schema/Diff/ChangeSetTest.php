<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\ChangeSet;
use PHPUnit\Framework\TestCase;

final class ChangeSetTest extends TestCase
{
    public function test_constructor_with_added_and_removed_only(): void
    {
        $changeSet = new ChangeSet(['a', 'b'], ['c']);

        static::assertSame(['a', 'b'], $changeSet->added);
        static::assertSame(['c'], $changeSet->removed);
        static::assertNull($changeSet->modified);
        static::assertNull($changeSet->renamed);
    }

    public function test_constructor_with_all_properties(): void
    {
        $changeSet = new ChangeSet(['a'], ['b'], ['diff_ab'], ['old' => 'new']);

        static::assertSame(['a'], $changeSet->added);
        static::assertSame(['b'], $changeSet->removed);
        static::assertSame(['diff_ab'], $changeSet->modified);
        static::assertSame(['old' => 'new'], $changeSet->renamed);
    }

    public function test_constructor_with_empty_lists(): void
    {
        $changeSet = new ChangeSet([], []);

        static::assertSame([], $changeSet->added);
        static::assertSame([], $changeSet->removed);
        static::assertNull($changeSet->modified);
        static::assertNull($changeSet->renamed);
    }

    public function test_from_named_objects_detects_added(): void
    {
        $result = ChangeSet::fromNamedObjects(
            [],
            [['name' => 'a'], ['name' => 'b']],
            static fn(array $item): string => $item['name'],
        );

        static::assertCount(2, $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_from_named_objects_detects_added_removed_and_modified_simultaneously(): void
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
            static fn(array $item): string => $item['name'],
            static fn(array $a, array $b): ?string => $a['value'] === $b['value'] ? null : 'modified:' . $a['name'],
        );

        static::assertCount(1, $result->added);
        static::assertSame('add', $result->added[0]['name']);
        static::assertCount(1, $result->removed);
        static::assertSame('remove', $result->removed[0]['name']);
        static::assertSame(['modified:modify'], $result->modified);
    }

    public function test_from_named_objects_detects_modified(): void
    {
        $source = [['name' => 'a', 'value' => 1]];
        $target = [['name' => 'a', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn(array $item): string => $item['name'],
            static fn(array $a, array $b): ?string => $a['value'] === $b['value'] ? null : 'value_changed',
        );

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertSame(['value_changed'], $result->modified);
    }

    public function test_from_named_objects_detects_removed(): void
    {
        $result = ChangeSet::fromNamedObjects(
            [['name' => 'a'], ['name' => 'b']],
            [],
            static fn(array $item): string => $item['name'],
        );

        static::assertSame([], $result->added);
        static::assertCount(2, $result->removed);
    }

    public function test_from_named_objects_modified_returns_null_when_all_equal(): void
    {
        $items = [['name' => 'a', 'value' => 1]];

        $result = ChangeSet::fromNamedObjects(
            $items,
            $items,
            static fn(array $item): string => $item['name'],
            static fn(array $a, array $b): ?string => $a === $b ? null : 'diff',
        );

        static::assertNull($result->modified);
    }

    public function test_from_named_objects_no_changes_for_identical(): void
    {
        $items = [['name' => 'a', 'value' => 1], ['name' => 'b', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects(
            $items,
            $items,
            static fn(array $item): string => $item['name'],
            static fn(array $a, array $b): ?string => $a === $b ? null : 'changed',
        );

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertNull($result->modified);
    }

    public function test_from_named_objects_uses_identity_function_for_matching(): void
    {
        $source = [['id' => 'x', 'label' => 'old']];
        $target = [['id' => 'x', 'label' => 'new']];

        $result = ChangeSet::fromNamedObjects(
            $source,
            $target,
            static fn(array $item): string => $item['id'],
            static fn(array $a, array $b): ?string => $a['label'] === $b['label'] ? null : 'label_changed',
        );

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertSame(['label_changed'], $result->modified);
    }

    public function test_from_named_objects_with_empty_collections(): void
    {
        $result = ChangeSet::fromNamedObjects(
            [],
            [],
            static fn(array $item): string => (string) $item['name'],
            static fn(array $a, array $b): ?string => null,
        );

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertNull($result->modified);
    }

    public function test_from_named_objects_without_modified_fn(): void
    {
        $source = [['name' => 'a', 'value' => 1]];
        $target = [['name' => 'a', 'value' => 2]];

        $result = ChangeSet::fromNamedObjects($source, $target, static fn(array $item): string => $item['name']);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertNull($result->modified);
    }
}
