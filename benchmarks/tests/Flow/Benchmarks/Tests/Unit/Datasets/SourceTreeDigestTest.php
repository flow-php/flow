<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Datasets;

use Flow\Benchmarks\Datasets\SourceTreeDigest;
use Flow\Benchmarks\Tests\Context\SourceTree;
use PHPUnit\Framework\TestCase;

final class SourceTreeDigestTest extends TestCase
{
    public function test_a_single_file_tree_is_digested(): void
    {
        $tree = new SourceTree('single_file');
        $tree->write('only.php', '<?php echo 1;');

        try {
            static::assertMatchesRegularExpression(
                '/^[0-9a-f]{32}$/',
                (new SourceTreeDigest($tree->relativePath() . '/only.php'))->value(),
            );
        } finally {
            $tree->remove();
        }
    }

    public function test_digest_changes_when_a_php_file_in_the_tree_changes(): void
    {
        $tree = new SourceTree('changed');
        $tree->write('a.php', '<?php echo 1;');

        try {
            $before = (new SourceTreeDigest($tree->relativePath()))->value();
            $tree->write('a.php', '<?php echo 1;;');

            static::assertNotSame($before, (new SourceTreeDigest($tree->relativePath()))->value());
        } finally {
            $tree->remove();
        }
    }

    public function test_digest_ignores_non_php_files(): void
    {
        $tree = new SourceTree('non_php');
        $tree->write('a.php', '<?php echo 1;');

        try {
            $before = (new SourceTreeDigest($tree->relativePath()))->value();
            $tree->write('notes.txt', 'anything at all');

            static::assertSame($before, (new SourceTreeDigest($tree->relativePath()))->value());
        } finally {
            $tree->remove();
        }
    }

    public function test_digest_is_order_independent(): void
    {
        $tree = new SourceTree('order');

        try {
            $tree->write('a.php', '<?php echo 1;');
            $tree->write('nested/b.php', '<?php echo 2;');
            $ascending = (new SourceTreeDigest($tree->relativePath()))->value();

            $tree->remove();
            $tree->write('nested/b.php', '<?php echo 2;');
            $tree->write('a.php', '<?php echo 1;');

            static::assertSame($ascending, (new SourceTreeDigest($tree->relativePath()))->value());
        } finally {
            $tree->remove();
        }
    }

    public function test_digest_is_stable_across_calls(): void
    {
        $tree = new SourceTree('stable');
        $tree->write('a.php', '<?php echo 1;');

        try {
            static::assertSame(
                (new SourceTreeDigest($tree->relativePath()))->value(),
                (new SourceTreeDigest($tree->relativePath()))->value(),
            );
        } finally {
            $tree->remove();
        }
    }
}
