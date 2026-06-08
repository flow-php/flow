<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Filter;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\FixedMatcher;
use Flow\Telemetry\Tests\Mother\TempDir;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function count;
use function file_put_contents;
use function fileperms;
use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\not;
use function glob;
use function is_dir;
use function rmdir;
use function umask;
use function unlink;

final class AttributeFilterTest extends TestCase
{
    public function test_all_matcher_drops_only_when_every_rule_matches(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                all(attribute_rule('a', MatchMode::EQUAL, 1), attribute_rule('b', MatchMode::EQUAL, 2)),
                cacheDir: $tmp->path(),
            );

            static::assertFalse($filter->shouldDrop(Attributes::create(['a' => 1])));
            static::assertTrue($filter->shouldDrop(Attributes::create(['a' => 1, 'b' => 2])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_any_matcher_drops_when_one_rule_matches(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                any(attribute_rule('a', MatchMode::EQUAL, 1), attribute_rule('b', MatchMode::EQUAL, 2)),
                cacheDir: $tmp->path(),
            );

            static::assertTrue($filter->shouldDrop(Attributes::create(['a' => 1])));
            static::assertFalse($filter->shouldDrop(Attributes::create(['c' => 3])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_not_matcher_inverts_the_match(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(not(attribute_rule('env', MatchMode::EQUAL, 'prod')), cacheDir: $tmp->path());

            static::assertTrue($filter->shouldDrop(Attributes::create(['env' => 'dev'])));
            static::assertFalse($filter->shouldDrop(Attributes::create(['env' => 'prod'])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_codegen_writes_a_matcher_file(): void
    {
        $tmp = TempDir::create();

        try {
            attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path());

            static::assertCount(1, $tmp->files());
        } finally {
            $tmp->remove();
        }
    }

    public function test_cache_dir_is_created_with_configured_permissions(): void
    {
        $tmp = TempDir::create();
        $cacheDir = $tmp->path() . '/perms';
        $umask = umask(0);

        try {
            attribute_filter(
                attribute_rule('x', MatchMode::EQUAL, 'y'),
                cacheDir: $cacheDir,
                cacheDirPermissions: 0o750,
            );

            static::assertDirectoryExists($cacheDir);
            static::assertSame(0o750, fileperms($cacheDir) & 0o777);
        } finally {
            umask($umask);

            foreach (glob($cacheDir . '/*') ?: [] as $file) {
                unlink($file);
            }

            if (is_dir($cacheDir)) {
                rmdir($cacheDir);
            }

            $tmp->remove();
        }
    }

    public function test_cache_dir_permissions_out_of_range_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cache directory permissions must be between 0 and 0777');

        attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDirPermissions: 0o1000);
    }

    public function test_codegen_reuses_the_same_file_for_identical_matchers(): void
    {
        $tmp = TempDir::create();

        try {
            attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path());
            attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path());

            static::assertCount(1, $tmp->files());
        } finally {
            $tmp->remove();
        }
    }

    public function test_datetime_matcher_skips_codegen_but_still_matches(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                attribute_rule('t', MatchMode::GREATER_THAN, new DateTimeImmutable('2020-01-01')),
                cacheDir: $tmp->path(),
            );

            static::assertCount(0, $tmp->files());
            static::assertTrue($filter->shouldDrop(Attributes::create(['t' => new DateTimeImmutable('2021-01-01')])));
            static::assertFalse($filter->shouldDrop(Attributes::create(['t' => new DateTimeImmutable('2019-01-01')])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_exclude_is_the_default_polarity(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(attribute_rule('drop', MatchMode::EQUAL, true), cacheDir: $tmp->path());

            static::assertTrue($filter->shouldDrop(Attributes::create(['drop' => true])));
            static::assertFalse($filter->shouldDrop(Attributes::create(['drop' => false])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_include_polarity_keeps_only_matching_signals(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                attribute_rule('keep', MatchMode::EQUAL, true),
                exclude: false,
                cacheDir: $tmp->path(),
            );

            static::assertFalse($filter->shouldDrop(Attributes::create(['keep' => true])));
            static::assertTrue($filter->shouldDrop(Attributes::create(['keep' => false])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_non_writable_cache_dir_falls_back_to_interpreted_matching(): void
    {
        $filter = attribute_filter(
            any(attribute_rule('a', MatchMode::EQUAL, 1), attribute_rule('b', MatchMode::CONTAINS, 'x')),
            cacheDir: '/flow-nonexistent-dir/that/cannot/be/written',
        );

        static::assertTrue($filter->shouldDrop(Attributes::create(['a' => 1])));
        static::assertTrue($filter->shouldDrop(Attributes::create(['b' => 'xyz'])));
        static::assertFalse($filter->shouldDrop(Attributes::create(['c' => 3])));
    }

    public function test_non_compilable_root_matcher_uses_interpreted_matching(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(new FixedMatcher(true), cacheDir: $tmp->path());

            static::assertTrue($filter->shouldDrop(Attributes::create([])));
            static::assertCount(0, $tmp->files());
        } finally {
            $tmp->remove();
        }
    }

    public function test_include_polarity_on_non_compilable_matcher(): void
    {
        $tmp = TempDir::create();

        try {
            // exclude:false + non-compilable => interpreted, negated (keep only matches)
            $keepMatching = attribute_filter(new FixedMatcher(true), exclude: false, cacheDir: $tmp->path());
            $dropMatching = attribute_filter(new FixedMatcher(false), exclude: false, cacheDir: $tmp->path());

            static::assertFalse($keepMatching->shouldDrop(Attributes::create([])));
            static::assertTrue($dropMatching->shouldDrop(Attributes::create([])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_drop_function_returns_the_compiled_closure(): void
    {
        $tmp = TempDir::create();

        try {
            $drop = attribute_filter(
                attribute_rule('k', MatchMode::EQUAL, 'v'),
                cacheDir: $tmp->path(),
            )->dropFunction();

            static::assertTrue($drop(Attributes::create(['k' => 'v'])));
            static::assertFalse($drop(Attributes::create(['k' => 'other'])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_interpreted_fallback_short_circuits_all_matcher_on_first_non_match(): void
    {
        $filter = attribute_filter(
            all(attribute_rule('a', MatchMode::EQUAL, 1), attribute_rule('b', MatchMode::EQUAL, 2)),
            cacheDir: '/flow-nonexistent-dir/that/cannot/be/written',
        );

        static::assertFalse($filter->shouldDrop(Attributes::create(['a' => 999, 'b' => 2])));
    }

    public function test_sources_default_to_signal(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertSame(
                [AttributeSource::SIGNAL],
                attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path())->sources(),
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_sources_are_exposed(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertSame(
                [AttributeSource::RESOURCE, AttributeSource::SCOPE],
                attribute_filter(
                    attribute_rule('x', MatchMode::EQUAL, 'y'),
                    sources: [AttributeSource::RESOURCE, AttributeSource::SCOPE],
                    cacheDir: $tmp->path(),
                )->sources(),
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_empty_sources_falls_back_to_signal(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertSame(
                [AttributeSource::SIGNAL],
                attribute_filter(
                    attribute_rule('x', MatchMode::EQUAL, 'y'),
                    sources: [],
                    cacheDir: $tmp->path(),
                )->sources(),
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_should_drop_ors_the_matcher_across_sources(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                attribute_rule('env', MatchMode::EQUAL, 'prod'),
                sources: [AttributeSource::SIGNAL, AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            );

            // matches in the second source only -> dropped (OR across sources)
            static::assertTrue($filter->shouldDrop(
                Attributes::create(['env' => 'dev']),
                Attributes::create(['env' => 'prod']),
            ));
            // matches in no source -> kept
            static::assertFalse($filter->shouldDrop(
                Attributes::create(['env' => 'dev']),
                Attributes::create(['env' => 'staging']),
            ));
        } finally {
            $tmp->remove();
        }
    }

    public function test_exclude_false_keeps_signals_matching_in_any_source(): void
    {
        $tmp = TempDir::create();

        try {
            $filter = attribute_filter(
                attribute_rule('env', MatchMode::EQUAL, 'prod'),
                exclude: false,
                sources: [AttributeSource::SIGNAL, AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            );

            // matches in one source -> kept (polarity applied AFTER the OR)
            static::assertFalse($filter->shouldDrop(
                Attributes::create(['env' => 'dev']),
                Attributes::create(['env' => 'prod']),
            ));
            // matches in no source -> dropped
            static::assertTrue($filter->shouldDrop(
                Attributes::create(['env' => 'dev']),
                Attributes::create(['env' => 'staging']),
            ));
        } finally {
            $tmp->remove();
        }
    }

    public function test_distinct_matchers_produce_distinct_cache_files(): void
    {
        $tmp = TempDir::create();

        try {
            attribute_filter(attribute_rule('a', MatchMode::EQUAL, 1), cacheDir: $tmp->path());
            attribute_filter(attribute_rule('b', MatchMode::EQUAL, 2), cacheDir: $tmp->path());

            static::assertSame(2, count($tmp->files()));
        } finally {
            $tmp->remove();
        }
    }

    public function test_corrupt_cache_file_is_regenerated(): void
    {
        $tmp = TempDir::create();

        try {
            attribute_filter(attribute_rule('http.route', MatchMode::EQUAL, '/health'), cacheDir: $tmp->path());

            foreach ($tmp->files() as $file) {
                file_put_contents($file, '<?php return 42;');
            }

            $filter = attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/health'),
                cacheDir: $tmp->path(),
            );

            static::assertTrue($filter->shouldDrop(Attributes::create(['http.route' => '/health'])));
            static::assertFalse($filter->shouldDrop(Attributes::create(['http.route' => '/api'])));
        } finally {
            $tmp->remove();
        }
    }
}
