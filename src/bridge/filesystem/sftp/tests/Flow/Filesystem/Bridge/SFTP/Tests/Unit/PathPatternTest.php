<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\DirectoryTraversal\PathPattern;

use function Flow\Filesystem\DSL\path;

final class PathPatternTest extends FlowTestCase
{
    public function test_a_path_without_wildcards_accepts_everything_below_it(): void
    {
        $pattern = new PathPattern(path('sftp:///upload'));

        static::assertTrue($pattern->accepts(path('sftp:///upload/2024/01/orders.csv')));
        static::assertTrue($pattern->mayContainMatches('/upload/2024/01'));
    }

    public function test_directory_that_cannot_lead_to_a_match_is_not_descended_into(): void
    {
        $pattern = new PathPattern(path('sftp:///upload/2024/*.csv'));

        static::assertFalse($pattern->mayContainMatches('/upload/2023'));
    }

    public function test_directory_deeper_than_the_pattern_is_not_descended_into(): void
    {
        $pattern = new PathPattern(path('sftp:///upload/*.csv'));

        static::assertFalse($pattern->mayContainMatches('/upload/2024'));
    }

    public function test_directory_on_the_way_to_a_match_is_descended_into(): void
    {
        $pattern = new PathPattern(path('sftp:///upload/2024/*.csv'));

        static::assertTrue($pattern->mayContainMatches('/upload/2024'));
    }

    public function test_pattern_accepts_only_matching_paths(): void
    {
        $pattern = new PathPattern(path('sftp:///upload/*.csv'));

        static::assertTrue($pattern->accepts(path('sftp:///upload/orders.csv')));
        static::assertFalse($pattern->accepts(path('sftp:///upload/orders.json')));
    }

    public function test_recursive_pattern_descends_into_every_directory(): void
    {
        $pattern = new PathPattern(path('sftp:///upload/**/*.csv'));

        static::assertTrue($pattern->mayContainMatches('/upload/2024'));
        static::assertTrue($pattern->mayContainMatches('/upload/2024/01/02/03'));
        static::assertTrue($pattern->accepts(path('sftp:///upload/2024/01/orders.csv')));
    }
}
