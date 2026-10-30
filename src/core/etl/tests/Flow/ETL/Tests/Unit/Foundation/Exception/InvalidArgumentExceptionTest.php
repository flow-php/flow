<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function Flow\Filesystem\DSL\path;

final class InvalidArgumentExceptionTest extends FlowTestCase
{
    public function test_an_unsupported_path_names_the_filesystem_the_path_and_where_to_pass_another(): void
    {
        static::assertSame(
            'Filesystem Flow\Filesystem\Local\NativeLocalFilesystem serves "file://" paths, given: '
            . '"memory://orders.csv". Pass the filesystem that handles this scheme, e.g. '
            . 'from_csv($path, filesystem: aws_s3_filesystem(...)).',
            InvalidArgumentException::unsupportedPath(
                new NativeLocalFilesystem(),
                path('memory://orders.csv'),
                'from_csv',
            )->getMessage(),
        );
    }
}
