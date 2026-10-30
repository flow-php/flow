<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function var_export;

/**
 * What a step does to rows, to compare an original step with its DescribeRoundTrip::rebuilt() copy.
 */
final class StepRoundTrip
{
    /**
     * The var_export() text of $rows after $transformer, bound to $schema.
     */
    public static function transformed(
        Transformer $transformer,
        Schema $schema,
        Rows $rows,
        FlowContext $context,
    ): string {
        return var_export($transformer->bind($schema)->step->transform($rows, $context->backend())->toArray(), true);
    }

    /**
     * The bytes $loader writes for $rows at $file, which is removed afterwards.
     */
    public static function written(Loader $loader, Rows $rows, FlowContext $context, Path $file): string
    {
        $loader->load($rows, $context->backend());

        if ($loader instanceof Closure) {
            $loader->closure();
        }

        $filesystem = new NativeLocalFilesystem();
        $bytes = $filesystem->readFrom($file)->content();
        $filesystem->rm($file);

        return $bytes;
    }
}
