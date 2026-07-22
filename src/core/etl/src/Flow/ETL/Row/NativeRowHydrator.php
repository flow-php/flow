<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function class_exists;
use function extension_loaded;

final class NativeRowHydrator implements Hydrator
{
    private readonly RustRowHydratorNative $native;

    private readonly PhpRowHydrator $php;

    public function __construct()
    {
        if (!self::isSupported()) {
            throw new RuntimeException('flow_php extension with the native row hydrator is not loaded');
        }

        $this->native = new RustRowHydratorNative();
        $this->php = new PhpRowHydrator();
    }

    public static function isSupported(): bool
    {
        return extension_loaded('flow_php') && class_exists(RustRowHydratorNative::class, false);
    }

    public function cast(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            return $this->php->cast($batch, null);
        }

        return $this->native->cast($batch, $schema);
    }

    public function dehydrate(Rows $rows): array
    {
        return $this->native->dehydrate($rows);
    }

    public function hydrate(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            throw new InvalidArgumentException(
                'NativeRowHydrator::hydrate() requires a schema, use cast() to infer from values',
            );
        }

        return $this->native->hydrate($batch, $schema);
    }
}
