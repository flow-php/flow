<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function class_exists;
use function extension_loaded;
use function method_exists;

final class NativeRowHydrator implements Hydrator
{
    private readonly RustRowHydratorNative $native;

    public function __construct()
    {
        if (!self::isSupported()) {
            throw new RuntimeException('flow_php extension with the native row hydrator is not loaded');
        }

        $this->native = new RustRowHydratorNative();
    }

    public static function isSupported(): bool
    {
        return (
            extension_loaded('flow_php')
            && class_exists(RustRowHydratorNative::class, false)
            && method_exists(RustRowHydratorNative::class, 'datetimeZones')
        );
    }

    public function dehydrate(Rows $rows): array
    {
        return $this->native->dehydrate($rows);
    }

    public function hydrate(array $batch, Schema $schema): Rows
    {
        return $this->native->hydrate($batch, $schema);
    }
}
