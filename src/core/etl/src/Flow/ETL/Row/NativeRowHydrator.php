<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Exception\ExtensionException;
use RuntimeException;
use Throwable;

use function class_exists;
use function extension_loaded;

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
        return extension_loaded('flow_php') && class_exists(RustRowHydratorNative::class, false);
    }

    public function dehydrate(Rows $rows): array
    {
        return $this->native->dehydrate($rows);
    }

    public function hydrate(array $batch, Schema $schema): Rows
    {
        try {
            return $this->native->hydrate($batch, $schema);
        } catch (ExtensionException $e) {
            throw self::unwrap($e);
        }
    }

    /**
     * The extension turns any PHP exception raised inside it into an ExtensionException carrying the
     * original as previous. A batch refused by the row gate must reach the caller as the same
     * exception both hydrators throw, or the two disagree on nothing but the type.
     */
    private static function unwrap(ExtensionException $exception): Throwable
    {
        $previous = $exception->getPrevious();

        return $previous instanceof SchemaMismatchException ? $previous : $exception;
    }
}
