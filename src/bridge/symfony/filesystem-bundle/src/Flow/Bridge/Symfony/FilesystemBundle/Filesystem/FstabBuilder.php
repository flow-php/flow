<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\{InvalidArgumentException, LogicException};
use Flow\Filesystem\{FilesystemTable, Protocol};
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;

final class FstabBuilder
{
    /**
     * @param array<string, array<string, mixed>> $filesystems
     */
    public static function build(
        FilesystemFactoryRegistry $registry,
        string $fstabName,
        array $filesystems,
        ?FilesystemTelemetryConfig $telemetryConfig = null,
    ) : FilesystemTable {
        $table = new FilesystemTable();

        if ($telemetryConfig !== null) {
            $table = $table->withTelemetry($telemetryConfig);
        }

        foreach ($filesystems as $protocolName => $entry) {
            try {
                $protocol = new Protocol($protocolName);
                $factory = $registry->get($protocol);
                $filesystem = $factory->create($protocol, $entry);
            } catch (InvalidArgumentException|\Flow\ETL\Exception\InvalidArgumentException $e) {
                throw new LogicException(\sprintf(
                    'Fstab "%s" protocol "%s": %s',
                    $fstabName,
                    $protocolName,
                    $e->getMessage(),
                ), 0, $e);
            }

            $table->mount($filesystem);
        }

        return $table;
    }
}
