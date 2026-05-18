<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;

use function sprintf;

final class FstabBuilder
{
    /**
     * @param array<string, array<string, mixed>&array{type: string}> $filesystems each entry is keyed by mount protocol and must contain `type`
     */
    public static function build(
        FilesystemFactoryRegistry $registry,
        string $fstabName,
        array $filesystems,
        ?FilesystemTelemetryConfig $telemetryConfig = null,
    ): FilesystemTable {
        $table = new FilesystemTable();

        if ($telemetryConfig !== null) {
            $table = $table->withTelemetry($telemetryConfig);
        }

        foreach ($filesystems as $protocol => $entry) {
            $type = $entry['type'];
            $options = $entry;
            unset($options['type']);

            try {
                $factory = $registry->get($type);
                $filesystem = $factory->create($protocol, $options);
            } catch (InvalidArgumentException|\Flow\ETL\Exception\InvalidArgumentException $e) {
                throw new LogicException(
                    sprintf('Fstab "%s" mount "%s": %s', $fstabName, $protocol, $e->getMessage()),
                    0,
                    $e,
                );
            }

            $table->mount($filesystem);
        }

        return $table;
    }
}
