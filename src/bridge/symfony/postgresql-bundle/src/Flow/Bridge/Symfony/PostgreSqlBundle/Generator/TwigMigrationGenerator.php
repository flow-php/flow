<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Generator;

use Flow\Filesystem\{Filesystem, Path};
use Flow\PostgreSql\Migrations\{Configuration, Version, VersionGenerator};
use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Twig\Environment;

final readonly class TwigMigrationGenerator implements MigrationGenerator
{
    public function __construct(
        private Configuration $configuration,
        private VersionGenerator $versionGenerator,
        private Environment $twig,
        private Filesystem $filesystem,
    ) {
    }

    public function generateDataMigration(?string $name = null) : Version
    {
        $version = $this->versionGenerator->generate();
        $directory = $this->migrationDirectory($version, $name);

        $this->filesystem->writeTo(Path::from($directory . '/' . $this->configuration->migrationFileName))
            ->append($this->twig->render('data_migration.php.twig', ['namespace' => $this->configuration->migrationsNamespace]))
            ->close();

        if ($this->configuration->generateRollback) {
            $this->filesystem->writeTo(Path::from($directory . '/' . $this->configuration->rollbackFileName))
                ->append($this->twig->render('data_rollback.php.twig', ['namespace' => $this->configuration->migrationsNamespace]))
                ->close();
        }

        return $version;
    }

    /**
     * @param list<string> $upSql
     * @param null|list<string> $downSql
     */
    public function generateSchemaMigration(?string $name, array $upSql, ?array $downSql = null) : Version
    {
        $version = $this->versionGenerator->generate();
        $directory = $this->migrationDirectory($version, $name);

        $this->filesystem->writeTo(Path::from($directory . '/' . $this->configuration->migrationFileName))
            ->append($this->twig->render('schema_migration.php.twig', [
                'namespace' => $this->configuration->migrationsNamespace,
                'queries' => $upSql,
            ]))
            ->close();

        if ($downSql !== null) {
            $this->filesystem->writeTo(Path::from($directory . '/' . $this->configuration->rollbackFileName))
                ->append($this->twig->render('schema_rollback.php.twig', [
                    'namespace' => $this->configuration->migrationsNamespace,
                    'queries' => $downSql,
                ]))
                ->close();
        }

        return $version;
    }

    private function migrationDirectory(Version $version, ?string $name) : string
    {
        if ($name === null) {
            return $this->configuration->migrationsDirectory . '/' . $version;
        }

        return $this->configuration->migrationsDirectory . '/' . $version . '_' . $name;
    }
}
