<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\LocalFile;

/**
 * @infection-ignore-all
 */
class Avro
{
    /**
     * @param array<FileStream>|FileStream|string $uri
     * @param int $rows_in_batch
     * @param string $row_entry_name
     *
     * @throws MissingDependencyException
     *
     * @return Extractor
     */
    final public static function from(
        FileStream|string|array $uri,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        if (!\class_exists('AvroDataIO')) {
            throw new MissingDependencyException('Flix Tech Avro', 'flix-tech/avro-php');
        }

        if (\is_array($uri)) {
            /** @var array<Extractor> $extractors */
            $extractors = [];

            foreach ($uri as $fileStream) {
                $extractors[] = new AvroExtractor(
                    $fileStream,
                    $rows_in_batch,
                    $row_entry_name
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new AvroExtractor(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $rows_in_batch,
            $row_entry_name
        );
    }

    /**
     * @param FileStream|string $uri
     * @param null|Schema $schema
     * @param bool $safe_mode
     *
     * @throws MissingDependencyException
     *
     * @return Loader
     */
    final public static function to(FileStream|string $uri, Schema $schema = null, bool $safe_mode = false) : Loader
    {
        if (!\class_exists('AvroDataIO')) {
            throw new MissingDependencyException('Flix Tech Avro', 'flix-tech/avro-php');
        }

        return new AvroLoader(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $safe_mode,
            $schema
        );
    }
}
