<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function Flow\Types\DSL\type_enum;

final class ContentTypeDetector
{
    public function from(Path $path): string
    {
        if ($path->hasOption(Option::CONTENT_TYPE->value)) {
            $contentTypeOption = $path->getOption(Option::CONTENT_TYPE->value);

            if (\is_string($contentTypeOption)) {
                $contentType = $contentTypeOption;
            } else {
                type_enum(ContentType::class)->assert($contentTypeOption);

                $contentType = match ($contentTypeOption) {
                    ContentType::CSV => 'text/csv',
                    ContentType::JSON => 'application/json',
                    ContentType::XML => 'application/xml',
                    ContentType::PARQUET => 'application/vnd.apache.parquet',
                    ContentType::TEXT => 'text/plain',
                    ContentType::TSV => 'text/tab-separated-values',
                    ContentType::ZIP => 'application/zip',
                    ContentType::ORC => 'application/vnd.apache.orc',
                    ContentType::AVRO => 'application/vnd.apache.avro',
                    ContentType::BINARY => 'application/octet-stream',
                };
            }
        } else {
            $contentType = match ($path->extension()) {
                'csv' => 'text/csv',
                'json' => 'application/json',
                'xml' => 'application/xml',
                'parquet' => 'application/vnd.apache.parquet',
                'txt' => 'text/plain',
                'tsv' => 'text/tab-separated-values',
                'zip' => 'application/zip',
                'avro' => 'application/avro',
                'orc' => 'application/vnd.apache.orc',
                default => 'application/octet-stream',
            };
        }

        return $contentType;
    }
}
