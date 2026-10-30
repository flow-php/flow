<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Serializer;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Storage\Serializer\Exception\SerializationException;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Filesystem\Stream\StringSourceStream;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;
use function function_exists;
use function gzcompress;
use function gzuncompress;

final readonly class CompressingSerializer implements Describable, Serializer
{
    public function __construct(
        private Serializer $serializer,
        private int $compressionLevel = 9,
    ) {}

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        if (!function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new SerializationException(
                "'ext-zlib' is missing in, compression impossible due to lack of gzcompress.",
            );

            // @codeCoverageIgnoreEnd
        }

        $uncompressed = new StringDestinationStream(Path::from('memory://rows'));
        $this->serializer->serialize($rows, $uncompressed);
        $content = gzcompress($uncompressed->content(), $this->compressionLevel);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new SerializationException('Unable to compress serialized data.');

            // @codeCoverageIgnoreEnd
        }

        $destination->append($content);
        $destination->close();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['serializer'] ?? null, Serializer::class),
            type_integer()->assert($data['compression_level'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'serializer' => $this->serializer,
            'compression_level' => $this->compressionLevel,
        ]);
    }

    public function unserialize(SourceStream $source): Rows
    {
        if (!function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new SerializationException(
                "'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.",
            );

            // @codeCoverageIgnoreEnd
        }

        $payload = $source->content();
        $source->close();

        $content = gzuncompress($payload);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new SerializationException('Unable to decompress unserialized data.');

            // @codeCoverageIgnoreEnd
        }

        return $this->serializer->unserialize(new StringSourceStream(Path::from('memory://rows'), $content));
    }
}
