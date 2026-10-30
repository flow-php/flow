<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use SensitiveParameter;

final readonly class AdaptiveBackend implements Backend, Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    private Backend $backend;

    public function __construct(?FlowPhpExtension $extension = null)
    {
        $this->backend = ($extension ?? FlowPhpExtension::detect())->available() ? new RustBackend() : new PhpBackend();
    }

    public function builder(Definition $definition): ColumnBuilder
    {
        return $this->backend->builder($definition);
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        return $this->backend->constant($definition, $value, $count);
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        return $this->backend->decode($definition, $buffers, $count, $nullCount);
    }

    public function decodeFrame(string $body, Schema $schema, Codec $codec): Rows
    {
        return $this->backend->decodeFrame($body, $schema, $codec);
    }

    public function encodeFrame(Rows $rows, Codec $codec): string
    {
        return $this->backend->encodeFrame($rows, $codec);
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        return $this->backend->adopt($definition, $column);
    }

    public function allocatedBytes(): int
    {
        return $this->backend->allocatedBytes();
    }
}
