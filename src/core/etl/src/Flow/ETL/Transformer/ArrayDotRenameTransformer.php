<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ArrayDot\array_dot_rename;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Rename\ArrayKeyRename;

/**
 * @implements Transformer<array{array_key_renames: array<ArrayKeyRename>}>
 *
 * @psalm-immutable
 */
final class ArrayDotRenameTransformer implements Transformer
{
    /**
     * @var ArrayKeyRename[]
     */
    private readonly array $arrayKeyRenames;

    public function __construct(ArrayKeyRename ...$arrayKeyRenames)
    {
        $this->arrayKeyRenames = $arrayKeyRenames;
    }

    public function __serialize() : array
    {
        return [
            'array_key_renames' => $this->arrayKeyRenames,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayKeyRenames = $data['array_key_renames'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            foreach ($this->arrayKeyRenames as $arrayKeyRename) {
                $arrayEntry = $row->get($arrayKeyRename->arrayEntry());

                if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                    $entryClass = $arrayEntry::class;

                    throw new RuntimeException("{$arrayEntry->name()} is not ArrayEntry but {$entryClass}");
                }

                $row = $row->set(
                    new Row\Entry\ArrayEntry(
                        $arrayEntry->name(),
                        array_dot_rename($arrayEntry->value(), $arrayKeyRename->path(), $arrayKeyRename->newName())
                    )
                );
            }

            return $row;
        };

        return $rows->map($transformer);
    }
}
