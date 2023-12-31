<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Partitions;

final class Paths
{
    private ?Partitions $partitions;

    /**
     * @var array<Path>
     */
    private readonly array $paths;

    public function __construct(Path ...$paths)
    {
        $this->paths = $paths;
        $this->partitions = null;
    }

    public function partitions() : Partitions
    {
        if ($this->partitions === null) {
            $partitions = [];

            foreach ($this->paths as $path) {
                foreach ($path->partitions() as $partition) {
                    $partitions[$partition->id()] = $partition;
                }
            }

            $this->partitions = new Partitions(...\array_values($partitions));
        }

        return $this->partitions;
    }
}
