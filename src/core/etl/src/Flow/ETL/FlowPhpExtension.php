<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\RuntimeException;

use function constant;
use function defined;
use function extension_loaded;
use function phpversion;
use function sprintf;

final readonly class FlowPhpExtension
{
    /**
     * The contract version this package expects from flow_php, equal to the extension's FLOW_PHP_ABI. Both sides change
     * together whenever a class or interface the extension registers changes.
     */
    public const int ABI = 1;

    public function __construct(
        public bool $loaded,
        public ?int $abi,
        public string $version = 'unknown',
    ) {}

    public static function detect(): self
    {
        if (!extension_loaded('flow_php')) {
            return new self(false, null);
        }

        return new self(
            true,
            defined('FLOW_PHP_ABI') ? (int) constant('FLOW_PHP_ABI') : null,
            phpversion('flow_php') ?: 'unknown',
        );
    }

    /**
     * @throws RuntimeException flow_php is loaded but was built for another version of this package
     */
    public function available(): bool
    {
        if (!$this->loaded) {
            return false;
        }

        if ($this->abi !== self::ABI) {
            throw new RuntimeException(sprintf(
                'The loaded flow_php extension (%s, ABI %s) does not match flow-php/etl (ABI %d). Rebuild or upgrade '
                . 'flow_php to the version of flow-php/etl, or stop loading it to use the PHP implementation.',
                $this->version,
                $this->abi ?? 'none',
                self::ABI,
            ));
        }

        return true;
    }
}
