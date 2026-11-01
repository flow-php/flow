<?php

declare(strict_types=1);

namespace Flow\Website\Playwright;

use Playwright\Symfony\Client\ResponseConverter;
use RuntimeException;
use Symfony\Component\HttpFoundation\Response;

use function array_key_exists;
use function base64_decode;
use function file_exists;
use function file_put_contents;
use function is_dir;
use function is_string;
use function mkdir;
use function sha1;
use function sprintf;
use function sys_get_temp_dir;

/**
 * Workaround: playwright-symfony 0.10.0 serves base64 text for binary bodies; remove once fixed upstream.
 */
final class BinarySafeResponseConverter extends ResponseConverter
{
    /**
     * @return array<string, mixed>
     */
    public function prepareFulfillOptions(Response $response): array
    {
        $options = parent::prepareFulfillOptions($response);

        if (!array_key_exists('isBase64', $options) || !is_string($options['body'] ?? null)) {
            return $options;
        }

        $bytes = base64_decode($options['body'], true);

        if ($bytes === false) {
            return $options;
        }

        unset($options['body'], $options['isBase64']);
        $options['path'] = $this->spill($bytes);

        return $options;
    }

    private function spill(string $bytes): string
    {
        $directory = sys_get_temp_dir() . '/flow-playwright-assets';

        if (!is_dir($directory) && !mkdir($directory, 0o777, true) && !is_dir($directory)) {
            throw new RuntimeException(sprintf('Unable to create "%s".', $directory));
        }

        $path = $directory . '/' . sha1($bytes);

        if (!file_exists($path)) {
            file_put_contents($path, $bytes);
        }

        return $path;
    }
}
