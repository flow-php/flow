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
 * Works around a bug in playwright-php/playwright-symfony v0.10.0 that corrupts every binary
 * response.
 *
 * The upstream converter base64-encodes a binary body and marks it `isBase64: true`
 * (`ResponseConverter.php:91-93`). That flag is then handed straight to Playwright by the Node
 * bridge (`playwright-server.js:124`, `route.fulfill(command.options)`), and Playwright has no such
 * option - so the base64 text is served as the body. The browser receives `AGFz...` where the wasm
 * magic number should be, and the playground cannot boot; web fonts fail the same way.
 *
 * `path` IS a real Playwright fulfill option, so the decoded bytes are written to a temp file and
 * the response is fulfilled from there instead. Remove this class once upstream is fixed.
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
