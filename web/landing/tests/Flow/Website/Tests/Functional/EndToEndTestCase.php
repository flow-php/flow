<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Facebook\WebDriver\Exception\WebDriverException;
use Flow\Website\Kernel;
use Symfony\Component\Panther\{Client, PantherTestCase};

abstract class EndToEndTestCase extends PantherTestCase
{
    private array $tempFiles = [];

    protected function tearDown() : void
    {
        foreach ($this->tempFiles as $file) {
            @\unlink($file);
        }

        parent::tearDown();
    }

    protected function clearLocalStorage(Client $client) : void
    {
        $client->executeScript('window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-storage").clearCode();');
    }

    protected function createTempFile(string $filename, string $content) : string
    {
        $path = \sys_get_temp_dir() . '/' . $filename;
        \file_put_contents($path, $content);
        $this->tempFiles[] = $path;

        return $path;
    }

    protected function dismissAlert(Client $client) : void
    {
        try {
            $client->switchTo()->alert()->accept();
        } catch (\Exception) {
        }
    }

    protected function getFromLocalStorage(Client $client, string $key) : ?string
    {
        return $client->executeScript(\sprintf('return localStorage.getItem(%s);', \json_encode($key)));
    }

    protected function getPlaygroundCode(Client $client) : string
    {
        return $client->executeScript(
            'const textarea = document.getElementById("code-editor");
             const controller = window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-editor");
             return controller.getCode();'
        );
    }

    protected function setPlaygroundCode(Client $client, string $code) : void
    {
        $client->executeScript(\sprintf(
            'const textarea = document.getElementById("code-editor");
             const controller = window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-editor");
             controller.setCode(%s);
             // Manually save to localStorage for tests (bypass debounce)
             const playground = document.getElementById("playground");
             const storage = window.Stimulus.getControllerForElementAndIdentifier(playground, "playground-storage");
             if (storage) {
                 localStorage.setItem(storage.storageKeyValue || "flow-playground-code", %s);
             }',
            \json_encode($code),
            \json_encode($code)
        ));
    }

    protected function waitForWasmReady(Client $client, int $timeout = 30) : void
    {
        $startTime = \time();

        while (\time() - $startTime < $timeout) {
            try {
                $isReady = $client->executeScript(
                    'const playground = document.getElementById("playground");
                    if (!playground) return false;
                    const wasm = window.Stimulus.getControllerForElementAndIdentifier(playground, "wasm");
                    return wasm && wasm.isLoaded() && wasm.areResourcesLoaded();'
                );

                if ($isReady === true) {
                    $client->wait(0.5);

                    return;
                }
            } catch (WebDriverException) {
                // Page may not be fully attached yet, retry
            }

            $client->wait(0.5);
        }

        throw new \Exception('WASM did not initialize within ' . $timeout . ' seconds');
    }

    protected static function createE2EClient(array $options = []) : Client
    {
        return static::createPantherClient(\array_merge([
            'env' => ['APP_ENV' => 'test'],
        ], $options));
    }

    protected static function getKernelClass() : string
    {
        return Kernel::class;
    }

    /**
     * Navigate to URL with retry logic to handle transient WebDriver session issues.
     * Returns a fresh client that successfully navigated to the URL.
     */
    protected static function navigateWithRetry(string $url, int $maxRetries = 3) : Client
    {
        $lastException = null;

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            try {
                $client = static::createE2EClient();
                $client->request('GET', $url);

                return $client;
            } catch (WebDriverException $e) {
                $lastException = $e;

                if ($attempt === $maxRetries) {
                    throw $e;
                }
            }
        }

        throw $lastException;
    }
}
