<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Adapter\XML\Abstraction\XMLNode;

interface XMLWriter
{
    /**
     * @param list<?string> $values
     *
     * @return list<string> ` name="value"` per value
     */
    public function attributes(string $name, array $values): array;

    /**
     * @param list<?string> $values
     *
     * @return list<string> `<name>value</name>` per value; null and '' give `<name></name>`
     */
    public function elements(string $name, array $values): array;

    public function write(XMLNode $node): string;
}
