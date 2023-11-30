# ETL Adapter: Text

- [⬅️️ Back](../../introduction.md)

Flow PHP's Adapter Text is a meticulously crafted library dedicated to enabling seamless handling of text data within
your ETL (Extract, Transform, Load) workflows. This adapter is pivotal for developers seeking to effortlessly extract
from or load data into text formats, ensuring a fluid and dependable data transformation experience. By employing the
Adapter Text library, developers have access to a robust set of features tailored for precise text data handling,
simplifying complex data transformations and streamlining text data processing tasks. The Adapter Text library
encapsulates an intuitive set of functionalities, offering a streamlined API for engaging with text data, which is
crucial in contemporary data processing and transformation scenarios. This library exemplifies Flow PHP's commitment to
providing versatile and efficient data processing solutions, making it an excellent choice for developers navigating
text data in large-scale and data-intensive projects. With Flow PHP's Adapter Text, managing text data within your ETL
workflows becomes a more refined and efficient endeavor, perfectly aligning with the robust and adaptable framework of
the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-text:1.x@dev
```

## Extractor

```php
<?php

use Flow\ETL\DSL\Text;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(Text::from(new LocalFile($path)))
    ->fetch();
```

## Loader

> :warning: Heads up, TextLoader expects rows to have single entry in order to properly write them into file.

```php 
<?php

use Flow\ETL\DSL\Text;
use Flow\ETL\Row;
use Flow\ETL\Rows;

(new Flow())
    ->process(
        new Rows(
            Row::create(new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\StringEntry('name', 'Tomek')),
            Row::create(new Row\Entry\StringEntry('name', 'Dawid')),
        )
    )
    ->load(Text::to($path))
    ->run();
```
