# ETL Adapter: XML

Flow PHP's Adapter XML is a dedicated library engineered to facilitate seamless interactions with XML data within your
ETL (Extract, Transform, Load) processes. This adapter empowers developers to effortlessly extract from and load data
into XML formats, ensuring a smooth and reliable data transformation journey. By harnessing the Adapter XML library,
developers can tap into a robust set of features designed for precise XML data handling, making complex data
transformations both manageable and efficient. The Adapter XML library encapsulates a rich set of functionalities,
providing a streamlined API for interacting with XML data, which is indispensable in modern data processing and
transformation workflows. This library embodies Flow PHP's commitment to providing versatile data processing solutions,
making it a prime choice for developers dealing with XML data in large-scale and data-intensive environments. With Flow
PHP's Adapter XML, managing XML data within your ETL workflows becomes a more simplified and efficient endeavor,
aligning perfectly with the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-xml:1.x@dev
```

## Extractor - XMLExtractor

Memory safe XML extractor 

`xml/simple_items.xml`

```xml
<root>
    <items>
        <item><id>1</id></item>
        <item><id>2</id></item>
        <item><id>3</id></item>
        <item><id>4</id></item>
        <item><id>5</id></item>
        <item><id>6</id></item>
    </items>
</root>
```

```php 
<?php

(new Flow())
    ->read(XML::from(__FLOW_DATA__ . '/simple_items.xml', 'root/items/item'))
    ->write(To::output(false))
    ->run()
;
```

Above code will generate Rows with 5 entries like the one below:

```shell
+----------------------------------------------+
|                                          row |
+----------------------------------------------+
| <?xml version="1.0"?><item><id>1</id></item> |
| <?xml version="1.0"?><item><id>2</id></item> |
| <?xml version="1.0"?><item><id>3</id></item> |
| <?xml version="1.0"?><item><id>4</id></item> |
| <?xml version="1.0"?><item><id>5</id></item> |
| <?xml version="1.0"?><item><id>6</id></item> |
+----------------------------------------------+
```

Each entry will be an XMLEntry type. 
From there you can use built in expressions to extract data from XML.

- `ref('row')->xpath('...');`
- `ref('row')->domNodeAttribute('...');`
- `ref('row')->domNodeValue('...');`

When working with collections XPath will return an ListEntry with XMLEntries inside. 
From there you can for example unpack or expand them. 

For more examples please look into `/examples/topics/xml` directory in [flow monorepo](https://github.com/flow-php/flow)

